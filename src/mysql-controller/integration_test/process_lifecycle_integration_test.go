package integration_test

import (
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"mysql-controller/internal/docker"
)

var _ = Describe("mysql-controller process lifecycle", func() {
	var (
		containerID string
		api         *apiClient
	)

	BeforeEach(func() {
		containerID = createControllerContainer("mysql0")
		DeferCleanup(func() {
			_ = docker.Logs(containerID)
			_ = docker.RemoveContainer(containerID)
		})
		api = newAPIClient(containerID)
	})

	It("starts mysqld and eventually reports synced", func() {
		Eventually(func() error {
			return isHealthy(api)
		}, "5m", "2s").Should(Succeed(),
			"expected mysql-controller to report mysqld as synced within 5 minutes")
	})

	Describe("crash recovery", func() {
		It("automatically restarts mysqld after a SIGKILL", func() {
			By("waiting for initial sync")
			Eventually(func() error {
				return isHealthy(api)
			}, "5m", "2s").Should(Succeed())

			By("killing the mysqld process directly")
			_, err := docker.Command("exec", containerID,
				"sh", "-c", "kill -9 $(cat /var/lib/mysql/mysqld.pid)")
			Expect(err).NotTo(HaveOccurred())

			By("waiting for the supervisor to detect the crash and restart mysqld")
			// Allow a brief window for the health check to reflect the crash
			// before polling for recovery — the supervisor restarts quickly
			// so we might not catch the unhealthy window, and that's fine.
			time.Sleep(2 * time.Second)

			Eventually(func() error {
				return isHealthy(api)
			}, "5m", "2s").Should(Succeed(),
				"expected mysqld to be restarted and synced after crash")
		})
	})

	Describe("manual stop and start", func() {
		BeforeEach(func() {
			By("waiting for mysqld to be synced before each stop/start test")
			Eventually(func() error {
				return isHealthy(api)
			}, "5m", "2s").Should(Succeed())
		})

		It("stops and restarts mysqld via the API", func() {
			By("stopping mysqld via POST /stop_mysql")
			resp, err := api.post("/stop_mysql")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			_ = resp.Body.Close()

			By("waiting for the supervisor to report stopped")
			Eventually(func() (string, error) {
				return mysqlStatus(api)
			}, "2m", "2s").Should(Equal("stopped"))

			By("restarting mysqld via POST /start_mysql_single_node")
			resp, err = api.post("/start_mysql_single_node")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			_ = resp.Body.Close()

			By("waiting for mysqld to be synced again")
			Eventually(func() error {
				return isHealthy(api)
			}, "5m", "2s").Should(Succeed())
		})
	})

	Describe("sequence number", func() {
		BeforeEach(func() {
			By("waiting for mysqld to be synced")
			Eventually(func() error {
				return isHealthy(api)
			}, "5m", "2s").Should(Succeed())
		})

		It("returns an error when mysqld is running", func() {
			_, statusCode, err := sequenceNumber(api)
			Expect(err).NotTo(HaveOccurred())
			Expect(statusCode).To(Equal(500),
				"expected /sequence_number to fail while mysqld is running")
		})

		It("returns a valid non-negative sequence number when mysqld is stopped", func() {
			By("stopping mysqld")
			resp, err := api.post("/stop_mysql")
			Expect(err).NotTo(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(200))
			_ = resp.Body.Close()

			Eventually(func() (string, error) {
				return mysqlStatus(api)
			}, "2m", "2s").Should(Equal("stopped"))

			By("querying the sequence number")
			body, statusCode, err := sequenceNumber(api)
			Expect(err).NotTo(HaveOccurred())
			Expect(statusCode).To(Equal(200),
				"expected /sequence_number to succeed when mysqld is stopped, body: %s", body)

			seqno, err := strconv.ParseInt(body, 10, 64)
			Expect(err).NotTo(HaveOccurred(),
				"expected sequence number body %q to be a valid integer", body)
			Expect(seqno).To(BeNumerically(">=", 0),
				"expected a non-negative sequence number")
		})
	})
})
