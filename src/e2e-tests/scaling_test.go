package e2e_tests

import (
	"database/sql"

	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"e2e-tests/utilities/bosh"
)

var _ = Describe("Scaling", Ordered, Label("scaling"), func() {
	var (
		db             *sql.DB
		deploymentName string
	)

	BeforeAll(func() {
		deploymentName = "pxc-scaling-" + uuid.New().String()

		Expect(bosh.DeployPXC(deploymentName,
			bosh.Operation("use-clustered.yml"),
			bosh.Operation("test/seed-test-user.yml"),
		)).To(Succeed())

		Expect(bosh.RunErrand(deploymentName, "smoke-tests", "mysql/first")).
			To(Succeed())

		proxyIPs, err := bosh.InstanceIPs(deploymentName, bosh.MatchByInstanceGroup("proxy"))
		Expect(err).NotTo(HaveOccurred())
		Expect(proxyIPs).To(HaveLen(2))

		db, err = sql.Open("mysql", "test-admin:integration-tests@tcp("+proxyIPs[0]+")/?tls=skip-verify")
		Expect(err).NotTo(HaveOccurred())
		db.SetMaxIdleConns(0)
		db.SetMaxOpenConns(1)
	})

	AfterAll(func() {
		if CurrentSpecReport().Failed() {
			return
		}

		Expect(bosh.DeleteDeployment(deploymentName)).To(Succeed())
	})

	It("starts with a healthy cluster of three nodes", func() {
		var unused, clusterSize string
		Expect(db.QueryRow(`SHOW GLOBAL STATUS LIKE 'wsrep\_cluster\_size'`).
			Scan(&unused, &clusterSize)).To(Succeed())
		Expect(clusterSize).To(Equal("3"))
	})

	It("can write data to this healthy database", func() {
		Expect(db.Exec(`CREATE DATABASE IF NOT EXISTS pxc_release_test_db`)).
			Error().NotTo(HaveOccurred())
		Expect(db.Exec(`CREATE TABLE IF NOT EXISTS pxc_release_test_db.scaling_test (test_data varchar(255) PRIMARY KEY)`)).
			Error().NotTo(HaveOccurred())
		Expect(db.Exec(`INSERT INTO pxc_release_test_db.scaling_test VALUES('data written with 3 nodes')`)).
			Error().NotTo(HaveOccurred())
	})

	It("scales the cluster down to one node", func() {
		Expect(bosh.DeployPXC(deploymentName,
			bosh.Operation("use-clustered.yml"),
			bosh.Operation("test/seed-test-user.yml"),
			bosh.Operation("minimal-mode.yml"),
		)).To(Succeed())

		var unused, clusterSize string
		Expect(db.QueryRow(`SHOW GLOBAL STATUS LIKE 'wsrep\_cluster\_size'`).
			Scan(&unused, &clusterSize)).To(Succeed())
		Expect(clusterSize).To(Equal("1"))
	})

	It("can write data to this scaled-down database", func() {
		Expect(db.Exec(`INSERT INTO pxc_release_test_db.scaling_test VALUES('data written with 1 nodes')`)).
			Error().NotTo(HaveOccurred())
	})

	verifyData := func(db *sql.DB) {
		var result []string
		rows, err := db.Query(`SELECT test_data FROM pxc_release_test_db.scaling_test`)
		Expect(err).NotTo(HaveOccurred())
		defer rows.Close()

		for rows.Next() {
			var data string
			Expect(rows.Scan(&data)).To(Succeed())
			result = append(result, data)
		}
		Expect(rows.Err()).NotTo(HaveOccurred())

		Expect(result).To(ConsistOf(
			"data written with 1 nodes",
			"data written with 3 nodes",
		))
	}

	It("retains the data from three nodes still", func() {
		verifyData(db)
	})

	It("can scale back up to three nodes", func() {
		Expect(bosh.DeployPXC(deploymentName,
			bosh.Operation("use-clustered.yml"),
			bosh.Operation("test/seed-test-user.yml"),
		)).To(Succeed())
	})

	It("retains the data on every mysql vm", func() {
		mysqlIps, err := bosh.InstanceIPs(deploymentName, bosh.MatchByInstanceGroup("mysql"))
		Expect(err).NotTo(HaveOccurred())
		Expect(mysqlIps).To(HaveLen(3))

		for _, host := range mysqlIps {
			db, err := sql.Open("mysql", "test-admin:integration-tests@tcp("+host+")/pxc_release_test_db?tls=skip-verify")
			Expect(err).NotTo(HaveOccurred())
			verifyData(db)
		}
	})

	It("still can access the data through the proxy ip", func() {
		verifyData(db)
	})
})
