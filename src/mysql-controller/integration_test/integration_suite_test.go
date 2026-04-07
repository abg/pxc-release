package integration_test

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"

	"mysql-controller/internal/docker"
	"mysql-controller/internal/tlscert"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "mysql-controller Integration Test Suite")
}

const (
	pxcDockerImage    = "percona/percona-xtradb-cluster:8.4"
	controllerAPIPort = "3307/tcp"

	apiUsername = "admin"
	apiPassword = "integration-test-secret"
)

var (
	sessionID           string
	sessionTmpdir       string
	mysqlControllerPath string

	sharedCert    tls.Certificate
	sharedCertPEM []byte
	sharedKeyPEM  []byte
)

var _ = BeforeSuite(func() {
	var err error

	mysqlControllerPath, err = gexec.BuildWithEnvironment(
		"mysql-controller/cmd/mysql-controller",
		[]string{"GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0"},
	)
	Expect(err).NotTo(HaveOccurred(),
		"Failed to compile mysql-controller for linux/amd64")

	sharedCertPEM, sharedKeyPEM, err = tlscert.GeneratePEM("localhost", "127.0.0.1")
	Expect(err).NotTo(HaveOccurred())

	sharedCert, err = tls.X509KeyPair(sharedCertPEM, sharedKeyPEM)
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})

var _ = BeforeEach(func() {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	Expect(err).NotTo(HaveOccurred())
	sessionID = hex.EncodeToString(b)

	sessionTmpdir, err = os.MkdirTemp("", "mysql-controller-integration-")
	Expect(err).NotTo(HaveOccurred())
	DeferCleanup(func() { _ = os.RemoveAll(sessionTmpdir) })
	Expect(os.Chmod(sessionTmpdir, 0o777)).To(Succeed())
})

// mustAbsPath resolves path relative to the test package directory.
func mustAbsPath(path string) string {
	abs, err := filepath.Abs(path)
	ExpectWithOffset(1, err).NotTo(HaveOccurred())
	return abs
}

// indentPEM indents each line of a PEM block with the given prefix,
// producing output suitable for a YAML block scalar (|).
func indentPEM(p []byte, prefix string) string {
	s := strings.TrimRight(string(p), "\n")
	lines := strings.Split(s, "\n")
	for i, l := range lines {
		lines[i] = prefix + l
	}
	return strings.Join(lines, "\n") + "\n"
}

// writeConfigFile generates a mysql-controller YAML config and writes it to path.
func writeConfigFile(path string) {
	content := fmt.Sprintf(`state_path: /var/lib/mysql/node_state.txt
grastate_path: /var/lib/mysql/grastate.dat
is_bootstrap_node: true
poll_interval: 5s
cluster_probe_timeout: 10s
mysql:
  mysqld_path: /usr/sbin/mysqld
  mysqld_args:
    - --defaults-file=/tmp/galera.cnf
  dsn: "root@unix(/var/lib/mysql/mysql.sock)/?interpolateParams=true"
api:
  bind_address: ":3307"
  available_when_read_only: false
  basic_auth:
    username: %s
    password: %s
  tls:
    ca: |
%s
    certificate: |
%s
    private_key: |
%s
  members:
    - id: mysql0
      endpoints:
        - https://127.0.0.1:3307/health
`,
		apiUsername,
		apiPassword,
		indentPEM(sharedCertPEM, "      "),
		indentPEM(sharedCertPEM, "      "),
		indentPEM(sharedKeyPEM, "      "),
	)

	Expect(os.WriteFile(path, []byte(content), 0o644)).To(Succeed())
}

// ContainerOption is a functional option for docker.ContainerSpec.
type ContainerOption func(*docker.ContainerSpec)

// createControllerContainer writes a config file to the session tmpdir then
// starts a PXC container running mysql-controller as PID 1.
func createControllerContainer(name string, opts ...ContainerOption) string {
	GinkgoHelper()

	configPath := filepath.Join(sessionTmpdir, "config.yaml")
	writeConfigFile(configPath)

	spec := docker.ContainerSpec{
		Name:       name + "." + sessionID,
		Image:      pxcDockerImage,
		Entrypoint: "/usr/local/bin/docker_entrypoint.sh",
		Ports:      []string{"3307"},
		Volumes: []string{
			mysqlControllerPath + ":/usr/local/bin/mysql-controller",
			configPath + ":/etc/mysql-controller/config.yaml",
			mustAbsPath("fixtures/docker_entrypoint.sh") + ":/usr/local/bin/docker_entrypoint.sh",
			mustAbsPath("fixtures/galera.cnf") + ":/tmp/galera.cnf",
			mustAbsPath("fixtures/init.sql") + ":/usr/local/bin/init.sql",
		},
	}

	for _, opt := range opts {
		opt(&spec)
	}

	return docker.RunContainer(spec)
}

// ---- HTTP client helpers ----

type apiClient struct {
	baseURL  string
	username string
	password string
	client   *http.Client
}

func newAPIClient(containerID string) *apiClient {
	GinkgoHelper()

	hostPort := docker.ContainerPort(containerID, controllerAPIPort)

	clientTLSCfg, err := tlscert.ClientTLSConfig(sharedCert)
	Expect(err).NotTo(HaveOccurred())

	return &apiClient{
		baseURL:  "https://127.0.0.1:" + hostPort,
		username: apiUsername,
		password: apiPassword,
		client: &http.Client{
			Transport: &http.Transport{TLSClientConfig: clientTLSCfg},
			// stop_mysql / start_mysql_* are blocking RPCs: they hold the
			// connection open until mysqld fully stops or starts (which can
			// take 30+ seconds with Galera).  Use a generous timeout so we
			// don't race against mysqld's graceful shutdown.
			Timeout: 3 * time.Minute,
		},
	}
}

func (a *apiClient) get(path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, a.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(a.username, a.password)
	return a.client.Do(req)
}

func (a *apiClient) post(path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, a.baseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(a.username, a.password)
	return a.client.Do(req)
}

// isHealthy returns nil when mysql-controller reports "synced" from /galera_status.
func isHealthy(a *apiClient) error {
	resp, err := a.get("/galera_status")
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// mysqlStatus returns the supervisor state string from /mysql_status.
func mysqlStatus(a *apiClient) (string, error) {
	resp, err := a.get("/mysql_status")
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), err
}

// sequenceNumber calls /sequence_number and returns the response body and HTTP status.
func sequenceNumber(a *apiClient) (string, int, error) {
	resp, err := a.get("/sequence_number")
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body)), resp.StatusCode, err
}
