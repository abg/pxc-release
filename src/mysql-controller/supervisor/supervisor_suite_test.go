package supervisor_test

import (
	"io"
	"log/slog"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSupervisor(t *testing.T) {
	useTestLogger(t, GinkgoWriter)
	RegisterFailHandler(Fail)
	RunSpecs(t, "Supervisor Suite")
}

func useTestLogger(t *testing.T, output io.Writer) {
	t.Helper()
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
}
