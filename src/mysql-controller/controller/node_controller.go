package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"mysql-controller/galera"
	"mysql-controller/supervisor"
)

// SupervisorController is the subset of supervisor.Supervisor used by NodeController.
type SupervisorController interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Status() supervisor.Status
}

type MySQLCmdBuilder interface {
	Build(ctx context.Context) (*exec.Cmd, error)
}

// NodeController implements api.ServiceController and api.SequenceNumberChecker.
// It serialises all mutating operations via a TryLock — concurrent calls fail
// immediately with "operation already in progress" rather than queuing.
//
// GetStatus is intentionally unguarded: cf-mysql-bootstrap polls /mysql_status
// while a Stop or Start is in progress, so blocking it behind the same mutex
// would deadlock.
type NodeController struct {
	Supervisor      SupervisorController
	StateFile       galera.StateFileReadWriter
	MySQLCmdBuilder MySQLCmdBuilder

	mu sync.Mutex
}

func NewNodeController(
	supervisor SupervisorController,
	stateFile galera.StateFileReadWriter,
	mysqlCmdBuilder MySQLCmdBuilder,
) *NodeController {
	return &NodeController{
		Supervisor:      supervisor,
		StateFile:       stateFile,
		MySQLCmdBuilder: mysqlCmdBuilder,
	}
}

// Bootstrap stops mysqld, writes NEEDS_BOOTSTRAP, then starts mysqld.
// The resolver will bootstrap the cluster on the next launch.
func (n *NodeController) Bootstrap(ctx context.Context) (string, error) {
	if !n.mu.TryLock() {
		return "", errors.New("operation already in progress")
	}
	defer n.mu.Unlock()

	if err := n.stopIfRunning(ctx); err != nil {
		return "", err
	}
	if err := n.StateFile.Write(galera.StateNeedsBootstrap); err != nil {
		return "", fmt.Errorf("writing state file: %w", err)
	}
	if err := n.Supervisor.Start(ctx); err != nil {
		return "", fmt.Errorf("starting mysqld: %w", err)
	}
	return "cluster bootstrap successful", nil
}

// Join stops mysqld, writes CLUSTERED, then starts mysqld.
// The resolver will join the existing cluster on the next launch.
func (n *NodeController) Join(ctx context.Context) (string, error) {
	if !n.mu.TryLock() {
		return "", errors.New("operation already in progress")
	}
	defer n.mu.Unlock()

	if err := n.stopIfRunning(ctx); err != nil {
		return "", err
	}
	if err := n.StateFile.Write(galera.StateClustered); err != nil {
		return "", fmt.Errorf("writing state file: %w", err)
	}
	if err := n.Supervisor.Start(ctx); err != nil {
		return "", fmt.Errorf("starting mysqld: %w", err)
	}
	return "join cluster successful", nil
}

// SingleNode stops mysqld, writes SINGLE_NODE, then starts mysqld.
func (n *NodeController) SingleNode(ctx context.Context) (string, error) {
	if !n.mu.TryLock() {
		return "", errors.New("operation already in progress")
	}
	defer n.mu.Unlock()

	if err := n.stopIfRunning(ctx); err != nil {
		return "", err
	}
	if err := n.StateFile.Write(galera.StateSingleNode); err != nil {
		return "", fmt.Errorf("writing state file: %w", err)
	}
	if err := n.Supervisor.Start(ctx); err != nil {
		return "", fmt.Errorf("starting mysqld: %w", err)
	}
	return "single node start successful", nil
}

// Stop stops mysqld and sets supervisor intent to IntentStop.
func (n *NodeController) Stop(ctx context.Context) (string, error) {
	if !n.mu.TryLock() {
		return "", errors.New("operation already in progress")
	}
	defer n.mu.Unlock()

	if err := n.Supervisor.Stop(ctx); err != nil {
		return "", fmt.Errorf("stopping mysqld: %w", err)
	}
	return "stop successful", nil
}

// GetStatus returns the current mysqld lifecycle state as a string.
// The response values are chosen for compatibility with cf-mysql-bootstrap,
// which polls this endpoint and pattern-matches on specific strings.
//
// StateCrashed is mapped to "starting" rather than "failing" because the
// supervisor will auto-restart — returning "failing" would cause
// cf-mysql-bootstrap to abort immediately, when the right behaviour is to
// keep polling until the restart succeeds or times out.
func (n *NodeController) GetStatus(_ context.Context) (string, error) {
	status := n.Supervisor.Status()
	switch status.State {
	case supervisor.StateCrashed:
		return "starting", nil
	default:
		return status.State.String(), nil
	}
}

// CheckSequenceNumber runs wsrep recovery against a stopped database
// and returns the recovered sequence number.
//
// The NodeController mutex guarantees mysqld cannot be restarted while
// recovery is running: Start/Bootstrap/Join/SingleNode all acquire the same
// mutex, so the supervisor cannot be asked to relaunch mysqld until this
// method returns.
func (n *NodeController) CheckSequenceNumber() (string, error) {
	if !n.mu.TryLock() {
		return "", errors.New("operation already in progress")
	}
	defer n.mu.Unlock()

	status := n.Supervisor.Status()
	if status.State != supervisor.StateStopped {
		return "", fmt.Errorf("cannot recover sequence number in state %s", status.State)
	}

	return n.recoverSeqno()
}

// stopIfRunning stops the supervisor if it is not already stopped.
// Must be called with mu held.
func (n *NodeController) stopIfRunning(ctx context.Context) error {
	status := n.Supervisor.Status()
	if status.State == supervisor.StateStopped &&
		status.Intent == supervisor.IntentStop {
		return nil
	}
	return n.Supervisor.Stop(ctx)
}

var seqnoRegex = regexp.MustCompile(`WSREP. Recovered position: (.*:-?\d+)`)

// recoverSeqno invokes mysqld --wsrep-recover, parses the sequence number
// from the error log, and returns it as a string.
// Must be called with mu held and supervisor confirmed stopped.
func (n *NodeController) recoverSeqno() (string, error) {
	var output bytes.Buffer
	cmd, err := n.MySQLCmdBuilder.Build(context.Background())
	if err != nil {
		return "", fmt.Errorf("building mysqld command: %w", err)
	}
	cmd.Stdout = &output
	cmd.Stderr = &output

	slog.Info("running wsrep recovery", "path", cmd.Path, "args", cmd.Args)

	if err := cmd.Run(); err != nil {
		// mysqld --wsrep-recover exits non-zero on some versions even on success;
		// parse the log before deciding whether to fail.
		slog.Warn("wsrep-recover exited non-zero", "error", err, "output", output.String())
		return "", fmt.Errorf("wsrep recovery failed")
	}

	matches := seqnoRegex.FindStringSubmatch(output.String())

	// Two matches here mean the regex found the pattern.
	// The first match is the entire string: "WSREP: Recovered position: $UUID:$SEQNO"
	// The second match will be the capturing group from the above regex: "$UUID:$SEQNO"
	if len(matches) != 2 {
		return "", fmt.Errorf("wsrep recovery output did not match expected format: %s", output.String())
	}

	uuidSeqno := matches[1]

	// This "zero" starting position is the default, if no state could be recovered from transaction logs
	// When the null starting position is found, this means incremental state transfer will not proceed, so no sense
	// setting the wsrep start position.
	const wsrepStartPositionZero = "00000000-0000-0000-0000-000000000000:-1"

	if uuidSeqno == wsrepStartPositionZero {
		return "", fmt.Errorf("wsrep recovery found zero position, no state could be recovered from transaction logs")
	}

	uuid, seqno, found := strings.Cut(uuidSeqno, ":") // validate format is $UUID:$SEQNO
	if !found || uuid == "" || seqno == "" {
		return "", fmt.Errorf("wsrep recovery output did not contain valid UUID and sequence number: %s", uuidSeqno)
	}

	if seqnoInt, err := strconv.ParseInt(seqno, 10, 64); err != nil {
		return "", fmt.Errorf("wsrep recovery output contained invalid sequence number: %s", seqno)
	} else if seqnoInt < 0 {
		return "", fmt.Errorf("Invalid sequence number %s", seqno)
	}

	return seqno, nil
}
