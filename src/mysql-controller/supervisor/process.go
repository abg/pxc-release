package supervisor

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

var ErrProcessNotRunning = errors.New("process not running")

// ProcessHandle represents a running child process.
// Single-use: once the process exits, create a new one via Launcher.
// Safe to call Signal and Wait concurrently.
type ProcessHandle struct {
	cmd     *exec.Cmd
	exitCh  chan struct{}
	exitErr error
	once    sync.Once
}

// Pid returns the process ID.
func (p *ProcessHandle) Pid() int {
	return p.cmd.Process.Pid
}

// Signal sends a signal to the process.
// Returns ErrProcessNotRunning if the process has already exited.
func (p *ProcessHandle) Signal(sig os.Signal) error {
	select {
	case <-p.exitCh:
		return ErrProcessNotRunning
	default:
	}

	if err := p.cmd.Process.Signal(sig); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return ErrProcessNotRunning
		}
		return fmt.Errorf("signal %s: %w", sig, err)
	}
	return nil
}

// Wait returns a channel that is closed when the process exits.
func (p *ProcessHandle) Wait() <-chan struct{} {
	return p.exitCh
}

// Err returns the exit error after the process has exited.
// Only valid after Wait() is closed.
func (p *ProcessHandle) Err() error {
	return p.exitErr
}

// reap starts the goroutine that calls cmd.Wait and closes exitCh.
// Must be called exactly once immediately after cmd.Start succeeds.
func (p *ProcessHandle) reap() {
	go p.once.Do(func() {
		p.exitErr = p.cmd.Wait()
		close(p.exitCh)
	})
}
