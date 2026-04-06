package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
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

// WaitTimeout blocks until the process exits or the timeout elapses.
// Returns true if the process exited within the timeout.
func (p *ProcessHandle) WaitTimeout(timeout time.Duration) bool {
	select {
	case <-p.exitCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// stopGracefully sends SIGTERM and waits up to gracePeriod for the
// process to exit cleanly. If it does not exit in time, sends SIGKILL.
// Called by the supervisor's SIGTERM handler — not for general use.
func (p *ProcessHandle) stopGracefully(ctx context.Context, gracePeriod time.Duration) error {
	if err := p.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, ErrProcessNotRunning) {
			return nil
		}
		return fmt.Errorf("SIGTERM: %w", err)
	}

	slog.Info("sent SIGTERM", "pid", p.Pid())

	select {
	case <-p.exitCh:
		slog.Info("process exited after SIGTERM", "pid", p.Pid(), "err", p.exitErr)
		return nil

	case <-time.After(gracePeriod):
		slog.Warn("process did not exit within grace period, sending SIGKILL",
			slog.Int("pid", p.Pid()),
			slog.Duration("grace_period", gracePeriod),
		)
		if err := p.Signal(syscall.SIGKILL); err != nil && !errors.Is(err, ErrProcessNotRunning) {
			return fmt.Errorf("SIGKILL: %w", err)
		}
		select {
		case <-p.exitCh:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}

	case <-ctx.Done():
		return ctx.Err()
	}
}

// reap starts the goroutine that calls cmd.Wait and closes exitCh.
// Must be called exactly once immediately after cmd.Start succeeds.
func (p *ProcessHandle) reap() {
	go p.once.Do(func() {
		p.exitErr = p.cmd.Wait()
		close(p.exitCh)
	})
}
