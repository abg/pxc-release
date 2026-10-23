package supervisor_test

import (
	"context"
	"errors"
	"os/exec"
	"sync"
	"sync/atomic"
)

// ─── stubBuilder ─────────────────────────────────────────────────────────────

// stubBuilder implements supervisor.CommandBuilder via a swappable fn.
// Concurrency-safe: fn may be replaced between builds.
type stubBuilder struct {
	mu sync.Mutex
	fn func() (*exec.Cmd, error)
}

func (b *stubBuilder) Build(_ context.Context) (*exec.Cmd, error) {
	b.mu.Lock()
	fn := b.fn
	b.mu.Unlock()
	return fn()
}

// longRunningBuilder returns a builder whose process never exits on its own.
func longRunningBuilder() *stubBuilder {
	return &stubBuilder{fn: func() (*exec.Cmd, error) {
		return exec.Command("sleep", "100"), nil
	}}
}

// exitsThenRunsBuilder returns a builder where the first launched process exits
// immediately (cleanly) and all subsequent launches produce long-running processes.
// Use this to exercise the crash-and-restart path.
func exitsThenRunsBuilder() *stubBuilder {
	var n atomic.Int32
	return &stubBuilder{fn: func() (*exec.Cmd, error) {
		if n.Add(1) == 1 {
			return exec.Command("true"), nil // exits with code 0 immediately
		}
		return exec.Command("sleep", "100"), nil
	}}
}

// failsThenRunsBuilder returns a builder where Build() returns an error for the
// first failCount calls, then succeeds. Use this to exercise the launch-failure path.
func failsThenRunsBuilder(failCount int) *stubBuilder {
	var n atomic.Int32
	return &stubBuilder{fn: func() (*exec.Cmd, error) {
		if int(n.Add(1)) <= failCount {
			return nil, errors.New("simulated build failure")
		}
		return exec.Command("sleep", "100"), nil
	}}
}

// sigTermIgnoringBuilder returns a builder whose process explicitly ignores
// SIGTERM. The while loop prevents sh from exec'ing sleep (which would lose
// the trap), ensuring SIGTERM is consistently ignored until SIGKILL arrives.
func sigTermIgnoringBuilder() *stubBuilder {
	return &stubBuilder{fn: func() (*exec.Cmd, error) {
		return exec.Command("sh", "-c", "trap '' TERM; while :; do sleep 1; done"), nil
	}}
}

// ─── blockingProbe ────────────────────────────────────────────────────────────

// blockingProbe blocks WaitReady until trigger() is called or ctx is cancelled.
// Safe to call WaitReady concurrently; trigger() is idempotent.
type blockingProbe struct {
	once      sync.Once
	triggerCh chan struct{}
	err       error
}

func newBlockingProbe() *blockingProbe {
	return &blockingProbe{triggerCh: make(chan struct{})}
}

func (p *blockingProbe) WaitReady(ctx context.Context) error {
	select {
	case <-p.triggerCh:
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// trigger signals all current and future WaitReady calls to return.
func (p *blockingProbe) trigger() { p.once.Do(func() { close(p.triggerCh) }) }

// ─── immediateProbe ───────────────────────────────────────────────────────────

// immediateProbe satisfies ReadinessProbe and returns immediately on every call.
type immediateProbe struct{ err error }

func (p *immediateProbe) WaitReady(_ context.Context) error { return p.err }

// ─── spyHook ─────────────────────────────────────────────────────────────────

// spyHook records calls to OnReady and can be configured to return an error.
type spyHook struct {
	mu    sync.Mutex
	count int
	err   error
}

func (h *spyHook) OnReady(_ context.Context) error {
	h.mu.Lock()
	h.count++
	h.mu.Unlock()
	return h.err
}

func (h *spyHook) callCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.count
}
