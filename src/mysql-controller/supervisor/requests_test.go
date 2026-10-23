package supervisor_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"mysql-controller/supervisor"
)

type builderFunc func(context.Context) (*exec.Cmd, error)

func (f builderFunc) Build(ctx context.Context) (*exec.Cmd, error) { return f(ctx) }

type probeFunc func(context.Context) error

func (f probeFunc) WaitReady(ctx context.Context) error { return f(ctx) }

type hookFunc func(context.Context) error

func (f hookFunc) OnReady(ctx context.Context) error { return f(ctx) }

// A request evaluates Done only after registering its waiter. This lets tests
// order overlapping requests without relying on scheduler sleeps.
type observedContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func registeredRequest(t *testing.T, f func(context.Context) error) <-chan error {
	t.Helper()
	waiting := make(chan struct{})
	result := request(t, func(ctx context.Context) error {
		return f(&observedContext{Context: ctx, waiting: waiting})
	})
	select {
	case <-waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not register its waiter")
	}
	return result
}

// Use a child of the test binary so SIGTERM handling is installed before
// readiness is reported, without shell descendants surviving a failed test.
func TestSupervisorChild(t *testing.T) {
	if os.Getenv("SUPERVISOR_TEST_CHILD") != "1" {
		return
	}
	signal.Ignore(syscall.SIGTERM)
	_, _ = os.Stdout.WriteString("ready\n")
	for {
		time.Sleep(time.Hour)
	}
}

type readyWriter struct {
	once  sync.Once
	ready chan struct{}
}

func (w *readyWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.ready) })
	return len(p), nil
}

func testChild(ready chan struct{}) *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestSupervisorChild$")
	cmd.Env = append(os.Environ(), "SUPERVISOR_TEST_CHILD=1")
	cmd.Stdout = &readyWriter{ready: ready}
	cmd.Stderr = io.Discard
	return cmd
}

func waitUntil(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func runSupervisor(t *testing.T, s *supervisor.Supervisor) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	useTestLogger(t, t.Output())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("supervisor did not finish shutdown")
		}
	})
	return cancel, done
}

func request(t *testing.T, f func(context.Context) error) <-chan error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() { result <- f(ctx) }()
	return result
}

func expectResult(t *testing.T, result <-chan error, want error) {
	t.Helper()
	select {
	case err := <-result:
		if !errors.Is(err, want) {
			t.Fatalf("request returned %v, want %v", err, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request did not complete")
	}
}

func TestStartWhileRunningDoesNotLaunchAgain(t *testing.T) {
	var builds atomic.Int32
	s := supervisor.New(supervisor.NewLauncher(builderFunc(func(context.Context) (*exec.Cmd, error) {
		builds.Add(1)
		return exec.Command("sleep", "100"), nil
	})), &immediateProbe{}, nil)
	runSupervisor(t, s)
	waitUntil(t, "running", func() bool { return s.Status().State == supervisor.StateRunning })
	for range 20 {
		expectResult(t, request(t, s.Start), nil)
	}
	time.Sleep(50 * time.Millisecond)
	if n := builds.Load(); n != 1 {
		t.Fatalf("launched %d children for repeated Start calls", n)
	}
}

func TestStopCancelsReadiness(t *testing.T) {
	p := &trackingProbe{}
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), p, nil)
	runSupervisor(t, s)
	waitUntil(t, "active readiness", func() bool { return p.active.Load() == 1 })
	expectResult(t, request(t, s.Stop), nil)
	if p.active.Load() != 0 || s.Status().State != supervisor.StateStopped {
		t.Fatal("stop completed before child and readiness settled")
	}
}

func TestStopCancelsReadyHook(t *testing.T) {
	entered := make(chan struct{})
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, hookFunc(func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	}))
	runSupervisor(t, s)
	<-entered
	expectResult(t, request(t, s.Stop), nil)
}

func TestStartWaitsForStopAndReplacementReadiness(t *testing.T) {
	var builds atomic.Int32
	childReady := make(chan struct{}, 2)
	releaseReplacement := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseReplacement) }) })
	b := builderFunc(func(ctx context.Context) (*exec.Cmd, error) {
		builds.Add(1)
		ready := make(chan struct{})
		go func() {
			select {
			case <-ready:
				childReady <- struct{}{}
			case <-ctx.Done():
			}
		}()
		return testChild(ready), nil
	})
	p := probeFunc(func(ctx context.Context) error {
		select {
		case <-childReady:
		case <-ctx.Done():
			return ctx.Err()
		}
		if builds.Load() > 1 {
			select {
			case <-releaseReplacement:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	s := supervisor.New(supervisor.NewLauncher(b), p, nil, supervisor.WithStopTimeout(150*time.Millisecond))
	runSupervisor(t, s)
	waitUntil(t, "running", func() bool { return s.Status().State == supervisor.StateRunning })
	stopped := request(t, s.Stop)
	waitUntil(t, "stopping", func() bool { return s.Status().State == supervisor.StateStopping })
	started := request(t, s.Start)
	waitUntil(t, "run intent during stop", func() bool { return s.Status().Intent == supervisor.IntentRun })
	if builds.Load() != 1 {
		t.Fatal("replacement launched before previous child exited")
	}
	expectResult(t, stopped, nil)
	waitUntil(t, "replacement launch", func() bool { return builds.Load() == 2 })
	select {
	case err := <-started:
		t.Fatalf("Start completed before replacement readiness: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(releaseReplacement) })
	expectResult(t, started, nil)
}

func TestRequestsCoalesceWhileBuilderIsBlocked(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var builds atomic.Int32
	s := supervisor.New(supervisor.NewLauncher(builderFunc(func(context.Context) (*exec.Cmd, error) {
		if builds.Add(1) == 1 {
			close(entered)
			<-release // Deliberately slow to acknowledge cancellation.
		}
		return exec.Command("sleep", "100"), nil
	})), &immediateProbe{}, nil)
	runSupervisor(t, s)
	<-entered
	// Each call times out waiting, but must durably record its intent without
	// being blocked behind startup or a full wake channel.
	for range 30 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		err := s.Stop(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Stop while builder blocked: %v", err)
		}
	}
	started := request(t, s.Start)
	waitUntil(t, "latest run intent", func() bool { return s.Status().Intent == supervisor.IntentRun })
	releaseOnce.Do(func() { close(release) })
	expectResult(t, started, nil)
	if builds.Load() != 2 {
		t.Fatalf("expected cancelled build and replacement, got %d builds", builds.Load())
	}
}

func TestStopSupersedesPendingStart(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &trackingProbe{}, nil)
	runSupervisor(t, s)
	waitUntil(t, "starting", func() bool { return s.Status().State == supervisor.StateStarting })
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Start(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("initial start wait: %v", err)
	}
	// Start's timed-out wait must not retract its intent.
	if s.Status().Intent != supervisor.IntentRun {
		t.Fatal("caller timeout retracted run intent")
	}
	started := registeredRequest(t, s.Start)
	expectResult(t, request(t, s.Stop), nil)
	expectResult(t, started, supervisor.ErrSuperseded)
}

func TestCallsAfterShutdownFailPromptly(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, nil)
	cancel, done := runSupervisor(t, s)
	waitUntil(t, "running", func() bool { return s.Status().State == supervisor.StateRunning })
	cancel()
	<-done
	expectResult(t, request(t, s.Start), supervisor.ErrSupervisorClosed)
	expectResult(t, request(t, s.Stop), supervisor.ErrSupervisorClosed)
}

func TestStopStartBeforeRunPreservesOrderingWithFullWakeChannel(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, nil)
	// No loop is consuming wake events, so the doorbell remains full.
	first := registeredRequest(t, s.Start)
	stops := make([]<-chan error, 20)
	for i := range stops {
		stops[i] = registeredRequest(t, s.Stop)
	}
	last := registeredRequest(t, s.Start)
	expectResult(t, first, supervisor.ErrSuperseded)
	runSupervisor(t, s)
	expectResult(t, last, nil)
	// Stop completions survive even if the caller observes them only after
	// the replacement has reached running and the stopped interval is over.
	for _, stopped := range stops {
		expectResult(t, stopped, nil)
	}
}

func TestReplacementWaitsForCancelledWorkerToRetire(t *testing.T) {
	var builds, probes atomic.Int32
	cancelled, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	b := builderFunc(func(context.Context) (*exec.Cmd, error) {
		builds.Add(1)
		return exec.Command("sleep", "100"), nil
	})
	p := probeFunc(func(ctx context.Context) error {
		if probes.Add(1) == 1 {
			<-ctx.Done()
			close(cancelled)
			<-release
			return nil // A stale success must never mark the replacement ready.
		}
		return nil
	})
	s := supervisor.New(supervisor.NewLauncher(b), p, nil)
	runSupervisor(t, s)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	waitUntil(t, "probe", func() bool { return probes.Load() == 1 })
	stopped := registeredRequest(t, s.Stop)
	<-cancelled
	started := registeredRequest(t, s.Start)
	if builds.Load() != 1 {
		t.Fatal("replacement launched while old worker was active")
	}
	select {
	case err := <-stopped:
		t.Fatalf("stop completed before worker retired: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	expectResult(t, stopped, nil)
	expectResult(t, started, nil)
	if builds.Load() != 2 {
		t.Fatalf("expected replacement, got %d builds", builds.Load())
	}
}

func TestTimedOutStopStillTerminatesChild(t *testing.T) {
	ready := make(chan struct{})
	s := supervisor.New(supervisor.NewLauncher(builderFunc(func(context.Context) (*exec.Cmd, error) {
		return testChild(ready), nil
	})), probeFunc(func(ctx context.Context) error {
		select {
		case <-ready:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}), nil, supervisor.WithStopTimeout(100*time.Millisecond))
	runSupervisor(t, s)
	waitUntil(t, "running", func() bool { return s.Status().State == supervisor.StateRunning })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := s.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop: %v", err)
	}
	waitUntil(t, "stopped despite caller timeout", func() bool { return s.Status().State == supervisor.StateStopped })
}

func TestAlreadyCancelledRequestDoesNotChangeIntent(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Stop(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Stop: %v", err)
	}
	if s.Status().Intent != supervisor.IntentRun {
		t.Fatal("rejected stop changed intent")
	}
}

func TestRunIsSingleUse(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, nil)
	cancel, done := runSupervisor(t, s)
	waitUntil(t, "running", func() bool { return s.Status().State == supervisor.StateRunning })
	second := make(chan struct{})
	go func() { s.Run(context.Background()); close(second) }()
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("second Run did not return")
	}
	cancel()
	<-done
	s.Run(context.Background())
}

func TestCancelledBuildCannotLaunchItsCommand(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "launched")
	ctx, cancel := context.WithCancel(context.Background())
	launcher := supervisor.NewLauncher(builderFunc(func(context.Context) (*exec.Cmd, error) {
		cancel()
		return exec.Command("touch", marker), nil
	}))
	proc, err := launcher.Launch(ctx)
	if !errors.Is(err, context.Canceled) || proc != nil {
		t.Fatalf("cancelled launch returned %v, %v", proc, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled command ran: %v", err)
	}
}

func TestConcurrentRequestsCompleteWithSlowWatcher(t *testing.T) {
	s := supervisor.New(supervisor.NewLauncher(longRunningBuilder()), &immediateProbe{}, nil)
	runSupervisor(t, s)
	_, unsubscribe := s.Watch() // Deliberately never read; its buffer will fill.
	defer unsubscribe()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errs := make(chan error, 8)
	for range 8 {
		go func() {
			for range 15 {
				if err := s.Stop(ctx); err != nil {
					errs <- err
					return
				}
				if err := s.Start(ctx); err != nil && !errors.Is(err, supervisor.ErrSuperseded) {
					errs <- err
					return
				}
			}
			errs <- nil
		}()
	}
	for range 8 {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent request: %v", err)
		}
	}
	expectResult(t, request(t, s.Stop), nil)
	expectResult(t, request(t, s.Start), nil)
}

func TestShutdownWaitsForStartupRetirementAndRejectsRequests(t *testing.T) {
	entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	s := supervisor.New(supervisor.NewLauncher(builderFunc(func(ctx context.Context) (*exec.Cmd, error) {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return exec.Command("sleep", "100"), nil
	})), &immediateProbe{}, nil)
	cancel, done := runSupervisor(t, s)
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	<-entered
	started := registeredRequest(t, s.Start)
	cancel()
	<-cancelled
	expectResult(t, started, supervisor.ErrSupervisorClosed)
	expectResult(t, request(t, s.Start), supervisor.ErrSupervisorClosed)
	select {
	case <-done:
		t.Fatal("Run returned while startup work was still active")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not retire cancelled startup")
	}
	if s.Status().State != supervisor.StateStopped {
		t.Fatal("shutdown did not publish stopped")
	}
}
