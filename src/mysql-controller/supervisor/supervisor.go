package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

const (
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
)

// State represents the current lifecycle state of the supervised process.
type State int

const (
	StateStopped  State = iota
	StateStarting       // process launched, readiness probe not yet passed
	StateRunning        // readiness probe passed
	StateStopping       // stop requested, waiting for process exit
	StateCrashed        // unexpected exit, backoff before restart
)

func (s State) String() string {
	switch s {
	case StateStopped:
		return "stopped"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateCrashed:
		return "crashed"
	default:
		return "unknown"
	}
}

// Intent represents what the supervisor has been asked to do.
type Intent int

const (
	IntentRun  Intent = iota // keep the process running
	IntentStop               // keep the process stopped
)

// Status is a snapshot of supervisor state at a point in time.
type Status struct {
	State        State
	Intent       Intent
	RestartCount int
	LastError    error
	Since        time.Time
}

// ReadinessProbe reports when the process is ready to serve traffic.
type ReadinessProbe interface {
	WaitReady(ctx context.Context) error
}

// OnReadyHook is called after the readiness probe passes on each
// successful start. Used to update persistent state after a healthy
// process start — e.g. writing the Galera state file.
// A nil OnReadyHook is a no-op.
type OnReadyHook interface {
	OnReady(ctx context.Context) error
}

// Supervisor manages the lifecycle of a child process, restarting it
// on crash with exponential backoff. It exposes Start/Stop for
// explicit lifecycle control and Watch for observing state transitions.
//
// Call Run in a goroutine; it blocks until ctx is cancelled.
// Intent defaults to IntentRun — the process starts immediately on Run.
type Supervisor struct {
	launcher *Launcher
	probe    ReadinessProbe
	onReady  OnReadyHook

	mu           sync.Mutex
	state        State
	intentVal    Intent
	intentCh     chan struct{} // closed when intent changes; replaced each time
	proc         *ProcessHandle
	since        time.Time
	restartCount int
	lastError    error
	watchers     []chan State
	// defines how long to wait for a graceful stop
	stopTimeout time.Duration
}

// New constructs a Supervisor. Call Run in a goroutine to start it.
// onReady may be nil if no post-ready hook is needed.
func New(launcher *Launcher, probe ReadinessProbe, onReady OnReadyHook) *Supervisor {
	return &Supervisor{
		launcher:    launcher,
		probe:       probe,
		onReady:     onReady,
		intentCh:    make(chan struct{}),
		since:       time.Now(),
		stopTimeout: 30 * time.Second,
	}
}

// Run starts the supervisor loop. Blocks until ctx is cancelled.
// The process is started immediately — intent defaults to IntentRun.
// Call in a goroutine from main.
func (s *Supervisor) Run(ctx context.Context) {
	s.run(ctx)

	// ctx cancelled — stop the process if it's running
	s.mu.Lock()
	proc := s.proc
	s.mu.Unlock()

	if proc != nil {
		s.stopProcess(proc)
	}
	s.setState(StateStopped)
}

// Start sets intent to IntentRun and blocks until StateRunning or ctx expires.
// No-op if already running.
func (s *Supervisor) Start(ctx context.Context) error {
	s.setIntent(IntentRun)
	return s.waitForState(ctx, StateRunning)
}

// Stop sets intent to IntentStop, signals the process, and blocks until
// StateStopped or ctx expires.
func (s *Supervisor) Stop(ctx context.Context) error {
	s.setIntent(IntentStop)
	return s.waitForState(ctx, StateStopped)
}

// Status returns a snapshot of the current supervisor state.
func (s *Supervisor) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{
		State:        s.state,
		Intent:       s.intentVal,
		RestartCount: s.restartCount,
		LastError:    s.lastError,
		Since:        s.since,
	}
}

// Watch returns a channel that receives each State transition in order.
// The channel is buffered; if the consumer is too slow, transitions may be
// dropped (the run loop does not block on slow watchers).
// Call the returned unsubscribe func when done — safe to call multiple times.
func (s *Supervisor) Watch() (<-chan State, func()) {
	return s.subscribeState()
}

// ----------------------------------------------------------------------------
// Internal
// ----------------------------------------------------------------------------

func (s *Supervisor) run(ctx context.Context) {
	backoff := initialBackoff

	for {
		// If stopped intentionally, wait for intent to change or ctx to cancel.
		if s.intent() == IntentStop {
			s.setState(StateStopped)
			intentCh := s.currentIntentCh()
			select {
			case <-intentCh:
				// intent changed — loop again
				continue
			case <-ctx.Done():
				return
			}
		}

		// Launch
		s.setState(StateStarting)
		proc, err := s.launcher.Launch(ctx)
		if err != nil {
			slog.Error("failed to launch process", "err", err)
			s.setLastError(err)
			s.setState(StateCrashed)
			if !s.doBackoff(ctx, backoff) {
				return // ctx cancelled
			}
			backoff = nextBackoff(backoff)
			continue
		}

		s.mu.Lock()
		s.proc = proc
		s.mu.Unlock()

		// Wait for ready, racing against premature process exit
		if err := s.waitReady(ctx, proc); err != nil {
			slog.Error("process failed to become ready", "err", err)
			s.setLastError(err)
			// Ensure process is gone before we retry
			s.stopProcess(proc)
			s.setState(StateCrashed)
			if !s.doBackoff(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}

		// waitReady does not select on intentCh, so a Stop call that arrived
		// while the probe was running will have set intentVal=IntentStop and
		// replaced intentCh with a fresh (never-closed) channel.  Check
		// intent now and honour a pending stop before entering the running
		// select — otherwise we block on the new intentCh forever.
		if s.intent() == IntentStop {
			s.stopProcess(proc)
			s.mu.Lock()
			s.proc = nil
			s.mu.Unlock()
			s.setState(StateStopped)
			nextIntentCh := s.currentIntentCh()
			select {
			case <-nextIntentCh:
				continue
			case <-ctx.Done():
				return
			}
		}

		// Notify hook — non-fatal if it fails, MySQL is running
		if s.onReady != nil {
			if err := s.onReady.OnReady(ctx); err != nil {
				slog.Warn("OnReady hook failed", "err", err)
			}
		}

		backoff = initialBackoff
		s.setState(StateRunning)

		intentCh := s.currentIntentCh()
		// Block until process exits or ctx cancelled
		select {
		case <-intentCh:
			// intent changed while running — check if we should stop
			if s.intent() != IntentStop {
				continue
			}
			s.stopProcess(proc)
			s.mu.Lock()
			s.proc = nil
			s.mu.Unlock()
			s.setState(StateStopped)
			// wait for intent to flip back to IntentRun
			nextIntentCh := s.currentIntentCh()
			select {
			case <-nextIntentCh:
				continue
			case <-ctx.Done():
				return
			}
		case <-proc.Wait():
			// unexpected exit; fallthrough to crash handling below
		case <-ctx.Done():
			return
		}

		s.mu.Lock()
		s.proc = nil
		s.mu.Unlock()

		if s.intent() == IntentStop {
			s.setState(StateStopped)
			// Wait for intent to change before looping
			intentCh := s.currentIntentCh()
			select {
			case <-intentCh:
				continue
			case <-ctx.Done():
				return
			}
		}

		// Unexpected exit — crashed
		slog.Warn("process exited unexpectedly",
			"err", proc.Err(),
			"restart_count", s.restartCount+1,
		)
		s.setLastError(proc.Err())
		s.setState(StateCrashed)
		s.mu.Lock()
		s.restartCount++
		s.mu.Unlock()

		if !s.doBackoff(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff)
	}
}

// waitReady races the readiness probe against premature process exit.
// Returns nil when the probe passes. Returns an error if the process
// exits before becoming ready, or if ctx is cancelled.
func (s *Supervisor) waitReady(ctx context.Context, proc *ProcessHandle) error {
	readyCh := make(chan error, 1)
	go func() {
		readyCh <- s.probe.WaitReady(ctx)
	}()

	select {
	case err := <-readyCh:
		return err
	case <-proc.Wait():
		return fmt.Errorf("process exited before becoming ready: %w", proc.Err())
	case <-ctx.Done():
		return ctx.Err()
	}
}

// doBackoff sleeps for d, returning true if the sleep completed or an intent
// change arrived, and false if ctx was cancelled.
//
// Subscribe-then-check: intentCh is captured before reading intentVal so that
// any setIntent call that races with the check is guaranteed to either be
// visible in the intent read (if it completed before the read) or to close the
// captured channel (if it completes after). This matches the pattern used by
// waitForState and is race-free.
func (s *Supervisor) doBackoff(ctx context.Context, d time.Duration) bool {
	slog.Info("backing off before restart", "duration", d)
	intentCh := s.currentIntentCh()
	if s.intent() == IntentStop {
		return true
	}
	select {
	case <-time.After(d):
		return true
	case <-intentCh:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func (s *Supervisor) subscribeState() (ch <-chan State, unsubscribe func()) {
	c := make(chan State, 8)
	s.mu.Lock()
	s.watchers = append(s.watchers, c)
	s.mu.Unlock()

	return c, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, w := range s.watchers {
			if w == c {
				s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
				return
			}
		}
	}
}

// waitForState blocks until the supervisor reaches the target state
// or ctx expires.
func (s *Supervisor) waitForState(ctx context.Context, target State) error {
	ch, unsubscribe := s.subscribeState()
	defer unsubscribe()

	// Check state AFTER subscribing — any transition from this point
	// forward will be delivered to ch, so the check and the receive
	// are now race-free.
	s.mu.Lock()
	if s.state == target {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()

	for {
		select {
		case state := <-ch:
			if state == target {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// stopProcess transitions to StateStopping and stops proc gracefully,
// escalating to SIGKILL if the stop timeout elapses.
func (s *Supervisor) stopProcess(proc *ProcessHandle) {
	stopCtx, cancel := context.WithTimeout(context.Background(), s.stopTimeout)
	defer cancel()
	s.setState(StateStopping)
	if err := proc.stopGracefully(stopCtx); err != nil {
		slog.Warn("error stopping process", "err", err)
	}
}

func (s *Supervisor) setState(state State) {
	s.mu.Lock()
	s.state = state
	s.since = time.Now()
	watchers := make([]chan State, len(s.watchers))
	copy(watchers, s.watchers)
	s.mu.Unlock()

	slog.Info("supervisor state changed", "state", state)

	for _, ch := range watchers {
		select {
		case ch <- state:
		default:
			// slow consumer — drop rather than block the run loop
		}
	}
}

func (s *Supervisor) setLastError(err error) {
	s.mu.Lock()
	s.lastError = err
	s.mu.Unlock()
}

func (s *Supervisor) intent() Intent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intentVal
}

func (s *Supervisor) setIntent(intent Intent) {
	s.mu.Lock()
	s.intentVal = intent
	old := s.intentCh
	s.intentCh = make(chan struct{})
	s.mu.Unlock()

	close(old) // wake anyone selecting on the old channel
}

func (s *Supervisor) currentIntentCh() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.intentCh
}
