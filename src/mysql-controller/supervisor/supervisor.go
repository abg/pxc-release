package supervisor

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
)

var (
	// ErrSuperseded means a Stop was accepted before this Start completed.
	ErrSuperseded = errors.New("start superseded by stop")
	// ErrSupervisorClosed means Run has begun shutting down or has returned.
	ErrSupervisorClosed = errors.New("supervisor closed")
)

// State represents the current lifecycle state of the supervised process.
type State int

const (
	StateStopped State = iota
	StateStarting
	StateRunning
	StateStopping
	StateCrashed
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

// Intent represents the desired state of the child process.
type Intent int

const (
	IntentRun Intent = iota
	IntentStop
)

// Status is a consistent snapshot of the supervisor's published state.
type Status struct {
	State        State
	Intent       Intent
	RestartCount int
	LastError    error
	Since        time.Time
}

// ReadinessProbe reports when the child is ready. It must return promptly when
// ctx is cancelled. No replacement is launched until the old probe has returned.
type ReadinessProbe interface{ WaitReady(context.Context) error }

// OnReadyHook runs after readiness on each successful start. A nil hook is a
// no-op; hook errors are logged and remain non-fatal. Hooks must return promptly
// on cancellation. Stop waits for the hook as well as the child to settle.
type OnReadyHook interface{ OnReady(context.Context) error }

type request struct {
	intent     Intent
	generation uint64
	done       chan error
}

// Supervisor reconciles durable intent with a single owned startup attempt and
// child. Only Run performs lifecycle work. The permanent, buffered wake channel
// is a doorbell: notifications may coalesce, but desired state and stop barriers
// remain protected by mu. No lifecycle work or caller wait holds mu.
//
// Intent defaults to Run. Requests before Run are accepted and wait for Run to
// acknowledge them. Run may be invoked only once; subsequent invocations return.
type Supervisor struct {
	launcher    *Launcher
	probe       ReadinessProbe
	onReady     OnReadyHook
	stopTimeout time.Duration
	wake        chan struct{}

	mu     sync.Mutex
	status Status
	// Each accepted stop invalidates earlier attempts, even if a later Start
	// restores run intent before the loop has observed the stop.
	stopGeneration    uint64
	runningGeneration uint64
	started           bool
	closing           bool
	waiters           map[*request]struct{}
	watchers          map[chan State]struct{}
}

type Option func(*Supervisor)

// WithStopTimeout sets the grace period between SIGTERM and SIGKILL.
func WithStopTimeout(d time.Duration) Option {
	return func(s *Supervisor) { s.stopTimeout = d }
}

func New(launcher *Launcher, probe ReadinessProbe, onReady OnReadyHook, opts ...Option) *Supervisor {
	s := &Supervisor{
		launcher: launcher, probe: probe, onReady: onReady,
		stopTimeout: 30 * time.Second, wake: make(chan struct{}, 1),
		status:  Status{Since: time.Now()},
		waiters: make(map[*request]struct{}), watchers: make(map[chan State]struct{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run owns the lifecycle loop until ctx is cancelled and all startup work and
// the child have settled. Shutdown rejects new requests and supersedes starts;
// pending stops still complete when their termination barriers are satisfied.
func (s *Supervisor) Run(ctx context.Context) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()
	(&runLoop{s: s, backoff: initialBackoff}).run(ctx)
}

// Start requests running and waits for a ready child satisfying all previously
// accepted stops. Repeated starts are idempotent. A later Stop returns
// ErrSuperseded to pending starts. Context cancellation cancels only the wait;
// accepted intent remains in force. An already-cancelled context is rejected.
func (s *Supervisor) Start(ctx context.Context) error { return s.request(ctx, IntentRun) }

// Stop commits the current startup attempt/child to termination and waits for
// confirmed exit and retirement of startup work. A later Start waits behind this
// barrier and cannot undo it. Context cancellation cancels only the caller's wait.
func (s *Supervisor) Stop(ctx context.Context) error { return s.request(ctx, IntentStop) }

func (s *Supervisor) request(ctx context.Context, intent Intent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrSupervisorClosed
	}
	s.status.Intent = intent
	if intent == IntentStop {
		s.stopGeneration++
		s.completeLocked(func(r *request) bool { return r.intent == IntentRun }, ErrSuperseded)
	}
	if intent == IntentRun && s.status.State == StateRunning && s.runningGeneration == s.stopGeneration {
		s.mu.Unlock()
		return nil
	}
	r := &request{intent: intent, generation: s.stopGeneration, done: make(chan error, 1)}
	s.waiters[r] = struct{}{}
	s.mu.Unlock()
	// State is recorded before ringing. A full channel already guarantees a
	// future reconciliation; the loop never drains notifications after a read.
	select {
	case s.wake <- struct{}{}:
	default:
	}
	defer func() { s.mu.Lock(); delete(s.waiters, r); s.mu.Unlock() }()
	select {
	case err := <-r.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Supervisor) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Watch observes transitions in order, with best-effort delivery to slow
// consumers. It is independent of reliable Start/Stop completion. Unsubscribe
// is idempotent; channels are not closed on unsubscribe or shutdown.
func (s *Supervisor) Watch() (<-chan State, func()) {
	ch := make(chan State, 8)
	s.mu.Lock()
	s.watchers[ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() { s.mu.Lock(); delete(s.watchers, ch); s.mu.Unlock() }
}

// The caller holds mu for these publication/completion operations. Sends never
// block, and each private completion channel is written exactly once.
func (s *Supervisor) setStateLocked(state State) {
	if s.status.State == state {
		return
	}
	s.status.State = state
	s.status.Since = time.Now()
	for ch := range s.watchers {
		select {
		case ch <- state:
		default:
		}
	}
}
func (s *Supervisor) completeLocked(matches func(*request) bool, err error) {
	for r := range s.waiters {
		if matches(r) {
			r.done <- err
			delete(s.waiters, r)
		}
	}
}

func nextBackoff(d time.Duration) time.Duration {
	return min(d*2, maxBackoff)
}
