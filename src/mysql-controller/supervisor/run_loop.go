package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"syscall"
	"time"
)

type launchResult struct {
	proc *ProcessHandle
	err  error
}

// runLoop is owned exclusively by Run. A launch and a readiness worker each
// return exactly one buffered result. Their channels remain owned until consumed,
// even after cancellation, so a delayed result cannot escape into a new attempt.
type runLoop struct {
	s             *Supervisor
	generation    uint64
	attemptCtx    context.Context
	cancelAttempt context.CancelFunc
	launch        <-chan launchResult
	ready         <-chan error
	proc          *ProcessHandle
	stopping      bool
	failed        bool
	backoff       time.Duration
	retry         *time.Timer
	kill          *time.Timer
}

func (l *runLoop) run(ctx context.Context) {
	for {
		if l.reconcile(ctx) {
			return
		}
		var exit <-chan struct{}
		if l.proc != nil {
			exit = l.proc.Wait()
		}
		var shutdown <-chan struct{}
		l.s.mu.Lock()
		if !l.s.closing {
			shutdown = ctx.Done()
		}
		l.s.mu.Unlock()

		select {
		case <-l.s.wake:
		case <-shutdown:
		case result := <-l.launch:
			l.launch = nil
			if result.err != nil {
				if l.attemptCtx.Err() == nil {
					l.failure(result.err, false)
				}
			} else {
				l.proc = result.proc
				if l.attemptCtx.Err() == nil {
					l.waitReady()
				}
			}
		case err := <-l.ready:
			l.ready = nil
			if l.attemptCtx.Err() == nil {
				if err != nil {
					l.failure(err, false)
				} else {
					// Readiness and intent acceptance are linearized under the same lock:
					// a stop cannot be satisfied by stale running state from this attempt.
					l.s.mu.Lock()
					if !l.s.closing && ctx.Err() == nil && l.generation == l.s.stopGeneration {
						l.backoff = initialBackoff
						l.s.runningGeneration = l.generation
						l.s.setStateLocked(StateRunning)
						l.s.completeLocked(func(r *request) bool {
							return r.intent == IntentRun && r.generation == l.generation
						}, nil)
					}
					l.s.mu.Unlock()
				}
			}
		case <-exit:
			if !l.stopping && l.attemptCtx.Err() == nil {
				l.failure(l.proc.Err(), true)
			}
			l.cancelAttempt()
			l.proc = nil
			stopTimer(&l.kill)
		case <-timerChannel(l.retry):
			stopTimer(&l.retry)
		case <-timerChannel(l.kill):
			// Never clear the handle here: SIGKILL delivery is not exit confirmation.
			// If signalling fails, retain ownership and retry rather than launch over it.
			l.signal(syscall.SIGKILL)
			l.kill.Reset(l.s.stopTimeout)
		}
	}
}

// reconcile initiates work and returns to select; it never waits for a worker,
// readiness, backoff, or process exit. Only short state operations hold mu.
func (l *runLoop) reconcile(ctx context.Context) bool {
	s := l.s
	s.mu.Lock()
	if ctx.Err() != nil && !s.closing {
		s.closing = true
		s.status.Intent = IntentStop
		s.stopGeneration++
		s.completeLocked(func(r *request) bool { return r.intent == IntentRun }, ErrSupervisorClosed)
	}
	mustStop := s.closing || s.status.Intent == IntentStop || l.generation != s.stopGeneration
	s.mu.Unlock()

	if mustStop || l.failed {
		if l.cancelAttempt != nil {
			l.cancelAttempt()
		}
		if mustStop {
			stopTimer(&l.retry)
		}
		if l.proc != nil && !l.stopping {
			l.stopping = true
			s.mu.Lock()
			s.setStateLocked(StateStopping)
			s.mu.Unlock()
			l.signal(syscall.SIGTERM)
			l.kill = time.NewTimer(s.stopTimeout)
		}
	}

	if l.launch != nil || l.ready != nil || l.proc != nil {
		return false
	}
	// No launch, worker, or child can still produce effects. This is the only
	// point where a stop barrier can complete or a replacement can be reserved.
	if l.cancelAttempt != nil {
		l.cancelAttempt()
		l.cancelAttempt = nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l.failed && !s.closing && s.status.Intent == IntentRun && l.generation == s.stopGeneration {
		s.setStateLocked(StateCrashed)
		l.retry = time.NewTimer(l.backoff)
		l.backoff = nextBackoff(l.backoff)
	} else if l.retry == nil {
		s.setStateLocked(StateStopped)
	}
	l.failed = false
	l.stopping = false
	s.completeLocked(func(r *request) bool { return r.intent == IntentStop && r.generation <= s.stopGeneration }, nil)
	if s.closing {
		return true
	}
	if s.status.Intent == IntentRun && l.retry == nil {
		l.generation = s.stopGeneration
		l.attemptCtx, l.cancelAttempt = context.WithCancel(ctx)
		results := make(chan launchResult, 1)
		l.launch = results
		s.setStateLocked(StateStarting)
		// Reserve the generation under mu, but do all command preparation and launch
		// outside it. A stop racing this reservation cancels and drains this attempt.
		go func(attemptCtx context.Context) {
			proc, err := s.launcher.Launch(attemptCtx)
			results <- launchResult{proc, err}
		}(l.attemptCtx)
	}
	return false
}

func (l *runLoop) waitReady() {
	results := make(chan error, 1)
	l.ready = results
	ctx := l.attemptCtx
	go func() {
		err := l.s.probe.WaitReady(ctx)
		if err == nil && ctx.Err() == nil && l.s.onReady != nil {
			if hookErr := l.s.onReady.OnReady(ctx); hookErr != nil {
				slog.Warn("OnReady hook failed", "err", hookErr)
			}
		}
		if err == nil {
			err = ctx.Err()
		}
		results <- err
	}()
}

func (l *runLoop) failure(err error, exited bool) {
	l.s.mu.Lock()
	defer l.s.mu.Unlock()
	// A stop accepted before we observed this failure already owns the exit.
	if l.s.closing || l.s.status.Intent == IntentStop || l.generation != l.s.stopGeneration {
		return
	}
	l.failed = true
	if err == nil {
		err = errors.New("process exited unexpectedly")
	}
	l.s.status.LastError = err
	if exited {
		l.s.status.RestartCount++
		l.s.setStateLocked(StateCrashed)
	}
}

func (l *runLoop) signal(sig os.Signal) {
	if err := l.proc.Signal(sig); err != nil && !errors.Is(err, ErrProcessNotRunning) {
		l.s.mu.Lock()
		l.s.status.LastError = err
		l.s.mu.Unlock()
		slog.Error("signalling child", "signal", sig, "err", err)
	}
}

func timerChannel(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}
func stopTimer(t **time.Timer) {
	if *t != nil {
		(*t).Stop()
		*t = nil
	}
}
