package supervisor_test

import (
	"context"
	"errors"
	"os/exec"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"mysql-controller/supervisor"
)

var _ = Describe("Supervisor", func() {
	var (
		sup     *supervisor.Supervisor
		ctx     context.Context
		cancel  context.CancelFunc
		builder *stubBuilder
		probe   supervisor.ReadinessProbe
		hook    *spyHook
	)

	BeforeEach(func() {
		hook = &spyHook{}
		builder = longRunningBuilder()
		probe = &immediateProbe{}
	})

	JustBeforeEach(func() {
		launcher := supervisor.NewLauncher(builder)
		sup = supervisor.New(launcher, probe, hook)
		ctx, cancel = context.WithCancel(context.Background())
		DeferCleanup(cancel)
		go sup.Run(ctx)
	})

	// ─── Happy path ──────────────────────────────────────────────────────────

	Describe("normal startup", func() {
		It("reaches StateRunning", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateRunning))
		})

		It("calls the OnReady hook exactly once", func() {
			Eventually(hook.callCount, "5s").Should(Equal(1))
		})
	})

	// ─── Hook fault tolerance ─────────────────────────────────────────────────

	Describe("OnReady hook failure", func() {
		// The hook failing should be logged and ignored — mysqld is running and
		// the supervisor should not crash or restart because of a hook error.
		BeforeEach(func() {
			hook.err = errFixture("hook failed")
		})

		It("is non-fatal — supervisor still reaches StateRunning", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateRunning))
		})
	})

	// ─── Graceful shutdown ────────────────────────────────────────────────────

	Describe("graceful shutdown", func() {
		It("stops the running process and reaches StateStopped when ctx is cancelled", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateRunning))
			cancel()
			Eventually(sup.Status, "10s").Should(HaveField("State", supervisor.StateStopped))
		})
	})

	// ─── Crash recovery ───────────────────────────────────────────────────────

	Describe("crash and restart", func() {
		// The first launch exits immediately (cleanly); subsequent launches stay up.
		// Simulates an unexpected mysqld exit while the supervisor is running.
		BeforeEach(func() {
			builder = exitsThenRunsBuilder()
		})

		It("transitions through StateCrashed then recovers to StateRunning", func() {
			// First process exits → crash
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateCrashed))
			// After backoff the supervisor restarts and recovers
			Eventually(sup.Status, "10s").Should(HaveField("State", supervisor.StateRunning))
			Expect(sup.Status().RestartCount).To(BeNumerically(">=", 1))
		})
	})

	// ─── Launch failure ───────────────────────────────────────────────────────

	Describe("launch failure", func() {
		// Build() fails twice then succeeds — simulates a transient environment
		// error (e.g. binary not yet available) that clears on retry.
		BeforeEach(func() {
			builder = failsThenRunsBuilder(2)
		})

		It("retries after each StateCrashed and eventually reaches StateRunning", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateCrashed))
			// Two failures → two backoff periods (1s + 2s) before third attempt succeeds.
			// RestartCount is NOT incremented for build failures — only for unexpected
			// process exits — so we check LastError instead of RestartCount here.
			Eventually(sup.Status, "15s").Should(HaveField("State", supervisor.StateRunning))
			Expect(sup.Status().LastError).To(MatchError(ContainSubstring("simulated build failure")))
		})
	})

	// ─── Stop during backoff ─────────────────────────────────────────────────

	Describe("Stop during crash-restart backoff", func() {
		// A process that always exits immediately drives the supervisor into a
		// crash-restart loop, causing it to sleep in doBackoff between attempts.
		BeforeEach(func() {
			builder = &stubBuilder{fn: func() (*exec.Cmd, error) {
				return exec.Command("true"), nil
			}}
		})

		It("wakes the backoff immediately rather than waiting for the timer to expire", func() {
			// Poll frequently so we catch StateCrashed while the 1 s initial
			// backoff is still sleeping.
			Eventually(sup.Status, "5s", "10ms").Should(
				HaveField("State", supervisor.StateCrashed))

			// Stop with a deadline shorter than the ≥1 s backoff.
			// With the fix, doBackoff wakes on the intent change and Stop
			// returns in milliseconds. Without it Stop blocks for the full
			// backoff duration and exceeds this deadline.
			stopCtx, stopCancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer stopCancel()

			Expect(sup.Stop(stopCtx)).To(Succeed())
			Expect(sup.Status().State).To(Equal(supervisor.StateStopped))
		})
	})

	// ─── Probe goroutine cleanup ──────────────────────────────────────────────

	Describe("probe goroutine cleanup on pre-ready exit", func() {
		// When the process exits before the readiness probe passes, waitReady
		// must cancel the probe goroutine promptly via its child context.
		// Without the fix, active stays non-zero until ctx is cancelled (the
		// whole supervisor lifetime); with it, active → 0 within milliseconds.
		It("terminates the WaitReady goroutine promptly after the process exits", func() {
			probe2 := &trackingProbe{}
			launcher2 := supervisor.NewLauncher(&stubBuilder{fn: func() (*exec.Cmd, error) {
				return exec.Command("true"), nil
			}})
			sup2 := supervisor.New(launcher2, probe2, nil)

			ctx2, cancel2 := context.WithCancel(context.Background())
			DeferCleanup(cancel2)
			go sup2.Run(ctx2)

			Eventually(sup2.Status, "5s").Should(HaveField("State", supervisor.StateCrashed))
			Eventually(probe2.active.Load, "500ms").Should(BeZero())
		})
	})

	// ─── Pre-ready exit ───────────────────────────────────────────────────────

	Describe("process exits before the readiness probe passes", func() {
		// The readiness probe blocks indefinitely; the process wins the race
		// by exiting immediately. This verifies that the supervisor correctly
		// detects a pre-ready exit rather than waiting for the probe forever.
		BeforeEach(func() {
			builder = &stubBuilder{fn: func() (*exec.Cmd, error) {
				return exec.Command("true"), nil
			}}
			probe = newBlockingProbe() // never triggered — process will always exit first
		})

		It("reaches StateCrashed without ever calling the OnReady hook", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateCrashed))
			// OnReady is only called after the readiness probe passes — it should
			// never have been called if the process exited before becoming ready.
			Expect(hook.callCount()).To(Equal(0))
		})
	})

	// ─── Stop / Start API ─────────────────────────────────────────────────────

	Describe("Stop / Start API", func() {
		JustBeforeEach(func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateRunning))
		})

		Describe("Stop", func() {
			It("signals the running process, blocks until stopped, and returns nil", func() {
				stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
				defer stopCancel()

				Expect(sup.Stop(stopCtx)).To(Succeed())
				Expect(sup.Status().State).To(Equal(supervisor.StateStopped))
			})

			It("is idempotent when called on an already-stopped supervisor", func() {
				stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
				defer stopCancel()
				Expect(sup.Stop(stopCtx)).To(Succeed())

				// Second Stop should return immediately — nothing to signal.
				stopCtx2, stopCancel2 := context.WithTimeout(ctx, 2*time.Second)
				defer stopCancel2()
				Expect(sup.Stop(stopCtx2)).To(Succeed())
			})
		})

		Describe("Start after Stop", func() {
			JustBeforeEach(func() {
				stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
				defer stopCancel()
				Expect(sup.Stop(stopCtx)).To(Succeed())
			})

			It("launches a new process and reaches StateRunning", func() {
				startCtx, startCancel := context.WithTimeout(ctx, 10*time.Second)
				defer startCancel()

				Expect(sup.Start(startCtx)).To(Succeed())
				Expect(sup.Status().State).To(Equal(supervisor.StateRunning))
			})
		})

		Describe("Stop respects ctx cancellation", func() {
			It("returns ctx.Err() when the context expires before the process stops", func() {
				// Use a probe that never passes so the process never starts — we then
				// Stop with a context that expires before the run loop sets StateStopped.
				// This validates that waitForState doesn't hang.
				//
				// In this sub-case we use a fresh supervisor with a blocking probe so
				// the supervisor is stuck in StateStarting, making it easy to race Stop.
				blockProbe := newBlockingProbe()
				launcher2 := supervisor.NewLauncher(longRunningBuilder())
				sup2 := supervisor.New(launcher2, blockProbe, nil)

				ctx2, cancel2 := context.WithCancel(context.Background())
				DeferCleanup(cancel2)
				go sup2.Run(ctx2)

				// Give StateStarting a moment to settle
				Eventually(sup2.Status, "3s").Should(HaveField("State", supervisor.StateStarting))

				// Stop with an already-expired context
				expiredCtx, expiredCancel := context.WithTimeout(ctx2, 0)
				defer expiredCancel()

				err := sup2.Stop(expiredCtx)
				Expect(err).To(MatchError(context.DeadlineExceeded))
			})
		})
	})

	// ─── SIGKILL escalation ───────────────────────────────────────────────────

	Describe("SIGKILL escalation when process ignores SIGTERM", func() {
		// A 200 ms stop timeout means SIGKILL fires almost immediately after
		// the unanswered SIGTERM. Stop must still return nil — the process died.
		It("kills the process after the stop timeout elapses and Stop returns nil", func() {
			launcher2 := supervisor.NewLauncher(sigTermIgnoringBuilder())
			sup2 := supervisor.New(launcher2, &immediateProbe{}, nil,
				supervisor.WithStopTimeout(200*time.Millisecond))

			ctx2, cancel2 := context.WithCancel(context.Background())
			DeferCleanup(cancel2)
			go sup2.Run(ctx2)

			Eventually(sup2.Status, "5s").Should(HaveField("State", supervisor.StateRunning))

			stopCtx, stopCancel := context.WithTimeout(ctx2, 5*time.Second)
			defer stopCancel()

			Expect(sup2.Stop(stopCtx)).To(Succeed())
			Expect(sup2.Status().State).To(Equal(supervisor.StateStopped))
		})
	})

	// ─── Stop during readiness probe ─────────────────────────────────────────

	Describe("Stop called while readiness probe is running", func() {
		// The supervisor is in StateStarting (probe not yet passed). Stop() sets
		// IntentStop. When the probe then passes, the supervisor must honour the
		// pending stop rather than transitioning to StateRunning.
		It("reaches StateStopped after the probe passes when IntentStop is already set", func() {
			blockProbe := newBlockingProbe()
			launcher2 := supervisor.NewLauncher(longRunningBuilder())
			sup2 := supervisor.New(launcher2, blockProbe, nil)

			ctx2, cancel2 := context.WithCancel(context.Background())
			DeferCleanup(cancel2)
			go sup2.Run(ctx2)

			Eventually(sup2.Status, "3s").Should(HaveField("State", supervisor.StateStarting))

			stopCtx, stopCancel := context.WithTimeout(ctx2, 5*time.Second)
			defer stopCancel()

			go func() { _ = sup2.Stop(stopCtx) }()

			// Wait for Stop to register IntentStop, then let the probe pass.
			Eventually(func() supervisor.Intent {
				return sup2.Status().Intent
			}, "3s").Should(Equal(supervisor.IntentStop))
			blockProbe.trigger()

			Eventually(sup2.Status, "5s").Should(HaveField("State", supervisor.StateStopped))
		})
	})

	// ─── cmd.Start() failure ─────────────────────────────────────────────────

	Describe("command Start() failure", func() {
		// Build() succeeds but the returned command refers to a non-existent
		// binary, so cmd.Start() fails inside Launch(). This exercises the
		// "starting mysqld" error-wrap path in launcher.go.
		BeforeEach(func() {
			builder = &stubBuilder{fn: func() (*exec.Cmd, error) {
				return exec.Command("/nonexistent-binary-for-supervisor-test"), nil
			}}
		})

		It("reaches StateCrashed and records the start error in LastError", func() {
			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateCrashed))
			Expect(sup.Status().LastError).To(MatchError(ContainSubstring("starting mysqld")))
		})
	})

	// ─── State.String ─────────────────────────────────────────────────────────

	Describe("State.String", func() {
		It("returns 'unknown' for an unrecognised State value", func() {
			Expect(supervisor.State(99).String()).To(Equal("unknown"))
		})
	})

	// ─── StateStopping transition ────────────────────────────────────────────

	Describe("StateStopping", func() {
		It("is delivered before StateStopped on the Watch channel", func() {
			// Subscribe before doing anything so we capture every transition.
			ch, unsubscribe := sup.Watch()
			defer unsubscribe()

			Eventually(sup.Status, "5s").Should(HaveField("State", supervisor.StateRunning))

			stopCtx, stopCancel := context.WithTimeout(ctx, 10*time.Second)
			defer stopCancel()
			go func() { _ = sup.Stop(stopCtx) }()

			// The channel receives every transition in order. StateStopping must
			// arrive before StateStopped — no process timing dependency, no polling.
			Eventually(ch, "5s").Should(Receive(Equal(supervisor.StateStopping)))
			Eventually(ch, "5s").Should(Receive(Equal(supervisor.StateStopped)))
		})
	})
})

// errFixture returns a simple sentinel error for use in test setup.
func errFixture(msg string) error {
	return errors.New(msg)
}
