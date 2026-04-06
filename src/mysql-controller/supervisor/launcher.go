package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
)

// Launcher holds the static configuration for launching a process
type Launcher struct {
	// CommandFactory is a function that returns an *exec.Cmd instance
	// This command will be started by the launcher
	CommandFactory CommandBuilder
}

type CommandBuilder interface {
	Build(context.Context) (*exec.Cmd, error)
}

func NewLauncher(cmdFactory CommandBuilder) *Launcher {
	return &Launcher{
		CommandFactory: cmdFactory,
	}
}

// Launch starts mysqld and returns a ProcessHandle.
//
// extraArgs are appended to BaseArgs at call time — typically the
// output of galera.Resolver.ResolveArgs(). nil is safe and treated
// as no extra args.
//
// Launch returns as soon as the process is started. It does NOT wait
// for mysqld to be ready to accept connections — that is the
// responsibility of the readiness probe.
//
// The context passed here should be a long-lived background context,
// not an HTTP request context. exec.CommandContext cancellation sends
// SIGKILL, bypassing the graceful shutdown path. Graceful shutdown is
// handled explicitly via ProcessHandle.stopGracefully.
//
// mysqld is started without Setpgid. It stays in the supervisor's
// process group so that BPM's SIGTERM to PID 1 (the supervisor) is
// NOT automatically forwarded to mysqld by the kernel. The supervisor
// explicitly stops mysqld via stopGracefully before exiting, giving
// InnoDB a clean shutdown window. Setpgid would detach mysqld from
// the process group but BPM signals processes in the container
// directly anyway, so it provides no benefit and makes the shutdown
// path less explicit.
func (l *Launcher) Launch(ctx context.Context) (*ProcessHandle, error) {
	cmd, err := l.CommandFactory.Build(ctx)
	if err != nil {
		return nil, err
	}

	slog.Info("launching process",
		slog.String("path", cmd.Path),
		slog.Any("args", cmd.Args),
	)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("starting mysqld: %w", err)
	}

	slog.Info("process started", "pid", cmd.Process.Pid)

	h := &ProcessHandle{
		cmd:    cmd,
		exitCh: make(chan struct{}),
	}
	h.reap()

	return h, nil
}
