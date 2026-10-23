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
	// Build must return promptly on cancellation and must not start the command.
	// The command must not use exec.CommandContext: the supervisor owns signals.
	Build(context.Context) (*exec.Cmd, error)
}

func NewLauncher(cmdFactory CommandBuilder) *Launcher {
	return &Launcher{
		CommandFactory: cmdFactory,
	}
}

// Launch starts mysqld and returns a ProcessHandle.
//
// Launch returns as soon as the process is started. It does NOT wait
// for mysqld to be ready to accept connections — that is the
// responsibility of the readiness probe.
//
// Cancellation aborts command preparation. If it races cmd.Start, Launch still
// returns the child handle so the supervisor can terminate and reap it.
func (l *Launcher) Launch(ctx context.Context) (*ProcessHandle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd, err := l.CommandFactory.Build(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
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
