package mysqld

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"slices"
)

type CommandBuilder struct {
	mysqldBinPath string
	baseArgs      []string
	resolver      Resolver
	output        io.Writer
}

type Resolver interface {
	Resolve(context.Context) ([]string, error)
}

func NewCommandBuilder(mysqldBinPath string, baseArgs []string, resolver Resolver, output io.Writer) *CommandBuilder {
	return &CommandBuilder{
		mysqldBinPath: mysqldBinPath,
		baseArgs:      baseArgs,
		resolver:      resolver,
		output:        output,
	}
}

func (b *CommandBuilder) Build(ctx context.Context) (*exec.Cmd, error) {
	extraArgs, err := b.resolver.Resolve(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving mysqld options: %w", err)
	}

	args := slices.Concat(b.baseArgs, extraArgs)

	cmd := exec.Command(b.mysqldBinPath, args...)
	cmd.Stdout = b.output
	cmd.Stderr = b.output

	return cmd, nil
}
