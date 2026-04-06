package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"sync"
	"time"

	"mysql-controller/galera"
	"mysql-controller/mysqld"
	"mysql-controller/supervisor"
)

type Config struct {
	StatePath          string
	GraStateFile       string
	IsBootstrapNode    bool
	MySQLServerBinPath string
	MySQLServerArgs    []string
	MySQLDSN           string
	PollReadyInterval  time.Duration
}

func main() {
	ctx := context.Background()
	var cfg Config

	var (
		grastateFile    = cfg.GraStateFile
		isBootstrapNode = cfg.IsBootstrapNode
		mysqldPath      = cfg.MySQLServerBinPath
		mysqldBaseArgs  = cfg.MySQLServerArgs
		pollInterval    = cfg.PollReadyInterval
	)

	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		slog.Error("db init", "error", err)
		os.Exit(1)
	}

	stateFile := galera.NewStateFile(cfg.StatePath)
	prober := galera.NewClusterProber()
	resolver := galera.NewResolver(stateFile, prober, grastateFile, isBootstrapNode)
	builder := mysqld.NewCommandBuilder(mysqldPath, mysqldBaseArgs, resolver, os.Stderr)
	launcher := supervisor.NewLauncher(builder)
	probe := galera.NewReadyWaiter(db, pollInterval)
	sup := supervisor.New(launcher, probe, resolver)

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := sup.Run(ctx); err != nil {
			slog.Error("process supervisor exited", "error", err)
		}
	})

	wg.Wait()
}
