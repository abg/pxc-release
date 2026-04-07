package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/go-sql-driver/mysql"

	"mysql-controller/api"
	"mysql-controller/config"
	"mysql-controller/controller"
	"mysql-controller/galera"
	"mysql-controller/mysqld"
	"mysql-controller/supervisor"
	"mysql-controller/tlsutil"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "path to configuration file")
	flag.Parse()

	if configPath == "" {
		slog.Error("config path must be provided")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	var cfg config.Config

	if err := config.Load(configPath, &cfg); err != nil {
		slog.Error("loading config", "error", err)
		os.Exit(1)
	}

	var (
		grastateFile    = cfg.GraStatePath
		isBootstrapNode = cfg.IsBootstrapNode
		mysqldPath      = cfg.MySQL.DaemonPath
		mysqldBaseArgs  = cfg.MySQL.DaemonArgs
		pollInterval    = cfg.ReadinessPollInterval
	)

	db, err := sql.Open("mysql", cfg.MySQL.DSN)
	if err != nil {
		slog.Error("db init", "error", err)
		os.Exit(1)
	}

	stateFile := galera.NewStateFile(cfg.StatePath)
	prober := galera.NewChecker(cfg.ClusterMembers(), newHTTPClient(cfg))
	galeraBootstrapOptionResolver := galera.NewResolver(stateFile, prober, grastateFile, isBootstrapNode)
	mysqldCmdBuilder := mysqld.NewCommandBuilder(mysqldPath, mysqldBaseArgs, galeraBootstrapOptionResolver, os.Stderr)
	launcher := supervisor.NewLauncher(mysqldCmdBuilder)
	probe := galera.NewReadyWaiter(db, pollInterval)
	sup := supervisor.New(launcher, probe, galeraBootstrapOptionResolver)

	hmacKey := make([]byte, 32)
	if _, err := rand.Read(hmacKey); err != nil {
		slog.Error("generating HMAC key", "error", err)
		os.Exit(1)
	}
	wsrepRecoveryOptionResolver := mysqld.NewOptionsResolver("--wsrep-recover", "--disable-log-error")
	wsrepRecoveryCmdBuilder := mysqld.NewCommandBuilder(mysqldPath, mysqldBaseArgs, wsrepRecoveryOptionResolver, os.Stderr)
	nodeController := controller.NewNodeController(sup, stateFile, wsrepRecoveryCmdBuilder)
	stateSnapshotter := galera.NewStateSnapshotter(db)
	apiServer := api.NewServer(cfg.API, hmacKey, stateSnapshotter, nodeController)
	tlsConfig, err := tlsutil.ServerTLSConfig(cfg.API.TLS.Certificate, cfg.API.TLS.Key)
	if err != nil {
		slog.Error("loading TLS config", "error", err)
		os.Exit(1)
	}
	tlsListener, err := tls.Listen("tcp", cfg.API.BindAddress, tlsConfig)
	if err != nil {
		slog.Error("binding api network", "error", err)
		os.Exit(1)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		sup.Run(ctx)
	})

	wg.Go(func() {
		if err := apiServer.Serve(tlsListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("api server", "error", err)
		}
	})

	wg.Go(func() {
		<-ctx.Done()
		slog.Info("shutting down API server")
		if err := apiServer.Shutdown(); err != nil {
			slog.Error("api server shutdown", "error", err)
		}
	})

	wg.Wait()
	slog.Info("mysql-controller exiting")
}

func newHTTPClient(cfg config.Config) *http.Client {
	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM([]byte(cfg.API.TLS.CA))

	tlsConfig := &tls.Config{
		RootCAs:    rootCAs,
		ServerName: cfg.API.TLS.ServerName,
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
		Timeout: cfg.ClusterProbeTimeout,
	}
}
