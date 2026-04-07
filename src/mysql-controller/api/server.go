package api

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"mysql-controller/config"
	"mysql-controller/galera"
)

type StateSnapshotter interface {
	State() (galera.DBState, error)
}

type ServiceController interface {
	Bootstrap(ctx context.Context) (string, error)
	Join(ctx context.Context) (string, error)
	SingleNode(ctx context.Context) (string, error)
	Stop(ctx context.Context) (string, error)
	GetStatus(ctx context.Context) (string, error)
	CheckSequenceNumber() (string, error)
}

type Server struct {
	httpServer *http.Server
}

func NewServer(
	apiConfig config.API,
	hmacKey []byte,
	stateSnapshotter StateSnapshotter,
	serviceController ServiceController,
) *Server {
	mux := http.NewServeMux()
	addRoutes(mux, apiConfig, hmacKey, stateSnapshotter, serviceController)
	return &Server{
		httpServer: &http.Server{
			ReadHeaderTimeout: 10 * time.Second,
			Handler:           mux,
		},
	}
}

func (s *Server) Serve(l net.Listener) error {
	return s.httpServer.Serve(l)
}

func (s *Server) Shutdown() error {
	shutdownTimeout := 30 * time.Second
	slog.Info("shutting down API server", slog.Duration("timeout", shutdownTimeout))
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
