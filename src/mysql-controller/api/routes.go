package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"

	"mysql-controller/config"
)

func addRoutes(
	mux *http.ServeMux,
	apiConfig config.API,
	hmacKey []byte,
	stateSnapshotter StateSnapshotter,
	serviceController ServiceController,
) {
	secure := func(h http.Handler) http.Handler {
		return basicAuthMiddleware(apiConfig.BasicAuth.Username, apiConfig.BasicAuth.Password, hmacKey, h)
	}

	// unprotected
	mux.Handle("GET /", plainHandler(func(_ *http.Request) (string, error) {
		return galeraStatus(stateSnapshotter, apiConfig.AvailableWhenReadOnly)
	}))

	mux.Handle("GET /galera_status", plainHandler(func(_ *http.Request) (string, error) {
		return galeraStatus(stateSnapshotter, apiConfig.AvailableWhenReadOnly)
	}))

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.Handle("GET /api/v1/status", v1StatusHandler(stateSnapshotter, apiConfig.AvailableWhenReadOnly))

	// protected
	mux.Handle("GET /mysql_status", secure(plainHandler(func(r *http.Request) (string, error) {
		return serviceController.GetStatus(r.Context())
	})))

	mux.Handle("POST /stop_mysql", secure(plainHandler(func(r *http.Request) (string, error) {
		return serviceController.Stop(r.Context())
	})))

	mux.Handle("POST /start_mysql_bootstrap", secure(plainHandler(func(r *http.Request) (string, error) {
		return serviceController.Bootstrap(r.Context())
	})))

	mux.Handle("POST /start_mysql_join", secure(plainHandler(func(r *http.Request) (string, error) {
		return serviceController.Join(r.Context())
	})))

	mux.Handle("POST /start_mysql_single_node", secure(plainHandler(func(r *http.Request) (string, error) {
		return serviceController.SingleNode(r.Context())
	})))

	mux.Handle("GET /sequence_number", secure(plainHandler(func(_ *http.Request) (string, error) {
		return serviceController.CheckSequenceNumber()
	})))
}

func galeraStatus(snapshotter StateSnapshotter, availableWhenReadOnly bool) (string, error) {
	s, err := snapshotter.State()
	if err != nil {
		return "", err
	}
	if !s.IsHealthy(availableWhenReadOnly) {
		return "", errors.New("not synced")
	}
	return "synced", nil
}

func v1StatusHandler(snapshotter StateSnapshotter, availableWhenReadOnly bool) http.Handler {
	var priorHealth atomic.Bool
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		s, err := snapshotter.State()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		currentHealth := s.IsHealthy(availableWhenReadOnly)
		resp := V1StatusResponse{
			WsrepLocalState:        uint(s.WsrepLocalState),
			WsrepLocalStateComment: s.WsrepLocalState.Comment(),
			WsrepLocalIndex:        s.WsrepLocalIndex,
			Healthy:                currentHealth,
		}

		oldHealth := priorHealth.Swap(currentHealth)
		if oldHealth != currentHealth {
			slog.Info("health transition", "healthy", currentHealth, "wsrep_local_state", resp.WsrepLocalStateComment)
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			slog.Warn("encoding response", "error", err)
		}
	})
}

type V1StatusResponse struct {
	WsrepLocalState        uint   `json:"wsrep_local_state"`
	WsrepLocalStateComment string `json:"wsrep_local_state_comment"`
	WsrepLocalIndex        uint   `json:"wsrep_local_index"`
	Healthy                bool   `json:"healthy"`
}

// plainHandler wraps a func(req) (string, error) into an http.Handler that
// writes the returned string as the response body, or a 500 on error.
func plainHandler(fn func(*http.Request) (string, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := fn(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, body)
	})
}
