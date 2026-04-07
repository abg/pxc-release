package galera

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

const readyQuery = `
SELECT
  COALESCE((SELECT VARIABLE_VALUE <> 'none'
            FROM performance_schema.global_variables
            WHERE VARIABLE_NAME = 'wsrep_provider'), 0)    AS wsrep_enabled,
  COALESCE((SELECT VARIABLE_VALUE = 4
            FROM performance_schema.global_status
            WHERE VARIABLE_NAME = 'wsrep_local_state'), 0) AS wsrep_synced`

type ReadyWaiter struct {
	db       *sql.DB
	interval time.Duration
}

func NewReadyWaiter(db *sql.DB, interval time.Duration) *ReadyWaiter {
	if interval == 0 {
		interval = 5 * time.Second
	}
	return &ReadyWaiter{db: db, interval: interval}
}

// WaitReady blocks until mysqld is ready to serve traffic, or ctx is cancelled.
// Ready means either:
//   - wsrep is disabled (standalone MySQL, no Galera), or
//   - wsrep_local_state = 4 (Synced)
func (w *ReadyWaiter) WaitReady(ctx context.Context) error {
	for {
		ready, err := w.isReady(ctx)
		if err != nil {
			slog.Debug("readiness check failed", "err", err)
		} else if ready {
			slog.Info("mysqld is ready")
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(w.interval):
		}
	}
}

func (w *ReadyWaiter) isReady(ctx context.Context) (bool, error) {
	var wsrepEnabled, wsrepSynced bool
	if err := w.db.QueryRowContext(ctx, readyQuery).Scan(&wsrepEnabled, &wsrepSynced); err != nil {
		// Connection refused, mysqld not yet accepting connections — not ready.
		return false, err
	}

	slog.Debug("readiness check",
		slog.Bool("wsrep_enabled", wsrepEnabled),
		slog.Bool("wsrep_synced", wsrepSynced),
	)

	return !wsrepEnabled || wsrepSynced, nil
}
