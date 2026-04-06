package galera

import (
	"context"
	"database/sql"
	"time"
)

type ReadyWaiter struct {
	db           *sql.DB
	pollInterval time.Duration
}

func NewReadyWaiter(db *sql.DB, pollInterval time.Duration) *ReadyWaiter {
	return &ReadyWaiter{db: db, pollInterval: pollInterval}
}

func (w *ReadyWaiter) WaitReady(ctx context.Context) error {
	return nil
}
