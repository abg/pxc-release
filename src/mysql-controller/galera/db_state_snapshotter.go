package galera

import "database/sql"

// DBStateSnapshotter queries the local mysqld instance for a full state snapshot
// in a single round-trip.
type DBStateSnapshotter struct {
	db *sql.DB
}

func NewStateSnapshotter(db *sql.DB) *DBStateSnapshotter {
	return &DBStateSnapshotter{db: db}
}

func (s *DBStateSnapshotter) State() (state DBState, err error) {
	err = s.db.QueryRow(`
		SELECT
		  (SELECT VARIABLE_VALUE
		   FROM performance_schema.global_status
		   WHERE VARIABLE_NAME = 'wsrep_local_index') AS wsrep_local_index,
		  (SELECT VARIABLE_VALUE
		   FROM performance_schema.global_status
		   WHERE VARIABLE_NAME = 'wsrep_local_state') AS wsrep_local_state,
		  @@global.read_only                           AS read_only,
		  @@global.pxc_maint_mode != 'DISABLED'        AS maintenance_enabled
	`).Scan(
		&state.WsrepLocalIndex,
		&state.WsrepLocalState,
		&state.ReadOnly,
		&state.MaintenanceEnabled,
	)
	return state, err
}
