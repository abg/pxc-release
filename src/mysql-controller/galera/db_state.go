package galera

import (
	"fmt"
	"math"
)

type WsrepLocalState uint

// https://docs.percona.com/percona-xtradb-cluster/8.0/wsrep-status-index.html#wsrep_local_state
const (
	WsrepInitialized    WsrepLocalState = iota
	WsrepJoining                        // https://splice.com/blog/iota-elegant-constants-golang/
	WsrepDonorDesynced
	WsrepJoined
	WsrepSynced
)

const InvalidIndex = math.MaxUint64

func (w WsrepLocalState) Comment() string {
	switch w {
	case WsrepInitialized:
		return "Initialized"
	case WsrepJoining:
		return "Joining"
	case WsrepDonorDesynced:
		return "Donor/Desynced"
	case WsrepJoined:
		return "Joined"
	case WsrepSynced:
		return "Synced"
	default:
		return fmt.Sprintf("Unrecognized state: %d", w)
	}
}

type DBState struct {
	WsrepLocalIndex    uint            `json:"wsrep_local_index"`
	WsrepLocalState    WsrepLocalState `json:"wsrep_local_state"`
	ReadOnly           bool            `json:"read_only"`
	MaintenanceEnabled bool            `json:"maintenance_enabled"`
}

func (s DBState) IsHealthy(availableWhenReadOnly bool) bool {
	switch {
	case s.WsrepLocalIndex == InvalidIndex:
		return false
	case s.ReadOnly && !availableWhenReadOnly:
		return false
	case s.MaintenanceEnabled:
		return false
	case s.WsrepLocalState == WsrepSynced, s.WsrepLocalState == WsrepDonorDesynced:
		return true
	default:
		return false
	}
}
