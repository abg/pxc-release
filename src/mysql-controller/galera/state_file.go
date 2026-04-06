package galera

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/google/renameio/v2"
)

// ErrNoStateFile is returned by Read when the state file does not exist.
// Absence is distinct from an empty or unrecognized file.
var ErrNoStateFile = errors.New("state file does not exist")

const (
	// StateUnknown is returned for files that exist but contain unrecognized content.
	// Treated as a normal join by the resolver.
	StateUnknown        = "UNKNOWN"
	StateNeedsBootstrap = "NEEDS_BOOTSTRAP"
	StateSingleNode     = "SINGLE_NODE"
	StateClustered      = "CLUSTERED"
)

// StateFile reads and writes the mysqld bootstrap state file.
// The file contains a single plain-text token written by the API
// prior to a bootstrap start request.
//
// File absence means normal join — it is not an error condition
// for the resolver, only for callers that explicitly need to know
// whether the file exists.
type StateFile struct {
	path string
}

func NewStateFile(path string) *StateFile {
	return &StateFile{path: path}
}

// Read returns the content of the state file.
// Returns ErrNoStateFile if the file does not exist.
// Returns StateUnknown for files with unrecognized content rather
// than an error — unrecognized content is treated as a normal join
// by the resolver, and logging the unexpected value is the caller's
// responsibility.
func (s *StateFile) Read() (string, error) {
	raw, err := os.ReadFile(s.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return StateUnknown, ErrNoStateFile
	case err != nil:
		return StateUnknown, fmt.Errorf("reading state file: %q: %w", s.path, err)
	}

	switch state := strings.TrimSpace(string(raw)); state {
	case StateClustered, StateNeedsBootstrap, StateSingleNode:
		return state, nil
	default:
		return StateUnknown, nil
	}
}

// Write atomically replaces the state file content.
// Uses a write-to-temp-then-rename pattern to avoid partial writes
// being observed by a concurrent Read.
func (s *StateFile) Write(state string) error {
	var token string
	switch state {
	case StateClustered, StateNeedsBootstrap, StateSingleNode:
		token = state
	default:
		return fmt.Errorf("unknown state file content: %q", state)
	}

	if err := renameio.WriteFile(s.path, []byte(token), 0o640); err != nil {
		return fmt.Errorf("writing state file %q: %w", s.path, err)
	}

	return nil
}
