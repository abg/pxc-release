package galera

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

type Resolver struct {
	stateFile       StateFileReadWriter
	grastateFile    string
	clusterProber   ClusterProber
	isBootstrapNode bool
}

type StateFileReadWriter interface {
	Read() (state string, err error)
	Write(state string) error
}

type ClusterProber interface {
	ClusterHealthy(ctx context.Context) (bool, error)
	PeerCount() int
}

func NewResolver(stateFileRW StateFileReadWriter, clusterProber ClusterProber, grastateFile string, isBootstrapNode bool) *Resolver {
	return &Resolver{
		stateFile:       stateFileRW,
		clusterProber:   clusterProber,
		grastateFile:    grastateFile,
		isBootstrapNode: isBootstrapNode,
	}
}

func (r *Resolver) Resolve(ctx context.Context) ([]string, error) {
	state, err := r.stateFile.Read()
	noStateFile := errors.Is(err, ErrNoStateFile)

	if err != nil && !noStateFile {
		return nil, err
	}

	slog.Info("resolving galera startup mode",
		slog.String("state", state),
		slog.Bool("state_file_present", !noStateFile),
		slog.Int("peer_count", r.clusterProber.PeerCount()),
		slog.Bool("is_bootstrap_node", r.isBootstrapNode),
	)

	// peerCount | stateFile        | bootstrapNode | clusterHealthy | result
	// 1         | any              | any           | any            | bootstrap
	// >1        | absent           | false         | any            | join
	// >1        | absent           | true          | yes            | join
	// >1        | absent           | true          | no             | bootstrap
	// >1        | NEEDS_BOOTSTRAP  | any           | yes            | join
	// >1        | NEEDS_BOOTSTRAP  | any           | no             | bootstrap
	// >1        | SINGLE_NODE      | any           | yes            | join  (upgrade path)
	// >1        | SINGLE_NODE      | any           | no             | bootstrap (upgrade path)
	// >1        | CLUSTERED        | any           | any            | join
	// >1        | unknown          | any           | any            | join
	//
	// bootstrap: forms new cluster by passing mysqld --wsrep-new-cluster option
	// join: is just starting in normal mode; galera will sync with cluster nodes defined in my.cnf
	// TODO: peerCount should probably be clusterSize, including this node
	switch {
	case r.clusterProber.PeerCount() == 1:
		// when only one node participating in the cluster, de facto bootstrap
		slog.Info("single-node deploy, bootstrapping")
		return r.bootstrap()
	case noStateFile && r.isBootstrapNode:
		// legacy assumption: no state file == first deploy
		slog.Info("first deploy, bootstrap node, evaluating bootstrap")
		return r.bootstrapOrJoinByHealth(ctx)
	case state == StateSingleNode && r.clusterProber.PeerCount() > 1:
		// previously deployed and set state SINGLE_NODE
		// handle upgrade to a full cluster deployment
		slog.Info("single-node to multi-node upgrade, checking cluster health")
		return r.bootstrapOrJoinByHealth(ctx)
	case state == StateNeedsBootstrap:
		slog.Info("NEEDS_BOOTSTRAP, checking cluster health")
		return r.bootstrapOrJoinByHealth(ctx)
	default:
		// No state file, non-bootstrap node, CLUSTERED, unknown — join normally.
		slog.Info("joining cluster", "state", state)
		return r.join()
	}
}

func (r *Resolver) bootstrap() (args []string, err error) {
	if err := r.removeGrastate(); err != nil {
		return nil, err
	}

	return []string{"--wsrep-new-cluster"}, nil
}

func (r *Resolver) removeGrastate() error {
	if err := os.Remove(r.grastateFile); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing grastate.dat: %w", err)
	}
	slog.Info("removed grastate.dat before bootstrap")
	return nil
}

func (r *Resolver) bootstrapOrJoinByHealth(ctx context.Context) (args []string, err error) {
	healthy, err := r.clusterProber.ClusterHealthy(ctx)
	if err != nil {
		return nil, fmt.Errorf("checking cluster health: %w", err)
	}
	if healthy {
		slog.Info("cluster is healthy, joining instead of bootstrapping")
		return r.join()
	}
	slog.Info("cluster is not healthy, bootstrapping")
	return r.bootstrap()
}

func (r *Resolver) join() (args []string, err error) {
	// no special logic
	return nil, nil
}

func (r *Resolver) OnReady() error {
	state := StateClustered
	if r.clusterProber.PeerCount() == 1 {
		state = StateSingleNode
	}

	return r.stateFile.Write(state)
}
