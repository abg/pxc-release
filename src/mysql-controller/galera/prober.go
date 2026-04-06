package galera

import "context"

type PeerProber struct{}

func NewClusterProber() *PeerProber {
	return &PeerProber{}
}

func (p *PeerProber) ClusterHealthy(ctx context.Context) (bool, error) {
	return false, nil
}

func (p *PeerProber) PeerCount() int {
	return -1
}
