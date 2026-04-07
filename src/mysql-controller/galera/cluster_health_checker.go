package galera

import (
	"context"
	"log/slog"
	"net/http"
)

type Checker struct {
	members    []Member
	httpClient *http.Client
}

type Member struct {
	ID        string
	Endpoints []string
}

var _ slog.LogValuer = (*Member)(nil)

func (m Member) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("id", m.ID),
		slog.Any("endpoints", m.Endpoints),
	)
}

func NewChecker(members []Member, client *http.Client) *Checker {
	return &Checker{
		members:    members,
		httpClient: client,
	}
}

func (p *Checker) ClusterHealthy(ctx context.Context) (bool, error) {
	slog.Info("checking for any healthy cluster node",
		slog.Any("members", p.members),
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	totalEndpoints := p.totalEndpoints()

	results := make(chan bool, totalEndpoints)

	for _, peer := range p.members {
		for _, url := range peer.Endpoints {
			go func(u string) {
				slog.Info("checking if node is healthy", slog.String("url", u))
				results <- p.probe(ctx, u)
			}(url)
		}
	}

	for range totalEndpoints {
		if <-results {
			return true, nil
		}
	}

	slog.Info("no nodes in cluster are healthy")
	return false, nil
}

func (p *Checker) totalEndpoints() int {
	count := 0
	for _, member := range p.members {
		count += len(member.Endpoints)
	}
	return count
}

func (p *Checker) probe(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		slog.Error("creating request failed", slog.String("url", url), slog.Any("error", err))
		return false
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		slog.Error("checking cluster member health failed", slog.String("url", url), slog.Any("error", err))
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	return resp.StatusCode == http.StatusOK
}

func (p *Checker) MemberCount() int {
	return len(p.members)
}
