package config

import (
	"fmt"
	"os"
	"time"

	"mysql-controller/galera"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	StatePath       string `yaml:"state_path"`
	GraStatePath    string `yaml:"grastate_path"`
	IsBootstrapNode bool   `yaml:"is_bootstrap_node"`

	ReadinessPollInterval time.Duration `yaml:"poll_interval"`
	ClusterProbeTimeout   time.Duration `yaml:"cluster_probe_timeout"`

	MySQL struct {
		DaemonPath string   `yaml:"mysqld_path"`
		DaemonArgs []string `yaml:"mysqld_args"`
		DSN        string   `yaml:"dsn"`
	} `yaml:"mysql"`

	API API `yaml:"api"`
}

type API struct {
	BindAddress           string `yaml:"bind_address"`
	AvailableWhenReadOnly bool   `yaml:"available_when_read_only"`

	BasicAuth struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"basic_auth"`

	TLS struct {
		CA          string `yaml:"ca"`
		Certificate string `yaml:"certificate"`
		Key         string `yaml:"private_key"`
		ServerName  string `yaml:"server_name"`
	} `yaml:"tls"`

	Members []struct {
		ID        string   `yaml:"id"`
		Endpoints []string `yaml:"endpoints"`
	} `yaml:"members"`
}

func (c *Config) ClusterMembers() (members []galera.Member) {
	for _, m := range c.API.Members {
		members = append(members, galera.Member{
			ID:        m.ID,
			Endpoints: m.Endpoints,
		})
	}
	return members
}

func Load(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("loading config path %q: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return err
	}

	return nil
}
