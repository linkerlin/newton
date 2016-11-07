// Copyright 2015 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"io/ioutil"
	"os"

	"gopkg.in/yaml.v2"
)

const (
	// ConfigFile is the default configuration file path on a Unix-based operating system.
	ConfigFile = "/etc/newton.yml"

	// EnvConfigFile is the name of environment variable which can be used to override default configuration file path.
	EnvConfigFile = "NEWTON_CONFIG"
)

// Newton contains configuration items which are related to Newton server.
type Newton struct {
	Common
	Address                string   `yaml:"address"`
	Listen                 string   `yaml:"listen"`
	ReadTimeout            string   `yaml:"read_timeout"`
	WriteTimeout           string   `yaml:"write_timeout"`
	AutoACK                bool     `yaml:"autoACK"`
	Hash                   bool     `yaml:"hash"`
	CertFile               string   `yaml:"certFile"`
	KeyFile                string   `yaml:"keyFile"`
	Origin                 string   `yaml:"origin"`
	AllowedHeaders         []string `yaml:"allowedHeaders"`
	AllowedMethods         []string `yaml:"allowedMethods"`
	AllowedOrigins         []string `yaml:"allowedOrigins"`
	ExposedHeaders         []string `yaml:"exposedHeaders"`
	MaxAge                 int      `yaml:"maxAge"`
	AuthCallbackUrl        string   `yaml:"authCallbackUrl"`
	WhitelistedHeaders     []string `yaml:"whitelistedHeaders"`
	DataTransferRate       string   `yaml:"dataTransferRate"`
	DataTransferBurstLimit string   `yaml:"dataTransferBurstLimit"`
}

// DHT contains configuration items which are related to DHT node.
type DHT struct {
	Common
	Multicast
	Unicast
	Listen              string `yaml:"listen"`
	Identifier          string `yaml:"identifier"`
	PeerCheckInterval   int64  `yaml:"peerCheckInterval"`
	InactivityThreshold int64  `yaml:"inactivityThreshold"`
	CertFile            string `yaml:"certFile"`
	KeyFile             string `yaml:"keyFile"`
}

type Multicast struct {
	Address   string `yaml:"address"`
	Enabled   bool   `yaml:"enabled"`
	Interface string `yaml:"interface"`
}

type Unicast struct {
	Peers             []string `yaml:"peers"`
	DiscoveryInterval string   `yaml:"discoveryInterval"`
}

// Common ships common configuration parameters.
type Common struct {
	Debug   bool   `yaml:"debug"`
	DataDir string `yaml:"dataDir"`
}

// Config is the configuration container for newton instances
type Config struct {
	Common
	Newton
	DHT
	Unicast
	Multicast
}

// New creates a new configuration object
func New(path string) (*Config, error) {
	if len(path) == 0 {
		path = os.Getenv(EnvConfigFile)
	}
	if len(path) == 0 {
		path = ConfigFile
	}
	// read whole the file
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	if err = yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	// This seems a bit hacky but it's absolutely worth.
	c.Newton.Common = c.Common
	c.DHT.Common = c.Common
	c.DHT.Unicast = c.Unicast
	c.DHT.Multicast = c.Multicast
	return &c, nil
}
