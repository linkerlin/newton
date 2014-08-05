package config

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
)

// FIXME: 32 bit compabilty is a problem for configuration items
const envConfigFile = "NEWTON_CONFIG_PATH"
const localConfigFile = "data/newton.conf"
const systemConfigFile = "/etc/newton/newton.conf"

// Config is the configuration container for newton instances
type Config struct {
	ConfigFile  string
	ShowHelp    bool
	ShowVersion bool
	Server      ServerInfo
	Database    DatabaseInfo
}

// ServerInfo contains configuration items which are related to newton daemon.
type ServerInfo struct {
	Port                   string `toml:"port"`
	ClientAnnounceInterval int64  `toml:"clientAnnounceInterval"`
	Identity               string `toml:"identity"`
	Password               string `toml:"password"`
	WanIP                  string `toml:"wanIP"`
}

// DatabaseInfo contains configuration items which are related to Gauss database daemon.
type DatabaseInfo struct {
	Addr          string `toml:"addr"`
	MaxUserClient int    `toml:"maxUserClient"`
}

// Load starts configuration loading process
func (c *Config) Load(arguments []string) error {
	var path string
	f := flag.NewFlagSet("newton", -1)
	f.SetOutput(ioutil.Discard)
	f.StringVar(&path, "config", "", "path to config file")
	f.Parse(arguments)

	// Load from config file specified in arguments.
	if path == "" {
		path = c.ConfigFile
	}

	if err := c.LoadFile(path); err != nil {
		return err
	}

	// Load from command line flags.
	if err := c.LoadFlags(arguments); err != nil {
		return err
	}

	return nil
}

// LoadFlags is a function that loads configuration from command line flags.
func (c *Config) LoadFlags(arguments []string) error {
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	// Generic configuration  parameters
	f.BoolVar(&c.ShowHelp, "h", false, "")
	f.BoolVar(&c.ShowHelp, "help", false, "")
	f.BoolVar(&c.ShowVersion, "version", false, "")

	// Server parameters
	f.StringVar(&c.Server.Port, "port", c.Server.Port, "")
	f.Int64Var(&c.Server.ClientAnnounceInterval, "client-announce-interval", c.Server.ClientAnnounceInterval, "")
	f.StringVar(&c.Server.Identity, "identity", c.Server.Identity, "")
	f.StringVar(&c.Server.Password, "password", c.Server.Password, "")
	f.StringVar(&c.Server.WanIP, "wan-ip", c.Server.WanIP, "")

	// Database parameters
	f.StringVar(&c.Database.Addr, "database-addr", c.Database.Addr, "")
	f.IntVar(&c.Database.MaxUserClient, "max-user-client", c.Database.MaxUserClient, "")

	if err := f.Parse(arguments); err != nil {
		return err
	}

	return nil
}

// LoadFile is a function that loads configuration from a file.
func (c *Config) LoadFile(path string) error {
	_, err := toml.DecodeFile(path, &c)
	return err
}

// New creates a new configuration object
func New() *Config {
	c := new(Config)
	c.ConfigFile = configFile()
	return c
}

func configFile() string {
	e := os.Getenv(envConfigFile)
	if _, err := os.Stat(e); err == nil {
		return e
	} else if _, err := os.Stat(localConfigFile); err == nil {
		return localConfigFile
	} else if _, err := os.Stat(systemConfigFile); err == nil {
		return systemConfigFile
	}
	return ""
}
