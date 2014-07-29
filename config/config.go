package config

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/BurntSushi/toml"
)

// FIXME: 32 bit compabilty is a problem for configuration items

const defaultSystemConfigPath = "data/newton.conf"

// Config is the configuration container for newton instances
type Config struct {
	SystemPath  string
	ShowHelp    bool
	ShowVersion bool
	Server      ServerInfo
	Database    DatabaseInfo
}

// ServerInfo contains configuration items which is related to newton daemon
type ServerInfo struct {
	Addr                   string `toml:"addr"`
	ClientAnnounceInterval int64  `toml:"clientAnnounceInterval"`
	Identity               string `toml:"identity"`
	Password               string `toml:"password"`
}

// DatabaseInfo contains configuration items which is related to Gauss database daemon
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

	// Load from system file.
	if err := c.LoadSystemFile(); err != nil {
		return err
	}

	// Load from config file specified in arguments.
	if path != "" {
		if err := c.LoadFile(path); err != nil {
			return err
		}
	}

	// Load from command line flags.
	if err := c.LoadFlags(arguments); err != nil {
		return err
	}

	return nil
}

// LoadSystemFile is a function that loads from the system newton configuration file if it exists.
func (c *Config) LoadSystemFile() error {
	if _, err := os.Stat(c.SystemPath); os.IsNotExist(err) {
		return nil
	}
	return c.LoadFile(c.SystemPath)
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
	f.StringVar(&c.Server.Addr, "addr", c.Server.Addr, "")
	f.Int64Var(&c.Server.ClientAnnounceInterval, "client-announce-interval", c.Server.ClientAnnounceInterval, "")
	f.StringVar(&c.Server.Identity, "identity", c.Server.Identity, "")
	f.StringVar(&c.Server.Password, "password", c.Server.Password, "")

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
	c.SystemPath = defaultSystemConfigPath
	return c
}
