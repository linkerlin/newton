package config

import (
	"flag"
	"github.com/BurntSushi/toml"
	"io/ioutil"
	"os"
)

// FIXME: 32 bit compabilty is a problem for configuration items

const DefaultSystemConfigPath = "data/newton.conf"

type Config struct {
	SystemPath  string
	ShowHelp    bool
	ShowVersion bool
	Server      ServerInfo
}

type ServerInfo struct {
	Addr                   string `toml:"addr"`
	ClientAnnounceInterval int64  `toml:"clientAnnounceInterval"`
}

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

// Loads from the system newton configuration file if it exists.
func (c *Config) LoadSystemFile() error {
	if _, err := os.Stat(c.SystemPath); os.IsNotExist(err) {
		return nil
	}
	return c.LoadFile(c.SystemPath)
}

// Loads configuration from command line flags.
func (c *Config) LoadFlags(arguments []string) error {
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)

	/* Generic configuration  parameters */
	f.BoolVar(&c.ShowHelp, "h", false, "")
	f.BoolVar(&c.ShowHelp, "help", false, "")
	f.BoolVar(&c.ShowVersion, "version", false, "")

	/* Server parameters */
	f.StringVar(&c.Server.Addr, "addr", c.Server.Addr, "")
	f.Int64Var(&c.Server.ClientAnnounceInterval, "client-announce-interval", c.Server.ClientAnnounceInterval, "")

	if err := f.Parse(arguments); err != nil {
		return err
	}

	return nil
}

// Loads configuration from a file.
func (c *Config) LoadFile(path string) error {
	_, err := toml.DecodeFile(path, &c)
	return err
}

// Creates a new configuration
func New() *Config {
	c := new(Config)
	c.SystemPath = DefaultSystemConfigPath
	return c
}
