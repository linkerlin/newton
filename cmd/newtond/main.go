package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/cstream/newton/config"
	"github.com/cstream/newton/newton"
)

var usage = `
newtond -- distributed message proxy server

Usage:
  newtond -addr <addr>
  newtond -h | -help
  newtond -version

Options:
  -h -help          Show this screen.
  --version         Show version.

Client Communication Options:
  -addr=<host:port>         The public host:port used for client communication.
`

// Usage prints help text for newtond tool
func Usage() string {
	return strings.TrimSpace(usage)
}

func main() {
	var config = config.New()
	if err := config.Load(os.Args[1:]); err != nil {
		fmt.Println(Usage() + "\n")
		fmt.Println(err.Error() + "\n")
		os.Exit(1)
	} else if config.ShowVersion {
		fmt.Println("newtond version", newton.ReleaseVersion)
		os.Exit(0)
	} else if config.ShowHelp {
		fmt.Println(Usage() + "\n")
		os.Exit(0)
	}

	// Use all processor cores.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Start a new newton instance
	var newton = newton.New(config)
	newton.RunServer()
}
