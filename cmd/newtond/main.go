package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/cstream/newton/config"
	"github.com/cstream/newton/newton"
)

var version = "0.0.1"
var usage = `
newtond -- distributed message relaying and static file server

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
		fmt.Println("newton version", newton.ReleaseVersion)
		os.Exit(0)
	} else if config.ShowHelp {
		fmt.Println(Usage() + "\n")
		os.Exit(0)
	}
	var newton = newton.New(config)
	newton.RunServer()
}
