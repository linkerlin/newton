package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/purak/newton/config"
	"github.com/purak/newton/newton"
)

var version = "0.0.1"
var usage = `
newtonctl -- configure newtond and obtain information at runtime
`

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
}
