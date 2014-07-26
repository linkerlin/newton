package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/purak/newton/config"
	"github.com/purak/newton/newton"
	"github.com/purak/newton/store"
)

var version = "0.0.1"
var usage = `
newtonctl -- configure newtond and get information at runtime

HELP TEXT
`

func Usage() string {
	return strings.TrimSpace(usage)
}

func main() {
	var config = config.New()
	args := os.Args[1:]

	if len(args) == 0 {
		fmt.Println("no command given\n")
		fmt.Println(Usage() + "\n")
		os.Exit(1)
	}

	if err := config.Load(args); err != nil {
		fmt.Println(Usage() + "\n")
		fmt.Println(err.Error() + "\n")
		os.Exit(1)
	}

	if config.ShowVersion {
		fmt.Println("newton version", newton.ReleaseVersion)
		os.Exit(0)
	} else if config.ShowHelp {
		fmt.Println(Usage() + "\n")
		os.Exit(0)
	}

	if args[0] == "cluster" {
		if len(args) < 2 {
			fmt.Println("not enough command\n")
			fmt.Println(Usage() + "\n")
			os.Exit(1)
		}

		clusterStore := store.NewClusterStore(config)
		if args[1] == "list" {
			listMembers(clusterStore)
		} else if args[1] == "add" {
			addMember(clusterStore)
		} else if args[1] == "delete" {
			deleteMember(clusterStore)
		} else if args[1] == "update" {
			updateMember(clusterStore)
		} else {
			fmt.Println("Unknown command: ", args[1])
			os.Exit(1)
		}
	}

}

func listMembers(cs *store.ClusterStore) {
	fmt.Println("LIST")
}

func addMember(cs *store.ClusterStore) {
	fmt.Println("ADD")
}

func deleteMember(cs *store.ClusterStore) {
	fmt.Println("DELETE")
}

func updateMember(cs *store.ClusterStore) {
	fmt.Println("UPDATE")
}
