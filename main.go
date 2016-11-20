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

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"runtime"
	"time"

	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
	"github.com/purak/newton/newton"
)

var usage = `newton -- HTTP-based RPC combined with service discovery.

Usage: newton [options] ...

Options:
  -h -help                      
      Shows this screen.

  -v -version                   
      Shows version information.

  -c -config                    
      Sets configuration file path. Default is /etc/newton.conf. 
      Set NEWTON_CONFIG to overwrite it.

  -generateIdentifier           
      Generates an 16 bit hexadecimal string as identifier.

  -log.format 
      If set use a syslog logger or JSON logging. Example: 
      logger:syslog?appname=bob&local=7 or logger:stdout?json=true. 
      Defaults to stderr.

  -log.level "info"
      Only log messages with the given severity or above. Valid levels: 
      [debug, info, warn, error, fatal].


The Go runtime version %s
Report bugs to https://github.com/purak/newton/issues`

// RelaseVersion is the program version.
const releaseVersion = "0.1"

var (
	path               string
	showHelp           bool
	showVersion        bool
	generateIdentifier bool
)

func init() {
	mrand.Seed(time.Now().UnixNano())
}

func main() {
	// Parse command line parameters
	f := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	f.SetOutput(ioutil.Discard)
	f.BoolVar(&showHelp, "h", false, "")
	f.BoolVar(&showHelp, "help", false, "")
	f.BoolVar(&showVersion, "version", false, "")
	f.BoolVar(&showVersion, "v", false, "")
	f.BoolVar(&generateIdentifier, "generateIdentifier", false, "")
	f.StringVar(&path, "config", config.ConfigFile, "")
	f.StringVar(&path, "c", config.ConfigFile, "")

	// Add logger flags
	log.AddFlags(f)

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Fatalf("Error parsing flags: %s", err)
	}

	if showVersion {
		fmt.Println("newton version ", releaseVersion)
		return
	} else if showHelp {
		msg := fmt.Sprintf(usage, runtime.Version())
		fmt.Println(msg)
		return
	} else if generateIdentifier {
		// Network identifier is a 16 byte length byte slice for the PartitionManager package that's used
		// to unify a specific network on the Internet.
		b := make([]byte, 16)
		if _, err := rand.Read(b); err != nil {
			log.Fatal("Error while generating network identifier: ", err)
		}
		log.Info("Random network identifier: ", hex.EncodeToString(b))
		return
	}
	c, err := config.New(path)
	if err != nil {
		log.Fatalf("Error while loading configuration: %v", err)
	}

	err = checkConfig(c)
	if err != nil {
		log.Fatalf("Error in configuration file: %v", err)
	} else {
		n, err := newton.New(c)
		if err != nil {
			log.Fatalf("Error while initializing Newton: %v", err)
		}
		if err := n.Start(); err != nil {
			log.Fatalf("Error while running Newton instance: %v", err)
		}
		log.Info("Good bye!")
	}
}

func checkConfig(c *config.Config) error {
	// TODO: Check variable health
	if c.Newton.Listen == "" {
		return errors.New("Newton.Listen cannot be empty.")
	} else if c.DataDir == "" {
		return errors.New("dataDir cannot be empty.")
	} else if c.Partition.Multicast.Enabled && c.Partition.Multicast.Address == "" {
		return errors.New("PartitionManager.Multicast.Address cannot be empty.")
	} else if c.Partition.Unicast.Listen == "" {
		return errors.New("PartitionManager.Unicast.Listen cannot be empty.")
	}
	// Check for certificate file
	if c.Newton.CertFile == "" {
		return errors.New("certFile cannot be empty.")
	} else if c.Newton.CertFile != "" {
		if _, err := os.Stat(c.Newton.CertFile); os.IsNotExist(err) {
			// path/to/whatever does not exist
			return fmt.Errorf("Certificate file could not be found: %s", c.Newton.CertFile)
		}
	}
	// Check for private key file
	if c.Newton.KeyFile == "" {
		return errors.New("keyFile cannot be empty.")
	} else if c.Newton.KeyFile != "" {
		if _, err := os.Stat(c.Newton.KeyFile); os.IsNotExist(err) {
			// path/to/whatever does not exist
			return fmt.Errorf("Private key file could not be found: %s", c.Newton.KeyFile)
		}
	}
	return nil
}
