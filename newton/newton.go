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

package newton

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"sync"
	"syscall"
	"time"

	"code.cloudfoundry.org/bytefmt"

	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"

	"gopkg.in/tylerb/graceful.v1"

	"github.com/gorilla/handlers"
	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"github.com/purak/newton/store"
)

var (
	httpServerGracePeriod time.Duration = 1500 * time.Millisecond
	newtonGracePeriod     time.Duration = 1500 * time.Millisecond
	newtonDB                            = "newton.db"
)

// Newton represents a new instance.
type Newton struct {
	nodeID                 string
	router                 *httprouter.Router
	dataTransferRate       float64
	dataTransferBurstLimit int64
	writeTimeout           time.Duration
	readTimeout            time.Duration
	config                 *config.Newton
	messageIDStore         *messageIDStore
	clientStore            ClientStore
	partitions             *partition.Partition
	waitGroup              sync.WaitGroup
	errGroup               errgroup.Group
	done                   chan struct{}
}

// New creates a new Newton instance
func New(cfg *config.Config) (*Newton, error) {
	writeTimeout, err := time.ParseDuration(cfg.WriteTimeout)
	if err != nil {
		return nil, err
	}
	readTimeout, err := time.ParseDuration(cfg.ReadTimeout)
	if err != nil {
		return nil, err
	}

	rate, err := bytefmt.ToBytes(cfg.DataTransferRate)
	if err != nil {
		return nil, err
	}

	burstLimit, err := bytefmt.ToBytes(cfg.DataTransferBurstLimit)
	if err != nil {
		return nil, err
	}

	// A basic LevelDB database to store basic info about grid
	spth := path.Join(cfg.Newton.DataDir, newtonDB)
	str, err := store.OpenLevelDB(spth)
	if err != nil {
		return nil, fmt.Errorf("%s: %s", spth, err)
	}
	router := httprouter.New()
	partman, err := partition.New(&cfg.DHT)
	if err != nil {
		return nil, err
	}
	messageIDStore, err := newMessageIDStore(str)
	if err != nil {
		return nil, err
	}
	n := &Newton{
		router:                 router,
		partitions:             partman,
		dataTransferRate:       float64(rate),
		dataTransferBurstLimit: int64(burstLimit),
		writeTimeout:           writeTimeout,
		readTimeout:            readTimeout,
		config:                 &cfg.Newton,
		clientStore:            NewClientStore(str),
		messageIDStore:         messageIDStore,
		done:                   make(chan struct{}),
	}
	return n, nil
}

func (n *Newton) createGracefulServer() *graceful.Server {
	router := httprouter.New()
	router.GET("/events/:name", n.eventsHandler)
	router.GET("/lookup/:name", n.lookupHandler)

	router.GET("/read/:name/:clientID/*routingKey", n.readHandler)
	router.HEAD("/read/:name/:clientID/*routingKey", n.readHandler)
	router.POST("/write-with-id/:messageID", n.messageIDHandler)

	router.POST("/write/:name/:clientID/*routingKey", n.writeHandler)
	router.GET("/read-with-id/:messageID", n.messageIDHandler)

	router.POST("/close/:messageID/:returnCode", n.closeHandler)

	ah := &AuthHandler{
		Handler:     handlers.RecoveryHandler()(router),
		callbackURL: n.config.AuthCallbackUrl,
	}

	if len(n.config.AllowedOrigins) == 0 {
		n.config.AllowedOrigins = append(n.config.AllowedOrigins, "*")

	}
	crh := handlers.CORS(
		handlers.AllowedHeaders(n.config.AllowedHeaders),
		handlers.AllowedMethods(n.config.AllowedMethods),
		handlers.AllowedOrigins(n.config.AllowedOrigins),
		handlers.ExposedHeaders(n.config.ExposedHeaders),
		handlers.MaxAge(n.config.MaxAge),
		handlers.AllowCredentials(),
	)(ah)

	s := &http.Server{
		Addr:    n.config.Listen,
		Handler: crh,
	}
	http2.ConfigureServer(s, nil)

	return &graceful.Server{
		NoSignalHandling: true,
		Server:           s,
	}

}

// Start starts a new Newton server instance and blocks until the server is closed.
func (n *Newton) Start() error {
	srv := n.createGracefulServer()
	// Wait for SIGTERM or SIGINT
	go n.waitForInterrupt(srv)
	log.Infof("Starting Newton instance with PID: %d, runtime: %s", os.Getpid(), runtime.Version())

	n.errGroup.Go(func() error {
		// Fire up DHT instance to map and find clients in Newton network
		return n.partitions.Run()
	})

	// Wait for DHT
	select {
	case <-n.partitions.StartChan:
	case <-n.partitions.StopChan:
		return n.errGroup.Wait()
	}

	n.errGroup.Go(func() error {
		log.Info("Listening HTTP/2 connections on ", n.config.Listen)
		err := srv.ListenAndServeTLS(n.config.CertFile, n.config.KeyFile)
		if err != nil {
			if opErr, ok := err.(*net.OpError); !ok || (ok && opErr.Op != "accept") {
				log.Debugf("Something wrong with HTTP/2 server: %s", err)
				// We call Close here because if HTTP/2 server fails, we cannot run this process anymore.
				// This block can be invoked during normal closing process but Close method will handle
				// redundant Close call.
				n.Close()
			}
			log.Errorf("Error while running HTTP/2 server on %s: %s", n.config.Listen, err)
			return err
		}
		return nil
	})

	// TODO: We have to wait for a potential HTTP server failure
	<-n.done

	// Wait for workers.
	n.waitGroup.Wait()
	log.Info("HTTP server has been stopped.")

	// Stop DHT instance
	n.partitions.Close()
	select {
	case <-n.partitions.StopChan:
	case <-time.After(newtonGracePeriod):
		log.Warn("Some goroutines did not finish in grace period. They will be killed.")
	}
	return n.errGroup.Wait()
}

func (n *Newton) waitForInterrupt(srv *graceful.Server) {
	shutDownChan := make(chan os.Signal)
	signal.Notify(shutDownChan, syscall.SIGTERM, syscall.SIGINT)
	s := <-shutDownChan
	log.Infof("Catched signal: %s", s.String())

	stopChan := srv.StopChan()
	srv.Stop(httpServerGracePeriod) // http server
	<-stopChan

	// Close workers.
	n.Close()
}

// Close signals Newton instance to close.
func (n *Newton) Close() {
	select {
	case <-n.done:
		log.Debug("Newton instance has already been closed.")
		return
	default:
	}
	close(n.done)
}

// Converts json to byte slice
func MsgToByte(msg interface{}) ([]byte, error) {
	r, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// jsonErrorResponse prepares a json response with the given description and  status code variables then returns the response.
func (n *Newton) jsonErrorResponse(w http.ResponseWriter, desc string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	msg := &ErrorMsg{
		Description: desc,
	}
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		log.Errorf("Error while returning error message to the client: %s", err)
	}
}
