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
	"errors"
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
	"github.com/purak/newton/kv"
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"github.com/purak/newton/store"

	ksrv "github.com/purak/newton/proto/kv"
	psrv "github.com/purak/newton/proto/partition"
)

var (
	httpServerGracePeriod time.Duration = 1500 * time.Millisecond
	newtonGracePeriod     time.Duration = 1500 * time.Millisecond
	newtonDB                            = "newton.db"
)

// Newton represents a new instance.
type Newton struct {
	router                 *httprouter.Router
	kv                     *kv.KV
	partition              *partition.Partition
	nodeID                 string
	dataTransferRate       float64
	dataTransferBurstLimit int64
	writeTimeout           time.Duration
	readTimeout            time.Duration
	config                 *config.Newton
	messageIDStore         *messageIDStore
	clientStore            ClientStore
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

	messageIDStore, err := newMessageIDStore(str)
	if err != nil {
		return nil, err
	}

	partman, err := partition.New(&cfg.Partition)
	if err != nil {
		return nil, err
	}

	router := httprouter.New()
	kvStore := kv.New(partman, router)
	n := &Newton{
		router:                 router,
		kv:                     kvStore,
		partition:              partman,
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
	n.router.GET("/events/:name", n.eventsHandler)
	n.router.GET("/lookup/:name", n.lookupHandler)

	n.router.GET("/read/:name/:clientID/*routingKey", n.readHandler)
	n.router.HEAD("/read/:name/:clientID/*routingKey", n.readHandler)
	n.router.POST("/write-with-id/:messageID", n.messageIDHandler)

	n.router.POST("/write/:name/:clientID/*routingKey", n.writeHandler)
	n.router.GET("/read-with-id/:messageID", n.messageIDHandler)

	n.router.POST("/close/:messageID/:returnCode", n.closeHandler)

	ah := &AuthHandler{
		Handler:     handlers.RecoveryHandler()(n.router),
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

var ErrTimeout = errors.New("Timeout exceeded")

// Start starts a new Newton server instance and blocks until the server is closed.
func (n *Newton) Start() error {
	srv := n.createGracefulServer()
	// Wait for SIGTERM or SIGINT
	go n.waitForInterrupt(srv)

	log.Infof("Starting Newton instance with PID: %d, runtime: %s", os.Getpid(), runtime.Version())
	// First, we need to start a gRPC instance to register internal
	// gRPC services: Partition, KeyValue store and etc.
	g, err := newInternalGRPC(n.config.GrpcListen)
	if err != nil {
		return err
	}

	n.errGroup.Go(func() error {
		return g.start()
	})

	select {
	case <-time.After(5 * time.Second):
		log.Warn("gRPC server could not be started. Quitting.")
		return ErrTimeout
	case <-g.startChan:
		// Run as usual.
	}

	// Start a partition manager service at background
	n.errGroup.Go(func() error {
		psrv.RegisterPartitionServer(g.server, n.partition)
		return n.partition.Start()
	})

	select {
	case <-n.partition.StartChan:
	case <-n.partition.StopChan:
		return n.errGroup.Wait()
	}

	// Register KV store's GRPC endpoints
	ksrv.RegisterKVServer(g.server, n.kv.Grpc)

	// Finally we can start an external HTTP/2 service to process incoming requests.
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

	// Stop partition manager instance
	n.partition.Stop()
	select {
	case <-n.partition.StopChan:
	case <-time.After(newtonGracePeriod):
		log.Warn("Some goroutines did not finish in grace period. They will be killed.")
	}

	g.gracefulStop()
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
