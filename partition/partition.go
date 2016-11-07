package partition

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	graceful "gopkg.in/tylerb/graceful.v1"

	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
)

type Partition struct {
	config        *config.DHT
	httpServer    *graceful.Server
	birthdate     int64
	members       *members
	unicastSocket *net.UDPConn
	serverErrGr   errgroup.Group
	requestWG     sync.WaitGroup
	waitGroup     sync.WaitGroup
	StartChan     chan struct{}
	StopChan      chan struct{}
	listening     chan struct{}
	closeUDPChan  chan struct{}
	done          chan struct{}
}

func New(c *config.DHT) (*Partition, error) {
	router := httprouter.New()
	s := &http.Server{
		Addr:    c.Listen,
		Handler: router,
	}
	http2.ConfigureServer(s, nil)
	srv := &graceful.Server{
		NoSignalHandling: true,
		Server:           s,
	}

	p := &Partition{
		config:       c,
		httpServer:   srv,
		birthdate:    time.Now().UnixNano(),
		members:      newMembers(),
		StartChan:    make(chan struct{}),
		StopChan:     make(chan struct{}),
		listening:    make(chan struct{}),
		closeUDPChan: make(chan struct{}),
		done:         make(chan struct{}),
	}
	return p, nil
}

// Run fires up a new partition table.
func (p *Partition) Run() error {
	defer func() {
		log.Info("DHT instance has been closed.")
		close(p.StopChan)
	}()
	if err := p.listenUnicastUDP(); err != nil {
		return err
	}

	// Start background workers after the UDP server has been started.
	<-p.listening

	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, p.birthdate); err != nil {
		return err
	}
	payload := buf.Bytes()
	log.Info("Trying to join the cluster")
	for _, addr := range p.config.Unicast.Peers {
		addr = strings.Trim(addr, " ")
		log.Debugf("Sending heartbeat message to %s", addr)
		if err := p.sendMessage(payload, addr); err != nil {
			log.Errorf("Error while sending heartbeat message to %s: %s", addr, err)
		}
	}

	p.waitGroup.Add(1)
	go p.heartbeatPeriodically(payload)

	p.waitGroup.Add(1)
	go p.sortMembersPeriodically()

	// Wait for cluster join

	close(p.StartChan)

	p.serverErrGr.Go(func() error {
		log.Infof("Listening HTTP/2 connections for partition manager on %s", p.config.Listen)
		err := p.httpServer.ListenAndServeTLS(p.config.CertFile, p.config.KeyFile)
		if err != nil {
			if opErr, ok := err.(*net.OpError); !ok || (ok && opErr.Op != "accept") {
				log.Debugf("Something wrong with HTTP/2 server: %s", err)
				// We call Close here because if HTTP/2 server fails, we cannot run this process anymore.
				// This block can be invoked during normal closing process but Close method will handle
				// redundant Close call.
				p.Close()
			}
			log.Errorf("Error while running HTTP/2 server on %s: %s", p.config.Listen, err)
			return err
		}
		return nil

	})

	<-p.done

	// Wait until background workers has been closed.
	p.waitGroup.Wait()

	// Try to close HTTP server
	stopChan := p.httpServer.StopChan()
	p.httpServer.Stop(5 * time.Second) // http server
	select {
	case <-time.After(6 * time.Second):
	case <-stopChan:
	}

	// Now, close UDP server
	close(p.closeUDPChan)

	// Wait until running operations in server has been finished.
	p.requestWG.Wait()

	// Wait until UDP server has been closed.
	return p.serverErrGr.Wait()
}

func (p *Partition) Close() {
	select {
	case <-p.done:
		// Already closed
		return
	default:
	}

	// Remove this item from cluster
	mm := p.getMemberList()
	var payload []byte
	for addr, _ := range mm {
		log.Debugf("Sending delete message to %s", addr)
		if err := p.sendMessage(payload, addr); err != nil {
			log.Errorf("Error while sending delete message to %s: %s", addr, err)
		}
	}

	close(p.done)
}
