package partition

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
)

type Partition struct {
	config        *config.DHT
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

	done chan struct{}
}

func New(router *httprouter.Router, c *config.DHT) (*Partition, *httprouter.Router, error) {
	p := &Partition{
		config:       c,
		birthdate:    time.Now().UnixNano(),
		members:      newMembers(),
		StartChan:    make(chan struct{}),
		StopChan:     make(chan struct{}),
		listening:    make(chan struct{}),
		closeUDPChan: make(chan struct{}),
		done:         make(chan struct{}),
	}
	return p, router, nil
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

	<-p.done

	// Wait until background workers has been closed.
	p.waitGroup.Wait()

	// Now, close UDP server
	close(p.closeUDPChan)

	// Wait until running operations in server has been finished.
	p.requestWG.Wait()

	// Wait until UDP server has been closed.
	return p.serverErrGr.Wait()
}

func (p *Partition) Stop() {
	close(p.done)
}
