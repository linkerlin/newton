package partition

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	graceful "gopkg.in/tylerb/graceful.v1"

	"golang.org/x/net/http2"
	"golang.org/x/sync/errgroup"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
)

const CLOCK_MONOTONIC_RAW uintptr = 4

type Partition struct {
	config          *config.DHT
	httpServer      *graceful.Server
	httpClient      *http.Client
	birthdate       int64
	members         *members
	unicastSocket   *net.UDPConn
	serverErrGr     errgroup.Group
	requestWG       sync.WaitGroup
	waitGroup       sync.WaitGroup
	table           *partitionTable
	StartChan       chan struct{}
	StopChan        chan struct{}
	listening       chan struct{}
	closeUDPChan    chan struct{}
	nodeInitialized chan struct{}
	done            chan struct{}
}

func clockMonotonicRaw() int64 {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, CLOCK_MONOTONIC_RAW, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec).UnixNano()
}

func New(c *config.DHT) (*Partition, error) {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	table := &partitionTable{
		Partition: make(map[int]string),
		Members:   make(map[string][]int),
		Sorted:    []memberSort{},
	}

	p := &Partition{
		config:          c,
		httpClient:      &http.Client{Transport: tr},
		birthdate:       time.Now().UnixNano(),
		members:         newMembers(),
		table:           table,
		StartChan:       make(chan struct{}),
		StopChan:        make(chan struct{}),
		listening:       make(chan struct{}),
		closeUDPChan:    make(chan struct{}),
		nodeInitialized: make(chan struct{}),
		done:            make(chan struct{}),
	}
	return p, nil
}

func (p *Partition) checkHTTPAliveness(httpStarted chan struct{}) {
	defer p.waitGroup.Done()

	dst := url.URL{
		Scheme: "https",
		Host:   p.config.Listen,
		Path:   "/aliveness",
	}
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			req, err := http.NewRequest("HEAD", dst.String(), nil)
			if err != nil {
				log.Errorf("Error while creating new request: ", err)
				continue
			}
			res, err := p.httpClient.Do(req)
			if err != nil {
				// Probably "connection refused"
				log.Debugf("Error while checking aliveness: %s", err)
			}
			if res.StatusCode == http.StatusOK {
				close(httpStarted)
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *Partition) runHTTPServerAtBackground(httpStarted chan struct{}) error {
	p.waitGroup.Add(1)
	go p.checkHTTPAliveness(httpStarted)

	log.Infof("Partition manager listens %s", p.config.Listen)
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
}

// Run fires up a new partition table.
func (p *Partition) Run() error {
	defer func() {
		log.Info("DHT instance has been closed.")
		close(p.StopChan)
	}()

	router := httprouter.New()
	router.HEAD("/aliveness", p.alivenessHandler)
	router.GET("/aliveness", p.alivenessHandler)
	router.POST("/partition-table/set", p.partitionSetHandler)
	s := &http.Server{
		Addr:    p.config.Listen,
		Handler: router,
	}
	http2.ConfigureServer(s, nil)
	p.httpServer = &graceful.Server{
		NoSignalHandling: true,
		Server:           s,
	}

	httpStarted := make(chan struct{})
	// TODO: We must close HTTP server if something went wrong in this function.
	p.serverErrGr.Go(func() error {
		return p.runHTTPServerAtBackground(httpStarted)
	})

	// TODO: We may need to set a timer and return about an error about timeout?
	<-httpStarted

	if err := p.listenUnicastUDP(); err != nil {
		return err
	}

	// Start background workers after the UDP server has been started.
	<-p.listening

	buf := new(bytes.Buffer)
	if err := buf.WriteByte(joinMessageFlag); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, p.birthdate); err != nil {
		return err
	}

	src := []byte(p.config.Address)
	if _, err := buf.Write(src); err != nil {
		return err
	}

	payload := buf.Bytes()
	p.waitGroup.Add(1)
	go p.tryToJoinCluster(payload)

	// Wait for cluster join
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	p.waitGroup.Add(1)
	go p.waitForConsensus(ctx)

	<-p.nodeInitialized
	cancel()

	close(p.StartChan)

	p.waitGroup.Add(1)
	go p.heartbeatPeriodically()

	p.waitGroup.Add(1)
	go p.splitBrainDetection()

	<-p.done

	// Wait until background workers has been closed.
	p.waitGroup.Wait()

	// Try to close HTTP server
	stopChan := p.httpServer.StopChan()
	p.httpServer.Stop(1 * time.Second) // http server
	select {
	case <-time.After(2 * time.Second):
	case <-stopChan:
	}

	// Now, close UDP server
	close(p.closeUDPChan)

	// Wait until running operations in server has been finished.
	p.requestWG.Wait()

	// Wait until UDP server has been closed.
	return p.serverErrGr.Wait()
}

func (p *Partition) waitForConsensus(ctx context.Context) {
	defer p.waitGroup.Done()
	select {
	case <-ctx.Done():
	case <-p.nodeInitialized:
		log.Debugf("Cluster formed by someone else.")
		return
	}

	c := p.memberCount()
	if c != 0 {
		master := p.getCoordinatorMember()
		log.Infof("Coordinator member is %s", master)
		// Check the master node. If you are the master, form a cluster.
		if master != p.config.Address {
			return
		}
	}
	p.waitGroup.Add(1)
	go p.createPartitionTable()
	return
}

func (p *Partition) tryToJoinCluster(payload []byte) {
	defer p.waitGroup.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	// TODO: Add multicast support

	log.Info("Trying to join the cluster")
	for {
		select {
		case <-ticker.C:
			for _, addr := range p.config.Unicast.Peers {
				addr = strings.Trim(addr, " ")
				log.Debugf("Sending join message to %s", addr)
				if err := p.sendMessage(payload, addr); err != nil {
					log.Errorf("Error while sending heartbeat message to %s: %s", addr, err)
				}
			}
		case <-p.nodeInitialized:
			return
		case <-p.done:
			return
		}
	}
}

func (p *Partition) Close() {
	select {
	case <-p.done:
		// Already closed
		return
	default:
	}

	close(p.done)
}
