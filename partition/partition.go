package partition

import (
	"bytes"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"google.golang.org/grpc"

	"golang.org/x/sync/errgroup"

	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
	psrv "github.com/purak/newton/proto/partition"
	"golang.org/x/net/context"
)

const ClockMonotonicRaw uintptr = 4

type Partition struct {
	config            *config.DHT
	partitionSrv      *grpc.Server
	birthdate         int64
	members           *members
	unicastSocket     *net.UDPConn
	serverErrGr       errgroup.Group
	requestWG         sync.WaitGroup
	waitGroup         sync.WaitGroup
	table             *psrv.PartitionTable
	suspiciousMembers *suspiciousMembers
	StartChan         chan struct{}
	StopChan          chan struct{}
	listening         chan struct{}
	closeUDPChan      chan struct{}
	nodeInitialized   chan struct{}
	done              chan struct{}
}

func clockMonotonicRaw() int64 {
	var ts syscall.Timespec
	syscall.Syscall(syscall.SYS_CLOCK_GETTIME, ClockMonotonicRaw, uintptr(unsafe.Pointer(&ts)), 0)
	sec, nsec := ts.Unix()
	return time.Unix(sec, nsec).UnixNano()
}

func New(c *config.DHT) (*Partition, error) {
	sMembers := &suspiciousMembers{
		m: make(map[string]struct{}),
	}

	pTable := &psrv.PartitionTable{
		Partitions: make(map[int32]string),
		Members:    make(map[string]*psrv.PartitionsOfMember),
		Sorted:     []*psrv.Member{},
	}

	p := &Partition{
		config:            c,
		birthdate:         time.Now().UnixNano(),
		members:           newMembers(),
		table:             pTable,
		suspiciousMembers: sMembers,
		StartChan:         make(chan struct{}),
		StopChan:          make(chan struct{}),
		listening:         make(chan struct{}),
		closeUDPChan:      make(chan struct{}),
		nodeInitialized:   make(chan struct{}),
		done:              make(chan struct{}),
	}
	return p, nil
}

func (p *Partition) checkgRPCAliveness(gRPCStarted chan struct{}) {
	defer p.waitGroup.Done()

	callYourself := func() error { // Set up a connection to the server.
		conn, err := grpc.Dial(p.config.Listen, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer func() {
			if err := conn.Close(); err != nil {
				log.Errorf("Error while closing connection: %s", err)
			}
		}()
		c := psrv.NewPartitionClient(conn)
		_, err = c.Aliveness(context.Background(), &psrv.Dummy{})
		if err != nil {
			return err
		}
		return nil
	}

	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if err := callYourself(); err != nil {
				log.Errorf("Error while checking gRPC server: %s", err)
				continue
			}
			close(gRPCStarted)
			return
		case <-p.done:
			return
		}
	}
}

func (p *Partition) runPartitionService(gRPCStarted chan struct{}) error {
	ln, err := net.Listen("tcp", p.config.Listen)
	if err != nil {
		return err
	}
	p.partitionSrv = grpc.NewServer()
	psrv.RegisterPartitionServer(p.partitionSrv, p)
	log.Infof("Partition manager runs on %s", p.config.Listen)

	p.waitGroup.Add(1)
	go p.checkgRPCAliveness(gRPCStarted)

	err = p.partitionSrv.Serve(ln)
	if err != nil {
		if opErr, ok := err.(*net.OpError); !ok || (ok && opErr.Op != "accept") {
			log.Debugf("Something wrong with gRPC server: %s", err)
			// We call Close here because if HTTP/2 server fails, we cannot run this process anymore.
			// This block can be invoked during normal closing process but Close method will handle
			// redundant Close call.
			p.Close()
			return err
		}
		// It's just like this: "accept tcp [::]:10000: use of closed network connection". Ignore it.
		return nil
	}
	return nil
}

// Run fires up a new partition table.
func (p *Partition) Run() error {
	defer func() {
		log.Info("Partition manager has been stopped.")
		close(p.StopChan)
	}()

	gRPCStarted := make(chan struct{})
	p.serverErrGr.Go(func() error {
		return p.runPartitionService(gRPCStarted)
	})

	// TODO: We may need to set a timer and return about an error about timeout?
	<-gRPCStarted

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

	p.partitionSrv.GracefulStop()

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
