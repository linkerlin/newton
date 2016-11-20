package partition

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sync/errgroup"

	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
	psrv "github.com/purak/newton/proto/partition"
	"golang.org/x/net/context"
)

const ClockMonotonicRaw uintptr = 4

type Partition struct {
	config            *config.Partition
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

func New(c *config.Partition) (*Partition, error) {
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

var ErrTimeout = errors.New("Timeout exceeded")

// Run fires up a new partition table.
func (p *Partition) Start() error {
	defer func() {
		log.Info("Partition manager has been stopped.")
		close(p.StopChan)
	}()

	if err := p.listenUnicastUDP(); err != nil {
		return err
	}

	select {
	case <-time.After(5 * time.Second):
		log.Warn("UDP server could not be started. Quitting.")
		return ErrTimeout
	case <-p.listening:
		// Run as usual.
	}

	// Start background workers after the UDP server has been started.
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

	log.Infof("Partition manager has been started.")
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
			for _, addr := range p.config.Unicast.Members {
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

func (p *Partition) Stop() {
	select {
	case <-p.done:
		// Already closed
		return
	default:
	}
	close(p.done)
}
