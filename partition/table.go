package partition

import (
	"bytes"
	"encoding/gob"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/purak/newton/log"
)

const partitionCount int = 23

var (
	partitionTableLock   sync.RWMutex
	errPartitionTableSet = errors.New("Failed to set partition table")
)

type partitionTable struct {
	Partition map[int]string
	Members   map[string][]int
	Sorted    []memberSort
}

func (p *Partition) createPartitionTable() {
	defer p.waitGroup.Done()

	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	// That's first-run. We must trust the discovery subsystem.
	sorted := p.sortMembersByAge()
	memberCount := len(sorted)
	log.Infof("Forming a cluster with %d node(s)", memberCount)
	partID := 0
	for partID < partitionCount {
		memberIdx := partID % memberCount
		m := sorted[memberIdx]
		parts := p.table.Members[m.Addr]
		parts = append(parts, partID)
		p.table.Members[m.Addr] = parts
		p.table.Partition[partID] = m.Addr
		partID++
	}
	p.table.Sorted = sorted

	if memberCount > 1 {
		if err := p.pushPartitionTable(); err != nil {
			// TODO: We must re-try to push the table.
			log.Errorf("Error while pushing partition table: %s", err)
			return
		}
	}

	select {
	case <-p.nodeInitialized:
		return
	default:
	}
	close(p.nodeInitialized)
}

func (p *Partition) pushPartitionTable() error {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network) // Will write to network.
	if err := enc.Encode(p.table); err != nil {
		return err
	}
	serialized := network.Bytes()
	var g errgroup.Group
	for _, item := range p.table.Sorted {
		addr := item.Addr
		if addr == p.config.Address {
			// Dont send that message yourself.
			continue
		}
		g.Go(func() error {
			return p.setPartitionTable(addr, serialized)
		})
	}

	// Wait for all HTTP pushes to complete.
	return g.Wait()
}

func (p *Partition) setPartitionTable(addr string, serialized []byte) error {
	dst := url.URL{
		Scheme: "https",
		Host:   addr,
		Path:   "/partition-table/set",
	}

	req, err := http.NewRequest("POST", dst.String(), bytes.NewReader(serialized))
	if err != nil {
		return err
	}

	res, err := p.httpClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		log.Errorf("Error while setting partition table to %s. Status code: %d",
			addr, res.StatusCode)
		return errPartitionTableSet
	}
	return nil
}

func (p *Partition) joinCluster(addr string, birthdate int64) error {
	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	item := memberSort{
		Addr:      addr,
		Birthdate: birthdate,
	}
	p.table.Sorted = append(p.table.Sorted, item)
	sort.Sort(ByAge(p.table.Sorted))
	if err := p.pushPartitionTable(); err != nil {
		return err
	}
	log.Infof("%s has been joined the cluster.", addr)
	return nil
}

func (p *Partition) getCoordinatorMemberFromPartitionTable() string {
	partitionTableLock.RLock()
	defer partitionTableLock.RUnlock()
	if len(p.table.Sorted) == 0 {
		log.Debugf("No member found in partition table.")
		return ""
	}
	return p.table.Sorted[0].Addr
}
