package partition

import (
	"fmt"
	"sync"

	"github.com/purak/newton/log"
)

const partitionCount int = 23

type partitionTable struct {
	mu        sync.RWMutex
	partition map[int]string
	members   map[string][]int
	sorted    []memberSort
}

func (p *Partition) setupPartitionTable() {
	defer p.waitGroup.Done()

	p.table.mu.Lock()
	defer p.table.mu.Unlock()

	log.Info("Setting up a new partition table")
	sorted := p.sortMembersByAge()
	memberCount := len(sorted)
	partID := 0
	for partID < partitionCount {
		memberIdx := partID % memberCount
		m := sorted[memberIdx]
		parts := p.table.members[m.addr]
		parts = append(parts, partID)
		p.table.members[m.addr] = parts
		p.table.partition[partID] = m.addr
		partID++
	}
	p.table.sorted = sorted

	fmt.Println(p.table)
}
