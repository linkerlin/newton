package partition

import (
	"encoding/binary"
	"math/rand"
	"sync"

	"github.com/purak/newton/log"
)

const partitionCount int32 = 523
const replicationFactor int32 = 2

type partitionTable struct {
	mu sync.RWMutex
	p  map[int32]map[string]struct{}
}

// Hash consistently chooses a hash bucket number in the range [0, numBuckets) for the given key.
// numBuckets must be >= 1.
func hashConsistently(key uint64, numBuckets int) int32 {

	var b int64 = -1
	var j int64

	for j < int64(numBuckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}

	return int32(b)
}

func findPartitionID(address string) int32 {
	number := binary.LittleEndian.Uint64([]byte(address))
	return hashConsistently(number, int(partitionCount-1))
}

func (p *Partition) setupPartitionTable() {
	defer p.waitGroup.Done()
	p.table.mu.Lock()
	defer p.table.mu.Unlock()

	log.Info("Setting up a new partition table")
	var i int32
	for i = 0; i < partitionCount; i++ {
		p.table.p[i] = make(map[string]struct{})
	}

	mm := p.getMemberList()
	mm[p.config.Listen] = 0
	count := partitionCount * replicationFactor
	memberCount := len(mm)
	c := count / memberCount
	leap := count % memberCount

	for address, _ := range mm {
		var i int32
		for i = 0; i < c; i++ {
			partID := rand.Intn(partitionCount)

		}

		items := p.table.p[partID]
		items[address] = struct{}{}
		p.table.p[partID] = items
	}
}
