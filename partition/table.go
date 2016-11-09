package partition

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/purak/newton/log"
)

const partitionCount int32 = 523

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
	mm["1.1.1.1:2312"] = 0
	mm["2.2.2.2:2312"] = 0
	mm["3.3.3.3:2312"] = 0
	/*mm["4.4.4.4:2312"] = 0
	mm["5.5.5.5:2312"] = 0
	mm["6.6.6.6:2312"] = 0
	mm["7.7.7.7:2312"] = 0
	mm["8.8.8.8:2312"] = 0
	mm["9.9.9.9:2312"] = 0
	mm["10.10.10.10:2312"] = 0*/

	memberCount := int32(len(mm))
	var countPerMember, leap int32
	if partitionCount >= memberCount {
		countPerMember = 2 * (partitionCount / memberCount)
		leap = 2 * (partitionCount % memberCount)
	} else {
		countPerMember = 1
		leap = memberCount % partitionCount
	}
	distribution := make(map[string]int32)
	for address, _ := range mm {
		distribution[address] = countPerMember
	}

	fmt.Println(leap)
	var c int32 = 0
	if leap != 0 {
		for address, count := range distribution {
			c++
			distribution[address] = count + 1
			if c >= leap {
				break
			}
		}
	}

	replicationFactor := 2
	/*
		for address, count := range distribution {
			for count > 0 {
				partID := rand.Int31n(partitionCount)
				items := p.table.p[partID]
				if _, ok := items[address]; ok {
					continue
				}
				items[address] = struct{}{}
				p.table.p[partID] = items
				count--
			}
		}*/

	for partID, items := range p.table.p {
		i := 0
		for address, count := range distribution {
			if count <= 0 {
				continue
			}
			if i >= replicationFactor {
				break
			}
			if _, ok := items[address]; ok {
				continue
			}
			items[address] = struct{}{}
			p.table.p[partID] = items
			count--
			distribution[address] = count
			i++
			if len(items) < replicationFactor {
				continue
			}
			break
		}
	}
	fmt.Println(p.table.p)

	hede := make(map[string]int)
	for partID, items := range p.table.p {
		if len(items) != 2 {
			fmt.Println("item count for part ", partID, " is", len(items))
		}
		for address, _ := range items {
			c, ok := hede[address]
			if ok {
				c++
				hede[address] = c
			} else {
				hede[address] = 1
			}

		}
	}
	fmt.Println(countPerMember)
	fmt.Println(hede)

}
