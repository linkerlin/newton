package partition

import "github.com/purak/newton/log"

func (p *Partition) setupPartitionTable() {
	defer p.waitGroup.Done()

	log.Info("Setting up a new partition table")
}
