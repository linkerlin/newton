package partition

import (
	"context"
	"time"

	"github.com/purak/newton/log"

	psrv "github.com/purak/newton/proto/partition"
)

func (p *Partition) getCoordinatorMember() string {
	items := p.sortMembersByAge()
	return items[0].Address
}

func (p *Partition) splitBrainDetection() {
	ticker := time.NewTicker(time.Second)
	defer p.waitGroup.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			currentCoordinator := p.getCoordinatorMemberFromPartitionTable()
			if len(currentCoordinator) == 0 {
				continue
			}
			addr := p.getCoordinatorMember()
			if addr != currentCoordinator {
				log.Warnf("Contradiction between partition table and discovery subsystem. Coordinator is %s in partition table but discovery reports %s as coordinator.", currentCoordinator, addr)
				if p.config.Address == addr {
					p.waitGroup.Add(1)
					go p.takeOverCluster(currentCoordinator)
				}
				continue
			}
			log.Debugf("Current cluster coordinator is %s", currentCoordinator)
		case <-p.done:
			return
		}
	}
}

func (p *Partition) takeOverCluster(cAddr string) {
	defer p.waitGroup.Done()
	partitionTableLock.RLock()
	if len(p.table.Sorted) == 0 {
		// TODO: It should be impossible. Deal with this.
		log.Errorf("No member found in partition table")
		return
	}
	birthdate := p.table.Sorted[0].Birthdate
	partitionTableLock.RUnlock()

	// Try to reach out the old coordinator.
	bd, err := p.checkMember(cAddr)
	if err != nil {
		// It should be offline currently or deals with an internal error.
		log.Errorf("Error while checking aliveness of the old coordinator node: %s", err)
	} else {
		// Restarted. Take over the cluster
		if birthdate == bd {
			// TODO: Possible split brain. Deal with this.
			return
		}
	}

	if err := p.deleteMemberWithBirthdate(cAddr, birthdate); err != nil {
		log.Errorf("Error while deleting the old coordinator from discovery: %s", err)
	}

	// You are the new coordinator. Set a new partition table.1
	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()
	p.table.Sorted = p.sortMembersByAge()

	log.Infof("Taking over coordination role.")
	// TODO: re-try this in error condition
	if err := p.pushPartitionTable(); err != nil {
		log.Errorf("Error while pushing partition table.")
	}
}

func (p *Partition) checkMember(cAddr string) (int64, error) {
	m, err := p.getMember(cAddr)
	if err != nil {
		return 0, err
	}
	conn := m.getConn()
	c := psrv.NewPartitionClient(conn)
	r, err := c.Aliveness(context.Background(), &psrv.Dummy{})
	if err != nil {
		return 0, err
	}
	return r.Birthdate, nil
}

func (p *Partition) notifyCoordinator(addr string) error {
	cAddr := p.getCoordinatorMemberFromPartitionTable()
	if cAddr == p.config.Address {
		// That's me.
		p.tryCheckSuspiciousMember(addr)
		return nil
	}

	m, err := p.getMember(cAddr)
	if err != nil {
		return err
	}
	conn := m.getConn()
	c := psrv.NewPartitionClient(conn)
	req := &psrv.DenunciateRequest{
		Address: addr,
	}
	_, err = c.DenunciateForMember(context.Background(), req)
	return err
}

func (p *Partition) checkSuspiciousMember(addr string, birthdate int64) {
	defer p.waitGroup.Done()
	defer func() {
		p.suspiciousMembers.mu.Lock()
		delete(p.suspiciousMembers.m, addr)
		p.suspiciousMembers.mu.Unlock()
	}()

	count := 1
	// TODO: retry-count should be configurable.
	for count <= 4 {
		count++
		bd, err := p.checkMember(addr)
		if err != nil {
			log.Errorf("Error while checking %s: %s", addr, err)
			// Wait some time
			<-time.After(500 * time.Millisecond)
			continue
		}
		if bd == birthdate {
			return
		}
	}
	// Remove from partition table
	// TODO: Rearrangements in partition table and backups will be implemented in the future.
	partitionTableLock.Lock()
	for i, item := range p.table.Sorted {
		if item.Address != addr {
			continue
		}
		p.table.Sorted = append(p.table.Sorted[:i], p.table.Sorted[i+1:]...)
		// Remove it from discovery
		if err := p.deleteMember(addr); err != nil {
			log.Errorf("Error while deleting stale member from discovery subsystem: %s", err)
		}
		break
	}
	if err := p.pushPartitionTable(); err != nil {
		log.Errorf("Error while pushing partition table: %s", err)
	}
	partitionTableLock.Unlock()
}
