package partition

import (
	"context"
	"errors"
	"sort"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/purak/newton/log"
	psrv "github.com/purak/newton/proto/partition"
)

const partitionCount int32 = 23

var (
	partitionTableLock    sync.RWMutex
	errPartitionTableSet  = errors.New("Failed to set partition table")
	ErrInvalidPartitionID = errors.New("Invalid partition ID")
)

func (p *Partition) createPartitionTable() {
	defer p.waitGroup.Done()

	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	// That's first-run. We must trust the discovery subsystem.
	sorted := p.sortMembersByAge()
	memberCount := int32(len(sorted))
	log.Infof("Forming a cluster with %d node(s)", memberCount)
	var partID int32
	for partID < partitionCount {
		memberIdx := partID % memberCount
		m := sorted[memberIdx]

		parts, ok := p.table.Members[m.Address]
		if !ok {
			parts = &psrv.PartitionsOfMember{
				Partitions: []int32{},
			}
		}
		parts.Partitions = append(parts.Partitions, partID)

		p.table.Members[m.Address] = parts
		p.table.Partitions[partID] = m.Address
		partID++
	}
	p.table.Sorted = sorted

	// Setup backup logic in consistent hashing logic.
	p.table.Backups = make(map[int32]string)
	p.table.BackupPartitionsOfMember = make(map[string]*psrv.BackupPartitionsOfMember)
	for idx, item := range p.table.Sorted {
		next := int32(idx + 1)
		if next >= memberCount {
			next = 0
		}
		// Get the next item, it's the backup node of the current one.
		nextItem := p.table.Sorted[next]
		// Copy integers
		backups := &psrv.BackupPartitionsOfMember{
			Partitions: []int32{},
		}
		for _, i := range p.table.Members[item.Address].Partitions {
			backups.Partitions = append(backups.Partitions, i)
			p.table.Backups[i] = nextItem.Address
		}
		p.table.BackupPartitionsOfMember[nextItem.Address] = backups
	}

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
	var g errgroup.Group
	for _, item := range p.table.Sorted {
		addr := item.Address
		if addr == p.config.Address {
			// Dont send that message yourself.
			continue
		}

		g.Go(func() error {
			m, err := p.getMember(addr)
			if err != nil {
				return err
			}
			conn := m.getConn()
			c := psrv.NewPartitionClient(conn)
			_, err = c.SetPartitionTable(context.Background(), p.table)
			return err
		})
	}

	// Wait for all HTTP pushes to complete.
	return g.Wait()
}

func (p *Partition) joinCluster(addr string, birthdate int64) error {
	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	item := &psrv.Member{
		Address:   addr,
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
	return p.table.Sorted[0].Address
}

func (p *Partition) FindPartitionOwner(partID int32) (string, bool, error) {
	partitionTableLock.RLock()
	defer partitionTableLock.RUnlock()
	rm, ok := p.table.Partitions[partID]
	if !ok {
		return "", false, ErrInvalidPartitionID
	}
	if rm == p.config.Address {
		return rm, true, nil
	}
	return rm, false, nil
}

func (p *Partition) FindBackupOwners(partID int32) ([]string, error) {
	partitionTableLock.RLock()
	defer partitionTableLock.RUnlock()
	b, ok := p.table.Backups[partID]
	if !ok {
		return nil, ErrInvalidPartitionID
	}
	// TODO: backup arrangement algorithm doesn't implemented yet.
	return []string{b}, nil
}

func (p *Partition) IsBackupOwner(partID int32, address string) (bool, error) {
	mm, err := p.FindBackupOwners(partID)
	if err != nil {
		return false, err
	}

	for _, mAddr := range mm {
		if mAddr == address {
			return true, nil
		}
	}
	return false, nil
}

func (p *Partition) AmIBackupOwner(partID int32) (bool, error) {
	mm, err := p.FindBackupOwners(partID)
	if err != nil {
		return false, err
	}

	for _, mAddr := range mm {
		if mAddr == p.config.Address {
			return true, nil
		}
	}
	return false, nil
}
