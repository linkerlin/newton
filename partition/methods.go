package partition

import (
	"errors"
	"sync"

	"golang.org/x/net/context"

	"github.com/purak/newton/log"
	psrv "github.com/purak/newton/proto/partition"
)

func (p *Partition) Aliveness(ctx context.Context, in *psrv.Dummy) (*psrv.AlivenessResponse, error) {
	return &psrv.AlivenessResponse{
		Birthdate: p.birthdate,
	}, nil
}

type suspiciousMembers struct {
	mu sync.RWMutex

	m map[string]struct{}
}

func (p *Partition) DenunciateForMember(ctx context.Context, in *psrv.DenunciateRequest) (*psrv.Dummy, error) {
	p.tryCheckSuspiciousMember(in.Address)
	return &psrv.Dummy{}, nil
}

func (p *Partition) tryCheckSuspiciousMember(addr string) {
	p.suspiciousMembers.mu.Lock()
	_, ok := p.suspiciousMembers.m[addr]
	if !ok {
		// TODO: We should keep members in a map with their birthdate for god's sake.
		partitionTableLock.RLock()
		for _, item := range p.table.Sorted {
			if item.Address != addr {
				continue
			}
			p.waitGroup.Add(1)
			p.suspiciousMembers.m[addr] = struct{}{}
			go p.checkSuspiciousMember(addr, item.Birthdate)
		}
		partitionTableLock.RUnlock()
	}
	p.suspiciousMembers.mu.Unlock()
}

func (p *Partition) SetPartitionTable(ctx context.Context, table *psrv.PartitionTable) (*psrv.Dummy, error) {
	partitionTableLock.Lock()
	defer partitionTableLock.Unlock()

	// Check the sender if you already initialized.
	select {
	case <-p.nodeInitialized:
		cn := p.getCoordinatorMember()
		cAddr := table.Sorted[0].Address
		if cn != cAddr {
			log.Warnf("%s tried to set a partition table. Rejected!", cAddr)
			return nil, errors.New("CONFLICT")
		}
	default:
	}

	log.Infof("Partition table received from coordinator node: %s", table.Sorted[0].Address)
	p.table = table

	// Remove stale members
	list := make(map[string]struct{})
	for _, item := range p.table.Sorted {
		list[item.Address] = struct{}{}
	}
	for _, addr := range p.getMemberList2() {
		if _, ok := list[addr]; !ok {
			err := p.deleteMember(addr)
			if err == errMemberNotFound {
				err = nil
			}
			if err != nil {
				log.Errorf("Error while deleting stale member from discovery subsystem: %s", err)
			}
		}
	}

	for _, item := range table.Sorted {
		if item.Address == p.config.Address {
			continue
		}
		// We may catch a restarted member. Check the birhdate.
		if !p.checkMemberWithBirthdate(item.Address, item.Birthdate) {
			err := p.deleteMember(item.Address)
			if err == errMemberNotFound {
				err = nil
			}
			if err != nil {
				log.Errorf("Error while deleting member from discovery subsystem: %s", err)
			}
		}
		err := p.addMember(item.Address, "", item.Birthdate)
		if err == errMemberAlreadyExist {
			err = nil
		}
		if err != nil {
			log.Errorf("Error while adding member to discovery subsystem: %s", err)
		}
	}

	select {
	case <-p.nodeInitialized:
		return &psrv.Dummy{}, nil
	default:
	}
	close(p.nodeInitialized)

	return &psrv.Dummy{}, nil
}
