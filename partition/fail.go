package partition

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/purak/newton/log"
)

func (p *Partition) getCoordinatorMember() string {
	items := p.sortMembersByAge()
	return items[0].Addr
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
	dst := url.URL{
		Scheme: "https",
		Host:   cAddr,
		Path:   "/aliveness",
	}
	req, err := http.NewRequest("GET", dst.String(), nil)
	if err != nil {
		return 0, err
	}

	c := &http.Client{
		Transport: p.httpTransport,
		Timeout:   500 * time.Millisecond,
	}
	res, err := c.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("Member returned HTTP %d", res.StatusCode)
	}
	msg := &AlivenessMsg{}
	if err := json.NewDecoder(res.Body).Decode(msg); err != nil {
		return 0, err
	}
	return msg.Birthdate, nil
}

type CheckMember struct {
	Member string `json:"member"`
}

func (p *Partition) notifyCoordinator(addr string) error {
	cAddr := p.getCoordinatorMemberFromPartitionTable()
	if cAddr == p.config.Address {
		// That's me.
		p.tryCheckSuspiciousMember(cAddr)
		return nil
	}

	dst := url.URL{
		Scheme: "https",
		Host:   cAddr,
		Path:   "/check-member",
	}
	cm := CheckMember{
		Member: addr,
	}
	b := new(bytes.Buffer)
	json.NewEncoder(b).Encode(cm)
	req, err := http.NewRequest("POST", dst.String(), b)
	if err != nil {
		return err
	}
	c := &http.Client{
		Transport: p.httpTransport,
		Timeout:   500 * time.Millisecond,
	}
	res, err := c.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("Coordinator returned HTTP %d", res.StatusCode)
	}
	return nil
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
	for count <= 3 {
		count++
		bd, err := p.checkMember(addr)
		if err != nil {
			log.Errorf("Error while checking %s: %s", addr, err)
			// Wait some time
			<-time.After(time.Second)
			continue
		}
		if bd == birthdate {
			return
		}
	}
	// Remove from partition table
	// TODO: Rearrangements in partition table and backups will be implemented in the future.
	partitionTableLock.RLock()
	for i, item := range p.table.Sorted {
		if item.Addr != addr {
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
	partitionTableLock.RUnlock()
}
