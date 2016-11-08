package partition

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/purak/newton/log"
)

type member struct {
	mu sync.RWMutex

	lastActivity int64
	birthdate    int64
}

type members struct {
	mu sync.RWMutex

	m map[string]*member
}

var (
	becomeLeader    string = "me"
	memberDeadLimit int64  = 1000000000 // 1 second in nanoseconds
)

var (
	errMemberAlreadyExist = errors.New("Member already exist")
	errMemberNotFound     = errors.New("Member could not be found")
)

func newMembers() *members {
	return &members{
		m: make(map[string]*member),
	}
}

func (p *Partition) addMember(addr string, birthdate int64) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	if _, ok := p.members.m[addr]; ok {
		return errMemberAlreadyExist
	}

	now := clockMonotonicRaw()
	member := &member{
		lastActivity: now,
		birthdate:    birthdate,
	}
	p.members.m[addr] = member
	p.waitGroup.Add(1)
	go p.checkAliveness(addr)
	log.Infof("New member has been added: %s", addr)
	return nil
}

func (p *Partition) checkAliveness(addr string) {
	defer p.waitGroup.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m, err := p.getMember(addr)
			if err == errMemberNotFound {
				log.Debugf("Member: %s could not be found. Quitting.", addr)
				return
			}
			if err != nil {
				log.Errorf("Error while checking lastActivity of %d", addr)
				continue
			}
			m.mu.RLock()
			dead := m.lastActivity+memberDeadLimit < clockMonotonicRaw()
			m.mu.RUnlock()

			if dead {
				err = p.deleteMember(addr)
				if err == errMemberNotFound {
					err = nil
				}
				if err != nil {
					log.Errorf("Error while deleting stale member from cluster: %s", err)
					// Don't quit. This can be important. Just keep logging about this.
					continue
				}
				return
			}
		case <-p.done:
			return
		}
	}
}

func (p *Partition) getMember(addr string) (*member, error) {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	member, ok := p.members.m[addr]
	if !ok {
		return nil, errMemberNotFound
	}

	return member, nil
}

func (p *Partition) updateMember(addr string) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	member, ok := p.members.m[addr]
	if !ok {
		return errMemberNotFound
	}

	member.lastActivity = clockMonotonicRaw()
	p.members.m[addr] = member

	log.Debugf("Member: %s is still alive", addr)
	return nil
}

func (p *Partition) deleteMember(addr string) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	if _, ok := p.members.m[addr]; !ok {
		return errMemberNotFound
	}
	delete(p.members.m, addr)
	log.Infof("Member has been deleted: %s", addr)
	return nil
}

func (p *Partition) getMemberList() map[string]int64 {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()
	// Get a thread-safe copy of members struct
	mm := make(map[string]int64)
	for addr, item := range p.members.m {
		mm[addr] = item.birthdate
	}
	return mm
}

func (p *Partition) memberCount() int {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()
	return len(p.members.m)
}

func (p *Partition) heartbeatPeriodically(payload []byte) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer p.waitGroup.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			mm := p.getMemberList()
			for _, addr := range p.config.Unicast.Peers {
				if _, ok := mm[addr]; !ok {
					// FIXME: Try to re-add them. This mechanisim should be reconsidered after configuration hot-loading feature is implemented.
					mm[addr] = 0
				}
			}
			for addr, _ := range mm {
				go func(payload []byte, addr string) {
					log.Debugf("Sending heartbeat message to %s", addr)
					if err := p.sendMessage(payload, addr); err != nil {
						log.Errorf("Error while sending heartbeat message to %s: %s", addr, err)
					}
				}(payload, addr)
			}
		case <-p.done:
			return
		}
	}
}

type memberSort struct {
	addr      string
	birthdate int64
}

type ByAge []memberSort

func (a ByAge) Len() int           { return len(a) }
func (a ByAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAge) Less(i, j int) bool { return a[i].birthdate < a[j].birthdate }

func (p *Partition) sortByAge() []memberSort {
	items := []memberSort{}
	mm := p.getMemberList()
	// Add itself
	mm[becomeLeader] = p.birthdate
	for addr, birthdate := range mm {
		item := memberSort{
			addr:      addr,
			birthdate: birthdate,
		}
		items = append(items, item)
	}
	sort.Sort(ByAge(items))
	return items

}

func (p *Partition) sortMembersPeriodically() {
	ticker := time.NewTicker(time.Second)
	defer p.waitGroup.Done()
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			items := p.sortByAge()
			item := items[0]
			log.Debugf("Current cluster leader is %s", item.addr)
			if item.addr == becomeLeader {
				// Take the leadership
			} else {
			}
		case <-p.done:
			return
		}
	}
}
