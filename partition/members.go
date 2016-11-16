package partition

import (
	"errors"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/purak/newton/log"
)

const (
	joinMessageFlag      byte = 0
	heartbeatMessageFlag byte = 1
)

type member struct {
	mu sync.RWMutex

	addr, ip     string
	lastActivity int64
	birthdate    int64
	available    bool
}

type members struct {
	mu sync.RWMutex

	m    map[string]*member
	byIP map[string]*member
}

var (
	memberDeadLimit int64 = 1000000000 // 1 second in nanoseconds
)

var (
	errDifferentBirthdate = errors.New("Birthdate is different")
	errMemberAlreadyExist = errors.New("Member already exist")
	errMemberNotFound     = errors.New("Member could not be found")
)

func newMembers() *members {
	return &members{
		m:    make(map[string]*member),
		byIP: make(map[string]*member),
	}
}

func (p *Partition) addMember(addr, ip string, birthdate int64) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	m, ok := p.members.m[addr]
	if ok {
		if m.birthdate != birthdate {
			return errDifferentBirthdate
		}
		return errMemberAlreadyExist
	}

	if len(ip) == 0 {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return err
		}

		ips, err := net.LookupHost(host)
		if err != nil {
			return err
		}
		// TODO: We should consider to support multiple IP address for an hostname.
		ip = net.JoinHostPort(ips[0], port)
	}

	now := clockMonotonicRaw()
	nm := &member{
		addr:         addr,
		ip:           ip,
		lastActivity: now,
		birthdate:    birthdate,
		available:    true,
	}
	p.members.m[addr] = nm
	p.members.byIP[ip] = nm

	p.waitGroup.Add(1)
	go p.checkAliveness(addr)
	log.Infof("New member has been added, Host: %s, IP: %s", addr, ip)
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
				bd, err := p.checkMember(addr)
				if err != nil || bd != m.birthdate {
					// Notify the coordinator
					if nErr := p.notifyCoordinator(addr); nErr != nil {
						log.Errorf("Error while notifying the coordinator node about an unhealthy member: %s", nErr)
					}
				}
				m.available = false
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

func (p *Partition) checkMemberWithBirthdate(addr string, birthdate int64) bool {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	member, ok := p.members.m[addr]
	if !ok {
		return false
	}

	return member.birthdate == birthdate
}

func (p *Partition) updateMemberByIP(ip string) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	m, ok := p.members.byIP[ip]
	if !ok {
		return errMemberNotFound
	}

	m.mu.Lock()
	m.available = true
	m.lastActivity = clockMonotonicRaw()
	m.mu.Unlock()

	p.members.byIP[ip] = m

	log.Debugf("Member: %s is still alive", ip)
	return nil
}

func (p *Partition) updateMember(addr string) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	m, ok := p.members.m[addr]
	if !ok {
		return errMemberNotFound
	}

	m.mu.Lock()
	m.available = true
	m.lastActivity = clockMonotonicRaw()
	m.mu.Unlock()

	log.Debugf("Member: %s is still alive", addr)
	return nil
}

func (p *Partition) deleteMember(addr string) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	m, ok := p.members.m[addr]
	if !ok {
		return errMemberNotFound
	}
	delete(p.members.byIP, m.ip)
	delete(p.members.m, addr)
	log.Infof("Member has been deleted: %s", addr)
	return nil
}

func (p *Partition) deleteMemberWithBirthdate(addr string, birthdate int64) error {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	m, ok := p.members.m[addr]
	if !ok {
		return errMemberNotFound
	}
	if m.birthdate != birthdate {
		return errDifferentBirthdate
	}
	delete(p.members.byIP, m.ip)
	delete(p.members.m, addr)
	log.Infof("Member has been deleted: %s", addr)
	return nil
}

func (p *Partition) memberCount() int {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()
	return len(p.members.m)
}

func (p *Partition) heartbeatPeriodically() {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer p.waitGroup.Done()
	defer ticker.Stop()

	payload := []byte{heartbeatMessageFlag}
	for {
		select {
		case <-ticker.C:
			p.members.mu.Lock()
			for addr, item := range p.members.m {
				if !item.available {
					continue
				}
				go func(payload []byte, addr string) {
					log.Debugf("Sending heartbeat message to %s", addr)
					if err := p.sendMessage(payload, addr); err != nil {
						log.Errorf("Error while sending heartbeat message to %s: %s", addr, err)
					}
				}(payload, addr)
			}
			p.members.mu.Unlock()
		case <-p.done:
			return
		}
	}
}

func (p *Partition) getMemberList2() []string {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()

	// Get a thread-safe copy of members struct
	mm := []string{}
	for addr, _ := range p.members.m {
		mm = append(mm, addr)
	}
	return mm
}

func (p *Partition) getMemberList() map[string]int64 {
	p.members.mu.Lock()
	defer p.members.mu.Unlock()
	// Get a thread-safe copy of members struct
	mm := make(map[string]int64)
	for addr, item := range p.members.m {
		if !item.available {
			continue
		}
		mm[addr] = item.birthdate
	}
	return mm
}

type memberSort struct {
	Addr      string
	Birthdate int64
}

type ByAge []memberSort

func (a ByAge) Len() int           { return len(a) }
func (a ByAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAge) Less(i, j int) bool { return a[i].Birthdate < a[j].Birthdate }

func (p *Partition) sortMembersByAge() []memberSort {
	items := []memberSort{}
	mm := p.getMemberList()
	// Add itself
	mm[p.config.Address] = p.birthdate
	for addr, birthdate := range mm {
		item := memberSort{
			Addr:      addr,
			Birthdate: birthdate,
		}
		items = append(items, item)
	}
	sort.Sort(ByAge(items))
	return items

}
