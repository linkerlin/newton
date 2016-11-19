package partition

import (
	"errors"
	"net"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/purak/newton/log"
	psrv "github.com/purak/newton/proto/partition"
)

const (
	joinMessageFlag      byte = 0
	heartbeatMessageFlag byte = 1
)

type member struct {
	mu sync.RWMutex

	conn         *grpc.ClientConn
	addr, ip     string
	lastActivity int64
	birthdate    int64
	available    bool
}

func (m *member) getConn() *grpc.ClientConn {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.conn
}

func (m *member) getAddr() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.addr
}

func (m *member) getIP() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.ip
}
func (m *member) getLastActivity() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastActivity
}

func (m *member) getBirthdate() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.birthdate
}

func (m *member) getAvailable() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.available
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
		if m.getBirthdate() != birthdate {
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

	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	now := clockMonotonicRaw()
	nm := &member{
		conn:         conn,
		addr:         addr,
		ip:           ip,
		lastActivity: now,
		birthdate:    birthdate,
		available:    true,
	}
	p.members.m[addr] = nm
	p.members.byIP[ip] = nm

	p.waitGroup.Add(1)
	go p.checkAliveness(addr, birthdate)
	log.Infof("New member has been added, Host: %s, IP: %s", addr, ip)
	return nil
}

func (p *Partition) checkAliveness(addr string, birthdate int64) {
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
				log.Errorf("Error while getting member %s: %s", addr, err)
				continue
			}
			if !p.checkMemberWithBirthdate(addr, birthdate) {
				log.Warnf("Stopping aliveness check for %s, birth date: %d", addr, birthdate)
				return
			}
			// Network operations may take a long time. Use locks wisely.
			dead := m.getLastActivity()+memberDeadLimit < clockMonotonicRaw()
			if dead {
				bd, err := p.checkMember(addr)
				if err != nil || bd != birthdate {
					// Notify the coordinator
					if nErr := p.notifyCoordinator(addr); nErr != nil {
						log.Errorf("Error while notifying the coordinator node about an unhealthy member: %s", nErr)
					}
					m.mu.Lock()
					m.available = false
					m.mu.Unlock()
				}
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
	// FIXME: This may be useless.
	m.mu.RLock()
	ip := m.ip
	m.mu.RUnlock()

	m.conn.Close()
	delete(p.members.byIP, ip)
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
	if m.getBirthdate() != birthdate {
		return errDifferentBirthdate
	}
	m.conn.Close()
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
				item.mu.RLock()
				if !item.available {
					item.mu.RUnlock()
					continue
				}
				item.mu.RUnlock()
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
		item.mu.RLock()
		if !item.available {
			item.mu.RUnlock()
			continue
		}
		mm[addr] = item.birthdate
		item.mu.RUnlock()
	}
	return mm
}

type ByAge []*psrv.Member

func (a ByAge) Len() int           { return len(a) }
func (a ByAge) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByAge) Less(i, j int) bool { return a[i].Birthdate < a[j].Birthdate }

func (p *Partition) sortMembersByAge() []*psrv.Member {
	items := []*psrv.Member{}
	mm := p.getMemberList()
	// Add itself
	mm[p.config.Address] = p.birthdate
	for addr, birthdate := range mm {
		item := &psrv.Member{
			Address:   addr,
			Birthdate: birthdate,
		}
		items = append(items, item)
	}
	sort.Sort(ByAge(items))
	return items

}
