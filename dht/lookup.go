package dht

import (
	"errors"
	"math/rand"
	"sync"
	"time"

	"github.com/purak/newton/log"
)

type remoteUserHashListeners struct {
	sync.RWMutex

	c map[string]map[chan []UserClientItem]struct{}
}

var (
	// ErrTimeout is an error that indicates a timeout in current request.
	ErrTimeout = errors.New("Timeout exceeded")

	// ErrNoClient is an error that indicates an unavailable UserHash on Newton network.
	ErrNoClient = errors.New("No client found for UserHash")

	// ErrNoClosePeer is an error that indicates no peer can found to ask or replicate the given UserHash
	ErrNoClosePeer = errors.New("No close peer for UserHash")
)

// FindClients runs a lookup query on DHT and returns UserClientItem array or an error.
func (d *DistributedHashTable) FindClients(userHash string) ([]UserClientItem, error) {
	var clients []UserClientItem
	rClients := d.remoteClients.getClients(userHash)
	if rClients != nil {
		for _, r := range rClients {
			c := UserClientItem{
				NodeID:   r.nodeID,
				ClientID: r.clientID,
				Addr:     r.extAddr,
			}
			clients = append(clients, c)
		}
	}

	if len(clients) != 0 {
		return clients, nil
	}

	if d.config.Multicast.Enabled {
		// Apply multicast lookup rules
		clients, err := d.multicastLookupUserhash(userHash)
		if err != nil {
			return nil, err
		}
		return clients, nil
	}

	// Run a query on DHT to find that UserHash's clients.
	// This query may take a long time, because of that we just ping
	// peers about the UserHash. Rest of the work will be done by asyncStore
	// engine.

	// Apply unicast lookup rules
	if _, ok := d.asyncStore.req[userHash]; !ok {
		err := d.unicastLookupUserHash(userHash)
		if err != nil {
			return nil, err
		}
		d.asyncStore.req[userHash] = make(map[string]struct{})
		d.asyncStore.pq.Add(userHash, d.asyncStore.expireInterval)
	}

	// No client found on this DHT instance, but we asked about that UserHash to our peers.
	// We expect a result in a short period of time if it's available on the network.
	// This period cannot be acceptable in a request. Because of that we return ErrNoClient.
	// We may return a proper result after some time. Wait for it.
	return nil, ErrNoClient
}

func (d *DistributedHashTable) multicastLookupUserhash(userHash string) ([]UserClientItem, error) {
	peers, err := d.findClosestPeers(userHash)
	if err != nil {
		return nil, err
	}
	msg := LookupUserHashMsg{
		Action:   LookupUserHash,
		NodeID:   d.nodeID,
		UserHash: userHash,
	}
	dt, _ := serialize(msg)
	ch := make(chan []UserClientItem)

	d.remoteUserHashListeners.Lock()
	if chans, ok := d.remoteUserHashListeners.c[userHash]; ok {
		chans[ch] = struct{}{}
	} else {
		d.remoteUserHashListeners.c[userHash] = make(map[chan []UserClientItem]struct{})
		d.remoteUserHashListeners.c[userHash][ch] = struct{}{}
	}
	d.remoteUserHashListeners.Unlock()

	l := len(peers)
	var c int
	// Prevent reuse of peers
	p := make(map[int]struct{})
	for c <= l {
		ri := rand.Intn(l)
		if _, ok := p[ri]; ok {
			continue
		}
		peer := peers[ri]
		p[ri] = struct{}{}
		c++
		err := d.sendMessage(dt, peer.addr)
		if err != nil {
			log.Errorf("Error while sending LookupUserHash message to %s", peer.addr)
			continue
		}

		select {
		case <-time.After(100 * time.Millisecond):
			log.Debugf("No answer from %s for UserHash: %d", peer.addr, userHash)
		case clients := <-ch:
			return clients, err
		}
	}

	d.remoteUserHashListeners.Lock()
	delete(d.remoteUserHashListeners.c[userHash], ch)
	d.remoteUserHashListeners.Unlock()
	return nil, ErrNoClient
}

// unicastLookupUserHash finds the closest peers in its routing table for  the given UserHash and sends
// lookupUserHash message all the closest peers.
func (d *DistributedHashTable) unicastLookupUserHash(userHash string) error {
	peers, err := d.findClosestPeers(userHash)
	if err != nil {
		return err
	}
	msg := LookupUserHashMsg{
		Action:   LookupUserHash,
		NodeID:   d.nodeID,
		UserHash: userHash,
	}
	dt, err := serialize(msg)
	if err != nil {
		return err
	}
	for _, peer := range peers {
		err := d.sendMessage(dt, peer.addr)
		if err != nil {
			log.Errorf("Error while sending LookupUserHash message to %s", peer.addr)
			continue
		}
	}
	return nil
}

// asyncLookupRequest triggers lookup mechanisim for the given UserHash in unicast mode.
func (d *DistributedHashTable) asyncLookupRequest(userHash, addr string) error {
	d.asyncStore.Lock()
	defer d.asyncStore.Unlock()
	if addrs, ok := d.asyncStore.req[userHash]; ok {
		if _, ok = addrs[addr]; ok {
			return nil
		}
	} else {
		if err := d.unicastLookupUserHash(userHash); err != nil {
			return err
		}
		d.asyncStore.counter[userHash] = 1
		d.asyncStore.req[userHash] = make(map[string]struct{})
		d.asyncStore.pq.Add(userHash, d.asyncStore.expireInterval)
	}
	d.asyncStore.req[userHash][addr] = struct{}{}
	return nil
}
