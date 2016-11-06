// Copyright 2015 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dht

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net"
	"path"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sync/errgroup"

	"github.com/purak/newton/config"
	"github.com/purak/newton/log"
	"github.com/purak/newton/store"
)

// Some parts of our dht implementation are borrowed from nictuku's dht implementation in Go.
// I hardly inspired by Petar Maymounkov's Kademlia paper but we need some extensions on it to
// handle newton's use cases.

// The original kademlia paper can be found here:
// http://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf
// Nictuku's dht implementation is here:
// https://github.com/nictuku/dht

const (
	// dhtDB keeps NodeID and other things which needs to be stateful.
	dhtDB = "dht.db"

	// peerDB keeps a snapshot of known peers to restore after a process restart.
	peerDB = "dht.peer.db"

	// We use SHA1 to identify other nodes and clients on the network. hashSize is a system-wide constant
	// for Kademlia-based DHT implementation.
	hashSize = 20

	// maxCheckTime, time limitation for an async lookup request between DHT nodes.
	maxCheckTime = 900 // 15 minutes in seconds

	// expireInterval, time limit to expire an async lookup request.
	expireInterval int64 = 1000 // 1 second in milliseconds

	// newPeerExpInt, time limit to expire a freshly added peer before getting an answer.
	newPeerExpInt int64 = 60000
)

// newPeerMap stores peer addresses to prevent spam NewPeerSuccess messages.
type newPeerMap struct {
	sync.RWMutex

	m  map[string]bool
	pq *store.PriorityQueue
}

// DistributedHashTable represents a node in a Newton network.
type DistributedHashTable struct {
	nodeID                   string
	addr                     string
	config                   *config.DHT
	store                    *store.LevelDB
	unicastDiscoveryInterval time.Duration
	unicastSocket            *net.UDPConn
	multicastSocket          *ipv4.PacketConn
	done                     chan struct{}
	waitGroup                sync.WaitGroup
	requestWG                sync.WaitGroup
	serverErrGr              errgroup.Group
	newPeerMap               newPeerMap
	newPeerExpInt            int64
	routingTable             *routingTable
	remoteClients            *remoteClients
	remoteUserHashListeners  remoteUserHashListeners
	asyncStore               asyncStore
	StartChan                chan struct{}
	StopChan                 chan struct{}
	listening                chan struct{}
	closeUDPChan             chan struct{}
}

// New register required variables and runs some functions to initialize a DistributedHashTable node.
func New(c *config.DHT, addr string) (*DistributedHashTable, error) {
	// A basic LevelDB database to store basic info about grid
	gdb, err := store.OpenLevelDB(path.Join(c.DataDir, dhtDB))
	if err != nil {
		log.Fatalf("Database file: %s, could not be created or opened: %s", path.Join(c.DataDir, dhtDB), err)
	}
	discoveryInterval, err := time.ParseDuration(c.Unicast.DiscoveryInterval)
	if err != nil {
		return nil, err
	}

	done := make(chan struct{})
	d := &DistributedHashTable{
		addr:   addr,
		config: c,
		unicastDiscoveryInterval: discoveryInterval,
		store:     gdb,
		done:      done,
		listening: make(chan struct{}),

		newPeerMap: newPeerMap{
			m:  make(map[string]bool),
			pq: store.NewPriorityQueue(),
		},

		newPeerExpInt: newPeerExpInt,
		remoteClients: newRemoteClients(),

		remoteUserHashListeners: remoteUserHashListeners{
			c: make(map[string]map[chan []UserClientItem]struct{}),
		},

		asyncStore: asyncStore{
			req:            make(map[string]map[string]struct{}),
			counter:        make(map[string]int),
			pq:             store.NewPriorityQueue(),
			maxCheckTime:   maxCheckTime,
			expireInterval: expireInterval,
		},
		StopChan:     make(chan struct{}),
		StartChan:    make(chan struct{}),
		closeUDPChan: make(chan struct{}),
	}
	d.setOrCreateNodeID()

	// Create or load the routing table.
	r := &routingTable{
		tree:        &tree{},
		expireQueue: store.NewPriorityQueue(),
	}

	if !c.Multicast.Enabled {
		// Newton runs in unicast mode. Enable peer database to restore them from a database to avoid discovery overhead.
		pth := path.Join(c.DataDir, peerDB)
		// Setup leveldb databases here
		pdb, err := store.OpenLevelDB(pth)
		if err != nil {
			// Return err
			return nil, err
		}
		r.store = pdb
	}

	d.routingTable = r

	return d, nil
}

// AddClient announces given client to the DHT network. It's just an alias for AnnounceClient
func (d *DistributedHashTable) AddClient(userHash string, clientID uint32) error {
	return d.AnnounceClient(userHash, clientID)
}

// DelClient deletes a client from remote client store.
func (d *DistributedHashTable) DelClient(userHash string, clientID uint32) error {
	if !d.config.Multicast.Enabled {
		// DelClient is only available in multicast mode.
		return nil
	}
	dc := DeleteClientMsg{
		Action:   DeleteClient,
		NodeID:   d.nodeID,
		UserHash: userHash,
		ClientID: clientID,
	}
	data, err := serialize(dc)
	if err != nil {
		return err
	}
	return d.sendMessage(data, d.config.Listen)
}

var (
	// ErrPeerNotFound is an error that indicates an unavailable peer.
	ErrPeerNotFound = errors.New("Peer coult not be found")

	// ErrRemoteClientFound is an error that indicates an unavailable remote client.
	ErrRemoteClientFound = errors.New("No remote client found")

	// ErrDuplicateItem is an error that indicates a duplicate client or peer item.
	ErrDuplicateItem = errors.New("Duplicate item")

	// ErrUserHashNotFound is an error that indicates an unavailable UserHash on DHT.
	ErrUserHashNotFound = errors.New("UserHash could not be found")
)

// GetNodeID returns this DHT node's NodeID in string form.
func (d *DistributedHashTable) GetNodeID() string {
	return d.nodeID
}

// Stop sends DeleteNodeID message to the peers of this DHT and stops running goroutines.
func (d *DistributedHashTable) Stop() {
	// Send a message to multicast group, I'm done. Goodbye!
	// It's only enabled in multicast mode because, in unicast mode, it could be security problem.
	// Remembert that, unicast mode is designed for a BitTorrent like usage scenario.
	if d.config.Multicast.Enabled {
		select {
		case <-d.listening:
			dn := DeleteNodeMsg{
				Action: DeleteNode,
				NodeID: d.nodeID,
			}
			data, err := serialize(dn)
			if err != nil {
				log.Errorf("Error while serializing message. Action code: %d: %s", DeleteNode, err)
			} else {
				if err = d.sendMessage(data, d.config.Listen); err != nil {
					log.Errorf("Error while sending message through multicast enabled UDP socket. Action code: %d: %s", DeleteNode, err)
				}
			}
		default:
			log.Warn("No socket available to publish DeleteNode message. Skipping.")
		}
	}
	close(d.done)
}

// Start starts functions/goroutines to manage DistributedHashTable related tasks.
func (d *DistributedHashTable) Start() error {
	defer func() {
		log.Info("DHT instance has been closed.")
		close(d.StopChan)
	}()

	if d.config.Multicast.Enabled {
		if err := d.listenMulticastUDP(); err != nil {
			return err
		}
		if err := d.startMulticastDiscovery(); err != nil {
			return err
		}
	} else {
		if err := d.listenUnicastUDP(); err != nil {
			return err
		}

		if err := d.startUnicastDiscovery(); err != nil {
			return err
		}
	}

	// Start background workers after the UDP server has been started.
	<-d.listening

	d.waitGroup.Add(3)
	go d.cleanNewPeerMap()
	go d.checkAsyncRequests()
	go d.removeStaleRemoteClients()

	d.serverErrGr.Go(func() error {
		return d.stalePeerChecker()
	})

	close(d.StartChan)

	<-d.done

	// Wait until background workers has been closed.
	d.waitGroup.Wait()

	// Now, close UDP server
	close(d.closeUDPChan)

	// Wait until running operations in server has been finished.
	d.requestWG.Wait()

	// Wait until UDP server has been closed.
	return d.serverErrGr.Wait()
}

// AddPeer sends "NewPeer" message to a remote host to be peer. The message is send through UDP port.
func (d *DistributedHashTable) AddPeer(addr string) error {
	d.newPeerMap.Lock()
	defer d.newPeerMap.Unlock()

	// Prepare data to send to a remote host.
	msg := NewPeerMsg{
		Action:     NewPeer,
		Identifier: d.config.Identifier,
		NodeID:     d.nodeID,
	}
	data, err := serialize(msg)
	if err != nil {
		return err
	}
	if err = d.sendMessage(data, addr); err != nil {
		return err
	}
	// add it to the newPeerMap to prevent spam NewPeerSuccess messages.
	if _, ok := d.newPeerMap.m[addr]; !ok {
		d.newPeerMap.m[addr] = true
		d.newPeerMap.pq.Add(addr, d.newPeerExpInt)
	}
	return nil
}

// AnnounceClient announces given client to other Newton instances.
func (d *DistributedHashTable) AnnounceClient(userHash string, clientID uint32) error {
	peers, err := d.findClosestPeers(userHash)
	if err != nil {
		return err
	}

	for _, peer := range peers {
		msg := AnnounceClientItemMsg{
			Action:   AnnounceClientItem,
			UserHash: userHash,
			ClientID: clientID,
			NodeID:   d.nodeID,
			Addr:     d.addr,
		}
		data, err := serialize(msg)
		if err != nil {
			log.Errorf("Error while serializing message. Code: %d, Error: %s", AnnounceClientItem, err)
			continue
		}
		err = d.sendMessage(data, peer.addr)
		if err != nil {
			log.Errorf("Error while sending UDP message: %v", err)
			continue
		}
	}
	return nil
}

func (d *DistributedHashTable) getRemoteClients(userHash string) ([]UserClientItem, error) {
	remoteClients := d.remoteClients.getClients(userHash)
	if remoteClients == nil {
		return nil, ErrRemoteClientFound
	}

	items := []UserClientItem{}
	for _, rClient := range remoteClients {
		p := d.getPeer(rClient.nodeID)
		if p == nil {
			log.Debug("Peer could not be found: ", rClient.nodeID)
			continue
		}
		m := UserClientItem{
			ClientID: rClient.clientID,
			Addr:     p.addr,
		}
		items = append(items, m)
	}
	return items, nil
}

// cleanNewPeerMap keeps clean newPeerMap structure by removing expired items from the map.
func (d *DistributedHashTable) cleanNewPeerMap() {
	tick := time.NewTicker(1000 * time.Millisecond)
	defer func() {
		tick.Stop()
		d.waitGroup.Done()
	}()

	for {
		select {
		case <-tick.C:
			d.newPeerMap.Lock()
			if d.newPeerMap.pq.Len() != 0 {
				addr := d.newPeerMap.pq.Expire()
				if addr != nil {
					delete(d.newPeerMap.m, addr.(string))
				}
			}
			d.newPeerMap.Unlock()
		case <-d.done:
			return
		}
	}
}

// setOrCreateNodeID sets a new NodeId or creates it
func (d *DistributedHashTable) setOrCreateNodeID() {
	// NodeID identifies this instance in Kademlia network(DistributedHashTable)
	i, err := d.store.Get([]byte("nodeId"))
	if err != nil {
		log.Fatal("Getting nodeId is failed.")
	}
	if len(i) == 0 {
		i, err = RandNodeID()
		if err != nil {
			log.Fatal("nodeId rand:", err)
		}
		if err := d.store.Set([]byte("nodeId"), i); err != nil {
			log.Fatalf("Error while setting nodeID: %v", err)
		}
		log.Info("New NodeID is generated.")
	}
	d.nodeID = hex.EncodeToString(i)
}

// RandNodeID generates a random 20-byte Node ID for Kademlia DHT.
func RandNodeID() ([]byte, error) {
	// Borrowed from: https://github.com/nictuku/dht/blob/d31162c7ecb5a7f06c81c659fd676e368bc779d1/dht.go#L928
	b := make([]byte, 20)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

// SHA1Validation checks validity of given hash string
func SHA1Validation(hash string) bool {
	b, err := hex.DecodeString(hash)
	if err != nil {
		return false
	}
	// SHA-1 produces a 160-bit (20-byte) hash value. A SHA-1 hash value is typically rendered as a hexadecimal number, 40 digits long
	if len(b) == 20 {
		return true
	}
	return false
}

// makeTimestamp returns current timestamp in millisecond format.
func makeTimestamp() int64 {
	return time.Now().UnixNano() / 1000000
}

func (d *DistributedHashTable) startListening() {
	select {
	case <-d.listening:
		return
	default:
	}
	close(d.listening)
	return
}
