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
	"encoding/json"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/purak/newton/log"
	"github.com/purak/newton/store"
)

// ErrInvalidNodeID means that the given NodeID is a malformed or invalid sort of invalid array of bytes.
var ErrInvalidNodeID = errors.New("Invalid NodeID")

type checkPeerEvent struct {
	nodeID string
	Chan   chan bool
}

// peer represents a remote newton node.
type peer struct {
	nodeID       string
	addr         string
	lastActivity int64
}

// PeerData is a struct to transfer a peer's data between states or databases.
type PeerData struct {
	Addr         string
	LastActivity int64
}

// routingTable is a struct to keep remote peers in k-buckets.
type routingTable struct {
	mu sync.RWMutex

	store       *store.LevelDB // LevelDB connection
	tree        *tree
	expireQueue *store.PriorityQueue // A binary heap based priority queue to catch inactive peers.
}

// setupPeers is a function that runs at start-up and inserts peers into in-memory database.
func (d *DistributedHashTable) setupPeers() ([]string, error) {
	// Using iterator.Iterator in this function should not be create a race condition on the DB snapshot.
	// goleveldb's documentation says that:
	//
	// NewIterator returns an iterator for the latest snapshot of the underlying DB.
	// The returned iterator is not safe for concurrent use, but it is safe to use multiple iterators concurrently,
	// with each in a dedicated goroutine. It is also safe to use an iterator concurrently with modifying its underlying DB.
	// The resultant key/value pairs are guaranteed to be consistent.
	//
	// We use levelDB in a sequential fashion in this function.
	addrs := []string{}
	iter := d.routingTable.store.NewIterator(nil, nil)
	defer iter.Release()
	for iter.Next() {
		key := hex.EncodeToString(iter.Key())
		var value PeerData
		err := json.Unmarshal(iter.Value(), &value)
		if err != nil {
			log.Error("An error occured while processing peer key: ", key, " Error: ", err)
			continue
		}

		log.Info("NodeID from peer database: ", key)
		now := makeTimestamp()
		if value.LastActivity > now-d.config.InactivityThreshold {
			// Reset lastActivity to prevent removing the peers immediately.
			err := d.addPeer(key, value.Addr, now)
			if err != nil {
				log.Error("Error while adding peer. NodeID: ", key, " Addr: ", value.Addr, " Error: ", err)
			}
		} else {
			addrs = append(addrs, value.Addr)
		}
	}
	return addrs, iter.Error()
}

// addPeer adds given peer to the in-memory database.
func (d *DistributedHashTable) addPeer(nodeID string, addr string, lastActivitiy int64) error {
	if !SHA1Validation(nodeID) {
		log.Error("Invalid NodeID: ", nodeID, " IP: ", addr, " Skipping.")
		return ErrInvalidNodeID
	}

	pr := d.getPeer(nodeID)
	if pr != nil {
		if pr.addr == addr {
			return ErrDuplicateItem
		}

		log.Warn("Address changed for NodeID: %s, removing old record: %s -> %s", pr.addr, addr)
		if err := d.deletePeer(pr.nodeID); err != nil {
			return err
		}
	}

	d.routingTable.mu.Lock()
	defer d.routingTable.mu.Unlock()

	d.routingTable.expireQueue.Add(nodeID, d.config.PeerCheckInterval)
	if !d.config.Multicast.Enabled {
		// Store this data in the persistent data store.
		pd := PeerData{
			Addr:         addr,
			LastActivity: lastActivitiy,
		}
		jp, err := json.Marshal(pd)
		if err != nil {
			log.Error("Error while marshaling PeerData. NodeID ", nodeID, " Error: ", err)
			return err
		}
		err = d.setPeer(nodeID, jp)
		if err != nil {
			log.Error("Error while inserting peer to leveldb. NodeID ", nodeID, " Error: ", err)
			return err
		}
	}
	// Main peer structure
	p := peer{
		nodeID:       nodeID,
		lastActivity: lastActivitiy,
		addr:         addr,
	}
	log.Debug("Inserting new peer NodeID: ", nodeID, " Address: ", addr)
	// We use a tree structure to to find closest peers for a given UserHash
	d.routingTable.tree.insert(&p)
	return nil
}

// checkPeer checks availability of a peer.
func (d *DistributedHashTable) checkPeer(nodeID string) bool {
	d.routingTable.mu.RLock()
	defer d.routingTable.mu.RUnlock()
	if peer := d.routingTable.tree.getPeer(nodeID); peer != nil {
		return peer.nodeID == nodeID
	}
	return false
}

// deletePeer removes a peer from its k-bucket and leveldb database.
func (d *DistributedHashTable) deletePeer(nodeID string) error {
	d.routingTable.mu.Lock()
	defer d.routingTable.mu.Unlock()
	if ok := d.routingTable.tree.cut(nodeID, 0); !ok {
		return errors.New("Item could not be deleted")
	}
	if !d.config.Multicast.Enabled {
		id, err := hex.DecodeString(nodeID)
		if err != nil {
			return err
		}
		if err := d.routingTable.store.Delete(id); err != nil {
			// TODO: make something to avoid garbage item in persistent database.
			return err
		}
	}
	log.Debug(nodeID, " has been deleted from routing table.")
	return nil

}

// GetPeer retrieves a peer from its k-bucket.
func (d *DistributedHashTable) getPeer(nodeID string) *peer {
	d.routingTable.mu.RLock()
	defer d.routingTable.mu.RUnlock()

	if p := d.routingTable.tree.getPeer(nodeID); p != nil {
		if p.nodeID == nodeID {
			return p
		}
	}
	return nil
}

// updateLastActivity updates given peer to the in-memory database.
func (d *DistributedHashTable) updateLastActivity(nodeID, addr string, lastActivity int64) error {
	d.routingTable.mu.Lock()
	defer d.routingTable.mu.Unlock()
	p := d.routingTable.tree.getPeer(nodeID)
	if p == nil {
		return ErrPeerNotFound
	}

	// p is a pointer to peer struct. Just update it.
	p.lastActivity = lastActivity

	// we only need this in unicast mode.
	if !d.config.Multicast.Enabled {
		// Update LevelDB database
		pd := PeerData{
			Addr:         p.addr,
			LastActivity: p.lastActivity,
		}
		jp, _ := json.Marshal(pd)
		if err := d.setPeer(nodeID, jp); err != nil {
			// TODO: Handle this properly.
			log.Error("Error while inserting peer to leveldb. NodeID ", nodeID, " Error: ", err)
			return err
		}
	}
	return nil
}

// setPeer sets a peer to the persistent database.
func (d *DistributedHashTable) setPeer(nodeID string, data []byte) error {
	key, err := hex.DecodeString(nodeID)
	if err != nil {
		return err
	}
	err = d.store.Set(key, data)
	if err != nil {
		log.Error("Setting peer is failed. Key: ", key, " Error: ", err)
		return nil
	}
	return nil
}

// stalePeerChecker fetches peers from expire queue, remove expired items and sends them to heartbeat queue.
func (d *DistributedHashTable) stalePeerChecker() error {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer ticker.Stop()

L:
	for {
		select {
		case <-ticker.C:
			d.removeStalePeers()
		case <-d.done:
			log.Info("Stopping stale peer checker.")
			break L
		}
	}
	if d.config.Multicast.Enabled {
		return nil
	}
	log.Info("Closing local peer storage.")
	err := d.routingTable.store.Close()
	if err != nil {
		log.Errorf("Error while closing local storage of routing table: %s", err)
	}
	return err
}

// removeStalePeers removes stale peers from routing table.
func (d *DistributedHashTable) removeStalePeers() {
	d.routingTable.mu.Lock()
	defer d.routingTable.mu.Unlock()

	if d.routingTable.expireQueue.Len() == 0 {
		return
	}

	// This returns a nodeID for a peer.
	n := d.routingTable.expireQueue.Expire()
	if n == nil {
		// nothing to expire
		return
	}
	nodeID := n.(string)
	p := d.routingTable.tree.getPeer(nodeID)
	if p == nil {
		log.Debug("Peer could not be found with NodeID: ", nodeID)
		return
	}
	now := makeTimestamp()
	if now >= p.lastActivity+d.config.InactivityThreshold {
		// Delete this peer from database. We have to run this as a goroutine here
		// because the following function locks RoutingTable to work like removeStalePeers.
		go func() {
			if err := d.deletePeer(p.nodeID); err != nil {
				log.Errorf("Error while deleting NodeID: %s from routing table", p.nodeID)
			}
		}()
		return
	}
	if !d.config.Multicast.Enabled {
		if err := d.unicastHeartbeat(p.addr); err != nil {
			log.Errorf("Error while sending heartbeat message through unicast UDP socket: %s", err)
		}
	}
	log.Debug("NodeID: ", nodeID, " IP: ", p.addr, " is alive. Pushing again to ExpireQueue. lastActivity: ", p.lastActivity)
	d.routingTable.expireQueue.Add(nodeID, d.config.PeerCheckInterval)
}

func (d *DistributedHashTable) unicastHeartbeat(addr string) error {
	// Send heartbeat message via UDP tracker
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}
	msg := PeerHeartbeatMsg{
		NodeID: d.nodeID,
		Action: PeerHeartbeat,
	}
	data, err := serialize(msg)
	if err != nil {
		return err
	}

	log.Debug("Sending heartbeat message to ", addr)
	um := udpMessage{Data: data, UDPAddr: uaddr}
	_, err = d.writeToUDP(um)
	return err
}

type replicationCandidate struct {
	nodeID string
	addr   string
}

func (d *DistributedHashTable) findClosestPeers(userHash string) ([]replicationCandidate, error) {
	d.routingTable.mu.RLock()
	defer d.routingTable.mu.RUnlock()
	var ret []replicationCandidate
	peers := d.routingTable.tree.lookup(userHash)
	for _, p := range peers {
		r := replicationCandidate{
			nodeID: p.nodeID,
			addr:   p.addr,
		}
		ret = append(ret, r)
	}
	if len(ret) == 0 {
		return nil, ErrNoClosePeer
	}
	return ret, nil
}

var errEmptyTree = errors.New("Empty tree")

func (d *DistributedHashTable) getPeerAddrsRandomly() ([]string, error) {
	d.routingTable.mu.RLock()
	defer d.routingTable.mu.RUnlock()
	b := make([]byte, hashSize)
	_, err := rand.Read(b)
	if err != nil {
		return nil, err
	}
	rr := hex.EncodeToString(b)
	ret := d.routingTable.tree.lookup(rr)
	if len(ret) == 0 {
		return nil, errEmptyTree
	}
	var peers []string
	for _, p := range ret {
		peers = append(peers, p.addr)
	}
	return peers, nil
}
