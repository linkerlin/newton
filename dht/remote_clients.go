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
	"errors"
	"sync"
	"time"

	"github.com/purak/newton/log"
	"github.com/purak/newton/store"
)

var staleInterval int64 = 1000 // 1 second in milliseconds

type rclient struct {
	nodeID   string
	clientID uint32
	// Address of the Newton's HTTP server. It can be in host:port or a domain:port form.
	extAddr string
	// Address of the Newton's UDP server
	addr string
	// Last announce in milliseconds
	lastAnnounce int64
}

// remoteClient keeps minimum required information to manage and distribute users over the Newton network.
type remoteClient struct {
	// Address of the Newton's HTTP server. It can be in host:port or a domain:port form.
	extAddr string

	// Address of the Newton's UDP server
	addr string

	// Last announce in milliseconds
	lastAnnounce int64
}

// remoteClients stores currently connected and replicated clients on this node.
type remoteClients struct {
	sync.RWMutex

	// Maximum inactivity duration in millisecods.
	staleInterval int64

	// A hashmap that stores remote clients of a UserHash value. UserHash:NodeID:ClientID:remoteClient
	cs map[string]map[string]map[uint32]remoteClient

	// Priority queue of UserHash values
	pq *store.PriorityQueue
}

// NewRemoteClients returns a new LocalClient instance.
func newRemoteClients() *remoteClients {
	l := &remoteClients{
		cs:            make(map[string]map[string]map[uint32]remoteClient),
		staleInterval: staleInterval,
		pq:            store.NewPriorityQueue(),
	}
	return l
}

// SetClient sets a new client item into the remote clients.
func (l *remoteClients) setClient(userHash, nodeID, addr, extAddr string, clientID uint32) error {
	l.Lock()
	defer l.Unlock()

	item := remoteClient{
		addr:         addr,
		extAddr:      extAddr,
		lastAnnounce: makeTimestamp(),
	}

	if nodes, ok := l.cs[userHash]; ok {
		// Add the new item to a existed UserHash value
		// We should find out an existing item in the table and set if it doesn't exist.
		clients, ok := nodes[nodeID]
		if ok {
			if _, ok = clients[clientID]; ok {
				return ErrDuplicateItem
			}
			clients[clientID] = item
			return nil
		}
		log.Debug("Setting a new client to UserHash: ", userHash, " ClientID: ", clientID)
		// Here we add a new item to the existed items
		//clients := make(map[uint32]remoteClient)
		clients[clientID] = item
		nodes[nodeID] = clients
		return nil

	}
	log.Debug("Setting a new client with a fresh UserHash: ", userHash, " ClientID: ", clientID)
	// set up a fresh slice on the table and insert replication item.
	nodes := make(map[string]map[uint32]remoteClient)
	clients := make(map[uint32]remoteClient)
	clients[clientID] = item
	nodes[nodeID] = clients
	l.cs[userHash] = nodes
	l.pq.Add(userHash, l.staleInterval)
	return nil
}

// getClients returns all known remote clients for the given UserHash, if it's possible.
func (l *remoteClients) getClients(userHash string) []rclient {
	l.RLock()
	defer l.RUnlock()

	nodes, ok := l.cs[userHash]
	if !ok {
		return nil
	}
	rcs := []rclient{}
	for nodeID, clients := range nodes {
		for clientID, client := range clients {
			rc := rclient{
				nodeID:       nodeID,
				clientID:     clientID,
				lastAnnounce: client.lastAnnounce,
				extAddr:      client.extAddr,
				addr:         client.addr,
			}
			rcs = append(rcs, rc)
		}
	}
	return rcs
}

// checkClient looks for a client item on this node with given values.
func (l *remoteClients) checkClient(userHash, nodeID string, clientID uint32) bool {
	l.RLock()
	defer l.RUnlock()

	nodes, ok := l.cs[userHash]
	if !ok {
		return false
	}
	clients, ok := nodes[nodeID]
	if !ok {
		return false
	}
	_, res := clients[clientID]
	return res
}

// UpdateLastAnnounce updates last announce field for the given client.
func (l *remoteClients) updateLastAnnounce(userHash, nodeID string, clientID uint32) {
	l.Lock()
	defer l.Unlock()

	nodes, ok := l.cs[userHash]
	if !ok {
		return
	}
	clients, ok := nodes[nodeID]
	if !ok {
		return
	}

	if client, ok := clients[clientID]; ok {
		log.Debugf("Updating LastAnnounce for UserHash: %s, NodeID: %s, ClientID: %d ", userHash, nodeID, clientID)
		client.lastAnnounce = makeTimestamp()
		clients[clientID] = client
	}
	return
}

var (
	// ErrNodeIDNotFound means that the given NodeID cannot be found on DHT.
	ErrNodeIDNotFound = errors.New("NodeID could not be found")

	// ErrClientIDNotFound means that the given ClientID cannot be found on DHT.
	ErrClientIDNotFound = errors.New("ClientID could not be found")
)

// DelClient deletes a client from remote client store.
func (l *remoteClients) delClient(userHash, nodeID string, clientID uint32) error {
	l.Lock()
	defer l.Unlock()

	nodes, ok := l.cs[userHash]
	if !ok {
		return ErrUserHashNotFound
	}

	clients, ok := nodes[nodeID]
	if !ok {
		return ErrNodeIDNotFound
	}
	_, ok = clients[clientID]
	if !ok {
		return ErrClientIDNotFound
	}

	log.Debug("Deleting client UserHash: ", userHash, " ClientID: ", clientID)
	delete(clients, clientID)

	if len(clients) == 0 {
		delete(nodes, nodeID)
	}
	if len(nodes) == 0 {
		delete(l.cs, userHash)
	}
	return nil
}

func (l *remoteClients) processStaleClients() {
	l.Lock()
	defer l.Unlock()

	if l.pq.Len() == 0 {
		// nothing to expire, empty queue
		return
	}
	uh := l.pq.Expire()
	if uh == nil {
		// nothing to expire
		return
	}
	userHash := uh.(string)

	log.Debug("Checking remote clients for UserHash: ", userHash)
	nodes, ok := l.cs[userHash]
	if !ok {
		return
	}

	for nodeID, clients := range nodes {
		for clientID, client := range clients {
			if makeTimestamp()-l.staleInterval >= client.lastAnnounce {
				log.Debugf("Deleting ClientID: %d for UserHash: %s NodeID: %s", clientID, userHash, nodeID)
				delete(clients, clientID)
				if len(clients) == 0 {
					delete(nodes, nodeID)
				}
				if len(nodes) == 0 {
					log.Debug("Deleting UserHash completely: ", userHash)
					delete(l.cs, userHash)
				}
				return
			}

		}
	}

	l.pq.Add(userHash, l.staleInterval)
}

func (d *DistributedHashTable) removeStaleRemoteClients() {
	defer d.waitGroup.Done()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.remoteClients.processStaleClients()
		case <-d.done:
			log.Info("Stop checking remote clients.")
			return
		}
	}
}
