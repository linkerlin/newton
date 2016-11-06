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

package newton

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/purak/newton/store"
)

// ClientStore interfaces provides basic services to handle Newton clients.
type ClientStore interface {
	// AddClient adds a new client to the store
	AddClient(userHash string, conn *connection) (uint32, error)
	// CheckClient checks availability of a client with given UserHash and ClientID variables.
	CheckClient(userHash string, clientID uint32) bool
	// DelClient deletes a client from the store with given UserHash and ClientID variables.
	DelClient(userHash string, clientID uint32)
	// GetClients retrieves client objects from the storage for given UserHash if available.
	GetClients(userHash string) []uint32
	// GetClient retrieves the connection object of the client from the store for given UserHash if available.
	GetClient(userHash string, clientID uint32) *connection
}

// clients maps ClientIDs to connection objects while keeping ClientID head for this Newton node.
type clients struct {
	// ClientIDs to connections
	c map[uint32]*connection
}

var pstoreKey []byte = []byte("clientID-head")

// userTable stores active client sessions by UserHash .
type clientStore struct {
	sync.RWMutex

	head   uint32
	pstore *store.LevelDB
	m      map[string]*clients
}

// NewClientStore returns a new client store instance.
func NewClientStore(str *store.LevelDB) ClientStore {
	return &clientStore{
		head:   0,
		pstore: str,
		m:      make(map[string]*clients),
	}
}

// AddClient adds a new client to the store
func (c *clientStore) AddClient(userHash string, conn *connection) (uint32, error) {
	c.Lock()
	defer c.Unlock()

	if c.head == 0 {
		// Get ClientID from persistent store.
		i, err := c.pstore.Get(pstoreKey)
		if err != nil {
			return 0, err
		}
		if len(i) != 0 {
			buf := bytes.NewReader(i)
			err := binary.Read(buf, binary.BigEndian, &c.head)
			if err != nil {
				return 0, err
			}
		}
	}

	// Generate a unique ClientID and write it to persistent store.
	c.head++
	// for readability
	clientID := c.head
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, clientID)
	if err != nil {
		return 0, err
	}
	err = c.pstore.Set(pstoreKey, buf.Bytes())
	if err != nil {
		return 0, err
	}

	// Register this client and return its ID
	clientItems, ok := c.m[userHash]
	if ok {
		clientItems.c[clientID] = conn
		c.m[userHash] = clientItems
	} else {
		cl := &clients{
			c: make(map[uint32]*connection),
		}
		cl.c[clientID] = conn
		c.m[userHash] = cl
	}

	return clientID, nil
}

// DelClient deletes a client from the store with given UserHash and ClientID variables.
func (c *clientStore) DelClient(userHash string, clientID uint32) {
	c.Lock()
	defer c.Unlock()

	if clientItems, ok := c.m[userHash]; ok {
		if _, ok = clientItems.c[clientID]; ok {
			delete(clientItems.c, clientID)
			if len(clientItems.c) == 0 {
				delete(c.m, userHash)
			}
		}
	}
}

// GetClient retrieves the connection object of the client from the store for given UserHash if available.
func (c *clientStore) GetClient(userHash string, clientID uint32) *connection {
	c.Lock()
	defer c.Unlock()
	if clientItems, ok := c.m[userHash]; ok {
		if conn, ok := clientItems.c[clientID]; ok {
			return conn
		}
	}
	return nil
}

// GetClients retrieves client objects from the storage for given UserHash if available.
func (c *clientStore) GetClients(userHash string) []uint32 {
	c.Lock()
	defer c.Unlock()
	var res []uint32
	if clients, ok := c.m[userHash]; ok {
		for clientID, _ := range clients.c {
			res = append(res, clientID)
		}
	}
	return res
}

// CheckClient checks availability of a client with given UserHash and ClientID variables.
func (c *clientStore) CheckClient(userHash string, clientID uint32) bool {
	c.Lock()
	defer c.Unlock()

	if clients, ok := c.m[userHash]; ok {
		if _, ok = clients.c[clientID]; ok {
			return true
		}
	}
	return false
}
