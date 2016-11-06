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
	"encoding/hex"
	"testing"
	"time"
)

var userHash = "94f5f8dd64b297708de84ee2bec9d198987082f7"

func setClient(c *remoteClients, count int) error {
	n1, err := RandNodeID()
	if err != nil {
		return err

	}
	nodeID := hex.EncodeToString(n1)
	i := 1
	addr := "192.168.2.1"
	extAddr := "10.0.0.1"
	for i <= count {
		clientID := uint32(i)
		err := c.setClient(userHash, nodeID, addr, extAddr, clientID)
		if err != nil {
			return err
		}
		i++
	}
	return nil
}

func TestSetRemoteClient(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 2); err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	clients := c.getClients(userHash)
	if len(clients) != 2 {
		t.Errorf("Expected: 1. Got: %d", len(clients))
	}
	if clients[0].clientID == clients[1].clientID {
		t.Errorf("Expected: different clientIDs. Got: the same")
	}
}

func TestDelRemoteClient(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 1); err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	clients := c.getClients(userHash)
	if len(clients) != 1 {
		t.Errorf("Expected: 1. Got: %d", len(clients))
	}
	if clients[0].clientID != 1 {
		t.Errorf("Expected: 1. Got: %d", clients[0].clientID)
	}

	err = c.delClient(userHash, clients[0].nodeID, clients[0].clientID)
	if err != nil {
		t.Errorf("Expected: nil. Got %s", err)
	}

	cls := c.getClients(userHash)
	if cls != nil {
		t.Errorf("Expected: nil. Got remote client data")
	}
}

func TestDelRemoteClientMulti(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 2); err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}

	clients := c.getClients(userHash)
	for _, client := range clients {
		if client.clientID != 1 {
			continue
		}
		err = c.delClient(userHash, client.nodeID, client.clientID)
		if err != nil {
			t.Errorf("Expected: nil, got %v", err)
		}

	}

	clients = c.getClients(userHash)
	if len(clients) != 1 {
		t.Errorf("Expected: 1, got: %d", len(clients))
	}
	if clients[0].clientID != 2 {
		t.Errorf("Expected: 2, got: %d", clients[0].clientID)
	}
}

func TestCheckRemoteClient(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 1); err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	clients := c.getClients(userHash)
	if ok := c.checkClient(userHash, clients[0].nodeID, clients[0].clientID); !ok {
		t.Errorf("Expected true. Got false")
	}
}

func TestUpdateLastAnnounce(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 1); err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	clients := c.getClients(userHash)
	t1 := clients[0].lastAnnounce

	time.Sleep(100 * time.Millisecond)
	c.updateLastAnnounce(userHash, clients[0].nodeID, clients[0].clientID)
	clients2 := c.getClients(userHash)
	t2 := clients2[0].lastAnnounce
	if t1 == t2 {
		t.Errorf("Expected: different values. Got the same value")
	}
}

func TestExpiredRemoteClient(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	c := d.remoteClients
	if err = setClient(c, 1); err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	clients := c.getClients(userHash)
	time.Sleep(1100 * time.Millisecond)
	for _, client := range clients {
		if ok := c.checkClient(userHash, client.nodeID, client.clientID); ok {
			t.Errorf("Expected false. Got true")
		}
	}
	c.Lock()
	if _, ok := c.cs[userHash]; ok {
		t.Errorf("Expected false. Got true")
	}
	c.Unlock()
}
