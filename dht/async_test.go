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

func TestCheckAsyncRequestWithoutClients(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	defer closeDHT(dd)

	userHash := "94f5f8dd64b297708de84ee2bec9d198987082f7"
	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	pAddr := "127.0.0.1:45623"
	if err := dd.addPeer(nID, pAddr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}

	dd.asyncStore.maxCheckTime = 3
	dd.asyncStore.expireInterval = 100 // in milliseconds
	addr := "127.0.0.1:5555"
	if err := dd.asyncLookupRequest(userHash, addr); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	dd.asyncStore.RLock()
	if dd.asyncStore.pq.Len() == 0 {
		t.Error("Expected 1, Got 0")
	}

	if counter, ok := dd.asyncStore.counter[userHash]; ok {
		if counter != 1 {
			t.Errorf("Expected 1, Got %d", counter)
		}
	} else {
		t.Error("Expected true, Got false")
	}
	if _, ok := dd.asyncStore.req[userHash]; !ok {
		t.Error("Expected true, Got false")
	}
	dd.asyncStore.RUnlock()

	time.Sleep(101 * time.Millisecond)
	if err := dd.checkAsyncRequest(); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	dd.asyncStore.RLock()
	if counter, ok := dd.asyncStore.counter[userHash]; ok {
		if counter != 2 {
			t.Errorf("Expected 2, Got %d", counter)
		}
	} else {
		t.Error("Expected true, Got false")
	}
	dd.asyncStore.RUnlock()

	// Exceed the limit by hand
	time.Sleep(101 * time.Millisecond)
	if err := dd.checkAsyncRequest(); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	time.Sleep(101 * time.Millisecond)
	if err := dd.checkAsyncRequest(); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	// Check it
	dd.asyncStore.RLock()
	if _, ok := dd.asyncStore.counter[userHash]; ok {
		t.Errorf("Expected false, Got true")
	}
	if _, ok := dd.asyncStore.req[userHash]; ok {
		t.Errorf("Expected false, Got true")
	}
	dd.asyncStore.RUnlock()

}

func TestCheckAsyncRequestWithClients(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	doneChan := make(chan struct{})
	defer func() {
		closeDHT(dd)
		close(doneChan)
	}()

	userHash := "94f5f8dd64b297708de84ee2bec9d198987082f7"
	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	pAddr := "127.0.0.1:45623"
	if err = dd.addPeer(nID, pAddr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	dd.asyncStore.maxCheckTime = 3
	dd.asyncStore.expireInterval = 100 // in milliseconds
	if err = dd.remoteClients.setClient(userHash, nID, "127.0.0.1:12312", testNewtonListen, 1); err != nil {
		t.Errorf("Expected nil. Got %v", err)
	}

	if err = dd.asyncLookupRequest(userHash, pAddr); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	ch := make(chan []byte, 2)
	if err = testUDPServer(pAddr, ch, doneChan); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	time.Sleep(101 * time.Millisecond)
	if err = dd.checkAsyncRequest(); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}

	msg := <-ch
	data, err := deserialize(msg)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	m := data.(LookupUserHashSuccessMsg)
	if m.Action != LookupUserHashSuccess {
		t.Errorf("Expected code %d, Got %d", LookupUserHashSuccess, m.Action)
	}
	if m.UserHash != userHash {
		t.Errorf("Expected UserHash: %s. Got: %s", userHash, m.UserHash)
	}
	if m.NodeID != dd.nodeID {
		t.Errorf("Expected NodeID: %s. Got: %s", dd.nodeID, m.NodeID)
	}
	if len(m.Clients) != 1 {
		t.Errorf("Expected 1. Got: %d", len(m.Clients))
	}
	client := m.Clients[0]
	if client.ClientID != 1 {
		t.Errorf("Expected ClientID 1. Got: %d", client.ClientID)
	}
	if client.NodeID != nID {
		t.Errorf("Expected NodeID: %s. Got: %s", dd.nodeID, m.NodeID)
	}
	if client.Addr != testNewtonListen {
		t.Errorf("Expected Addr: %s. Got: %s", testNewtonListen, client.Addr)
	}
}
