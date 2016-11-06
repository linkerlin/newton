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
	"net"
	"testing"
)

func TestNewPeer(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(dd)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	np := NewPeerMsg{
		Action:     NewPeer,
		Identifier: dd.config.Identifier,
		NodeID:     nID,
	}

	doneChan := make(chan struct{})
	addr := "127.0.0.1:5623"
	ch := make(chan []byte, 2)
	err = testUDPServer(addr, ch, doneChan)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	dd.handleNewPeer(np, uaddr)
	c, err := dd.findClosestPeers(nID)
	if err != nil {
		t.Errorf("Expected 1, Got %d", len(c))
	}
	if len(c) != 1 {
		t.Errorf("Expected 1, Got %d", len(c))
	}
	msg := <-ch
	m, err := deserialize(msg)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	resp := m.(NewPeerSuccessMsg)
	if resp.Action != NewPeerSuccess {
		t.Errorf("Expected: %d, Got %d", NewPeerSuccess, resp.Action)
	}
	if resp.Identifier != np.Identifier {
		t.Errorf("Expected: %s, Got %s", np.Identifier, resp.Identifier)
	}
	if resp.NodeID != dd.nodeID {
		t.Errorf("Expected: %s, Got %s", dd.nodeID, resp.NodeID)
	}
}

func TestNewPeerSuccess(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(dd)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	np := NewPeerSuccessMsg{
		Action:     NewPeerSuccess,
		Identifier: dd.config.Identifier,
		NodeID:     nID,
	}
	addr := "127.0.0.1:23523"
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	dd.newPeerMap.Lock()
	dd.newPeerMap.m[addr] = true
	dd.newPeerMap.Unlock()

	dd.handleNewPeerSuccess(np, uaddr)
	c, err := dd.findClosestPeers(nID)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	if len(c) != 1 {
		t.Errorf("Expected 1. Got: %d", len(c))
	}
}

func TestAnnounceUserClientItem(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(dd)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	addr := "127.0.0.1:23523"
	if err = dd.addPeer(nID, addr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}

	userHash := "94f5f8dd64b297708de84ee2bec9d198987082f7"
	clientID := uint32(1)
	au := AnnounceClientItemMsg{
		Action:   AnnounceClientItem,
		UserHash: userHash,
		ClientID: clientID,
		NodeID:   nID,
	}
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	dd.handleAnnounceUserClientItem(au, uaddr)
	if ok := dd.remoteClients.checkClient(userHash, nID, clientID); !ok {
		t.Errorf("Expected true. Got false")
	}
}

func TestLookupUserHashWithoutClients(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(dd)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	addr := "127.0.0.1:23522"
	if err = dd.addPeer(nID, addr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	l := LookupUserHashMsg{
		Action:   LookupUserHash,
		NodeID:   nID,
		UserHash: "94f5f8dd64b297708de84ee2bec9d198987082f7",
	}

	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	dd.handleLookupUserHash(l, uaddr)
	dd.asyncStore.RLock()
	if dd.asyncStore.pq.Len() != 1 {
		t.Errorf("Expected 1, Got %d", dd.asyncStore.pq.Len())
	}
	dd.asyncStore.RUnlock()
}

func TestLookupUserHashWithClients(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	doneChan := make(chan struct{})
	defer func() {
		close(doneChan)
		closeDHT(dd)
	}()
	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)

	addr := "127.0.0.1:6629"
	ch := make(chan []byte, 2)
	err = testUDPServer(addr, ch, doneChan)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	clientID := uint32(1)
	l := LookupUserHashMsg{
		Action:   LookupUserHash,
		NodeID:   nID,
		UserHash: "94f5f8dd64b297708de84ee2bec9d198987082f7",
	}
	if err = dd.addPeer(nID, addr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}

	rNodeID := "5b93ca464300ddcf4bb55d7e3c88f32dc90a713c"
	if err = dd.remoteClients.setClient(l.UserHash, rNodeID, "127.0.0.1:2315", testNewtonListen, clientID); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}

	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	dd.handleLookupUserHash(l, uaddr)

	if dd.asyncStore.pq.Len() != 0 {
		t.Errorf("Expected 0, Got %s", dd.asyncStore.pq.Len())
	}
	msg := <-ch
	m, err := deserialize(msg)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}
	resp := m.(LookupUserHashSuccessMsg)
	if resp.UserHash != l.UserHash {
		t.Errorf("Expected %s, Got %s", l.UserHash, resp.UserHash)
	}
	if resp.Action != LookupUserHashSuccess {
		t.Errorf("Expected: %d, Got %d", NewPeerSuccess, resp.Action)
	}
	if resp.NodeID != dd.nodeID {
		t.Errorf("Expected: %s, Got %s", dd.nodeID, resp.NodeID)
	}
	if len(resp.Clients) != 1 {
		t.Fatalf("Expected: 1, Got %d", len(resp.Clients))
	}
	client := resp.Clients[0]
	if client.NodeID != rNodeID {
		t.Errorf("Expected: %s, Got %s", client.NodeID, rNodeID)
	}
	if client.ClientID != clientID {
		t.Errorf("Expected: %d, Got %d", client.ClientID, clientID)
	}
	if client.Addr != testNewtonListen {
		t.Errorf("Expected: %s, Got %s", client.Addr, testNewtonListen)
	}
}

func TestLookupUserHashSuccess(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(dd)

	userHash := "94f5f8dd64b297708de84ee2bec9d198987082f7"
	clientID := uint32(1)
	remoteNodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	rnID := hex.EncodeToString(remoteNodeID)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	nID := hex.EncodeToString(nodeID)

	cl := UserClientItem{
		NodeID:   rnID,
		ClientID: clientID,
		Addr:     "127.0.0.1:34617",
	}
	clients := []UserClientItem{cl}
	ls := LookupUserHashSuccessMsg{
		Action:   LookupUserHashSuccess,
		NodeID:   nID,
		UserHash: userHash,
		Clients:  clients,
	}

	addr := "127.0.0.1:23523"
	if err = dd.addPeer(nID, addr, makeTimestamp()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		t.Errorf("Expected nil, Got %s", err)
	}

	dd.handleLookupUserHashSuccess(ls, uaddr)
	if ok := dd.remoteClients.checkClient(userHash, rnID, clientID); !ok {
		t.Errorf("Expected true. Got false")
	}
}
