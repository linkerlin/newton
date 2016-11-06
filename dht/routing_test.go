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
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestAddPeer(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(d)
	nodeID := "7608d0137de579067ace5f739c5001e204861300"
	addr := "192.186.23.22:8888"
	now := makeTimestamp()
	if err = d.addPeer(nodeID, addr, now); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	item := d.getPeer(nodeID)
	if nodeID != item.nodeID {
		t.Errorf("Expected: %s. Got: %s", nodeID, item.nodeID)
	}
	if addr != item.addr {
		t.Errorf("Expected: %s. Got: %s", addr, item.addr)
	}
	if now != item.lastActivity {
		t.Errorf("Expected: %d. Got: %d", now, item.lastActivity)
	}

}

func TestDeletePeer(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(d)

	nodeID := "7608d0137de579067ace5f739c5001e204861300"
	addr := "192.186.23.22:8888"
	now := makeTimestamp()
	if err = d.addPeer(nodeID, addr, now); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	if err = d.deletePeer(nodeID); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	p := d.getPeer(nodeID)
	if p != nil {
		t.Errorf("Expected nil. Got NodeID: %s", p.nodeID)
	}
}

func TestUpdateLastActivity(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(d)

	nodeID := "7608d0137de579067ace5f739c5001e204861300"
	addrOld := "192.186.23.22:8888"
	nowOld := makeTimestamp()
	if err = d.addPeer(nodeID, addrOld, nowOld); err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	addr := "292.186.23.22:5555"
	now := makeTimestamp() + 10000
	if err = d.updateLastActivity(nodeID, addr, now); err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}

	item := d.getPeer(nodeID)
	if item == nil {
		t.Errorf("Expected peer item. Got nil.")
	}
	if now != item.lastActivity {
		t.Errorf("Expected: %d. Got: %d", now, item.lastActivity)
	}
}

func TestFindClosestPeers(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(d)

	userHash := "f90d3bd595a9a8f23c05c7a81afb4d1b09410d80"
	nodeID1 := "7608d0137de579067ace5f739c5001e204861300"
	addr1 := "192.186.23.22:8888"
	nodeID2 := "0aaf8a6c0527c0c48a1a1252ae6c4cb4dc837b29"
	addr2 := "292.186.23.22:5555"
	nodeID3 := "f90d3bd595a9a8f23c05c7a81afb4d1b09410d80"
	addr3 := "122.186.23.22:8588"

	now := makeTimestamp()

	err = d.addPeer(nodeID1, addr1, now)
	if err != nil {
		t.Errorf("Expected nil. Got; %s", err)
	}
	err = d.addPeer(nodeID2, addr2, now)
	if err != nil {
		t.Errorf("Expected nil. Got; %s", err)
	}
	err = d.addPeer(nodeID3, addr3, now)
	if err != nil {
		t.Errorf("Expected nil. Got; %s", err)
	}

	candidates, err := d.findClosestPeers(userHash)
	if err != nil {
		t.Errorf("Expected nil. Got; %s", err)
	}
	if len(candidates) != 3 {
		t.Errorf("Expected 3, Got: %d", len(candidates))
	}
	candidate := candidates[0]
	if candidate.addr != addr3 {
		t.Errorf("Expected %s, Got: %s", addr3, candidate.addr)
	}

	if candidate.nodeID != nodeID3 {
		t.Errorf("Expected %s, Got: %s", nodeID3, candidate.nodeID)
	}
}

func TestPeerHeartbeat(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer func() {
		closeDHT(d)
		if err := d.serverErrGr.Wait(); err != nil {
			t.Errorf("Expected nil. Got: %v", err)
		}
	}()

	d.serverErrGr.Go(func() error {
		return d.stalePeerChecker()
	})

	nodeID := "7608d0137de579067ace5f739c5001e204861300"
	addr := "192.186.23.22:8888"
	now := makeTimestamp() - 35000
	if err = d.addPeer(nodeID, addr, now); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	time.Sleep(1100 * time.Millisecond)
	peer := d.getPeer(nodeID)
	if peer != nil {
		t.Errorf("Expected nil. Got peer: %s", peer.nodeID)
	}
}

func TestCheckPeer(t *testing.T) {
	d, err := setupDHT()
	if err != nil {
		t.Fatalf("Expected nil. Got: %v", err)
	}
	defer closeDHT(d)

	nodeID := "7608d0137de579067ace5f739c5001e204861300"
	addr := "192.186.23.22:8888"
	now := makeTimestamp() - 35000
	if err = d.addPeer(nodeID, addr, now); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	ok := d.checkPeer("foobar")
	if ok {
		t.Error("Expected false. Got true")
	}

	ok = d.checkPeer(nodeID)
	if !ok {
		t.Errorf("Expected true. Got false")
	}
}

func insertRandomNodes(count int, now int64) (*DistributedHashTable, error) {
	d, err := setupDHT()
	if err != nil {
		return nil, err
	}

	i := 0
	host := "192.168.2.1"
	for i <= count {
		i++
		n1, err := RandNodeID()
		if err != nil {
			return nil, err

		}
		nodeID := hex.EncodeToString(n1)
		addr := host + ":" + strconv.Itoa(i)
		if err := d.addPeer(nodeID, addr, now); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func TestGetPeerAddrsRandomly(t *testing.T) {
	now := makeTimestamp()
	d, err := insertRandomNodes(50, now)
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	peers1, err := d.getPeerAddrsRandomly()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}

	peers2, err := d.getPeerAddrsRandomly()
	if err != nil {
		t.Fatalf("Expected nil. Got: %s", err)
	}
	if strings.Join(peers1, "") == strings.Join(peers2, "") {
		t.Errorf("Expected different values. Got the same.")
	}
}

func TestSetupPeers(t *testing.T) {
	now := makeTimestamp()
	d, err := insertRandomNodes(50, now)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	defer closeDHT(d)
	addrs, err := d.setupPeers()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	if len(addrs) != 0 {
		t.Errorf("Expected empty list. It's not empty")
	}
}
