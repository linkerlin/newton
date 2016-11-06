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
	"bytes"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/purak/newton/config"
)

var testDHTListen = "127.0.0.1:12345"
var testNewtonListen = "http://grid.newton.network:8776"

func setupDHT() (*DistributedHashTable, error) {
	cfg := &config.DHT{}
	cfg.Debug = false
	cfg.Unicast.DiscoveryInterval = "5s"
	cfg.Multicast.Enabled = false
	cfg.Unicast.Listen = testDHTListen
	cfg.PeerCheckInterval = 200
	cfg.InactivityThreshold = 400
	cfg.Identifier = "09643df7a07e9a06dacaf6c3e6bbee6b"
	cfg.DataDir = "/tmp/newtonTest"
	d, err := New(cfg, testNewtonListen)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := d.Start(); err != nil {
			log.Println("Error while starting DHT instance: ", err)
		}
	}()

	select {
	case <-d.listening:
	case <-time.After(2000 * time.Millisecond):
		return nil, errors.New("DHT instance cannot be initialized")
	}

	return d, nil
}

func closeDHT(d *DistributedHashTable) {
	d.Stop()
	<-d.StopChan
	if err := os.RemoveAll("/tmp/newtonTest"); err != nil {
		println("Error while removing /tmp/newtonTest folder")
	}
}

func TestAddPeerDHT(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	addr := "127.0.0.1:8989"
	err = dd.AddPeer(addr)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	defer closeDHT(dd)

	dd.newPeerMap.Lock()
	if _, ok := dd.newPeerMap.m[addr]; !ok {
		t.Errorf("Expected true. Got: %t", ok)
	}
	dd.newPeerMap.Unlock()
}

func TestNewPeerMapExpire(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	defer closeDHT(dd)
	dd.newPeerExpInt = 1
	addr := "127.0.0.1:8989"
	err = dd.AddPeer(addr)
	if err != nil {
		t.Errorf("Expected nil. Got: %v", err)
	}
	time.Sleep(2 * time.Second)
	dd.newPeerMap.Lock()
	if _, ok := dd.newPeerMap.m[addr]; ok {
		t.Errorf("Expected false. Got: %t", ok)
	}
	dd.newPeerMap.Unlock()
}

func testUDPServer(addr string, ch chan []byte, doneChan chan struct{}) error {
	uaddr, err := net.ResolveUDPAddr("udp4", addr)
	if err != nil {
		return err
	}

	buff := make([]byte, 1024)
	socket, err := net.ListenUDP("udp4", uaddr)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-doneChan:
				err := socket.Close()
				if err != nil {
					log.Println("Error while closing test socket: ", err)
				}
				return
			default:
				_, _, err := socket.ReadFromUDP(buff[:])
				if err != nil {
					log.Printf("Error while reading data from the UDP socket: %s", err)
				}
				ch <- bytes.Trim(buff[:], "\x00")
			}
		}
	}()
	return nil
}

func TestAddClientWithPeer(t *testing.T) {
	dd, err := setupDHT()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	doneChan := make(chan struct{})
	defer func() {
		close(doneChan)
		closeDHT(dd)
	}()
	userHash := "94f5f8dd64b297708de84ee2bec9d198987082f7"
	clientID := uint32(1)

	nodeID, err := RandNodeID()
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	nID := hex.EncodeToString(nodeID)
	addr := "127.0.0.1:5625"
	ch := make(chan []byte, 2)

	if err = testUDPServer(addr, ch, doneChan); err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}

	if err = dd.addPeer(nID, addr, time.Now().Unix()); err != nil {
		t.Errorf("Expected nil. Got %s", err)
	}
	err = dd.AddClient(userHash, clientID)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	msg := <-ch
	data, err := deserialize(msg)
	m := data.(AnnounceClientItemMsg)
	if err != nil {
		t.Errorf("Expected nil. Got: %s", err)
	}
	if m.Action != AnnounceClientItem {
		t.Errorf("Expected Action: %d. Got: %d", AnnounceClientItem, m.Action)
	}
	if m.UserHash != userHash {
		t.Errorf("Expected UserHash: %s. Got: %s", userHash, m.UserHash)
	}
	if m.ClientID != 1 {
		t.Errorf("Expected ClientID 1. Got: %d", m.ClientID)
	}
	if m.NodeID != dd.nodeID {
		t.Errorf("Expected NodeID: %s. Got: %s", dd.nodeID, m.NodeID)
	}
	if m.Addr != testNewtonListen {
		t.Errorf("Expected Addr: %s. Got: %s", testNewtonListen, m.Addr)
	}

}
