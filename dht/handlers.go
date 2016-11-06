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
	"fmt"
	"net"

	"github.com/purak/newton/log"
)

// handleNewPeer handles NewPeer message and generates a response message.
func (d *DistributedHashTable) handleNewPeer(msg NewPeerMsg, addr *net.UDPAddr) {
	if d.config.Identifier != msg.Identifier {
		log.Error("Code: ", NewPeer, " message received for a different DHT. Remote address ", addr.String())
		return
	}
	if msg.NodeID == d.nodeID {
		log.Debugf("That's my NodeID. Dropping message code: %d", msg.Action)
		return
	}
	if !SHA1Validation(msg.NodeID) {
		log.Debug("Invalid.nodeID. Code: ", NewPeer)
		return
	}

	pAddr := addr.String()
	now := makeTimestamp()
	err := d.addPeer(msg.NodeID, pAddr, now)
	if err == ErrDuplicateItem {
		log.Debugf("Duplicate NodeID: %s Address: %s", msg.NodeID, pAddr)
		err = nil
	}
	if err != nil {
		log.Error("Error while adding peer: ", err)
		return
	}

	log.Debug("Sending ", NewPeerSuccess, " to ", pAddr)
	m := NewPeerSuccessMsg{
		Action:     NewPeerSuccess,
		Identifier: d.config.Identifier,
		NodeID:     d.nodeID,
	}
	dt, _ := serialize(m)
	um := udpMessage{Data: dt, UDPAddr: addr}
	c, err := d.writeToUDP(um)
	if err != nil {
		log.Error("Error while writing message to the UDP socket: ", err)
	}
	log.Debug(c, " bytes have written to the UDP socket.")
}

// handleNewPeerSuccess handles NewPeerSuccess message, adds new peer to the routing table.
func (d *DistributedHashTable) handleNewPeerSuccess(msg NewPeerSuccessMsg, addr fmt.Stringer) {
	d.newPeerMap.RLock()
	defer d.newPeerMap.RUnlock()

	if d.config.Identifier != msg.Identifier {
		log.Error("Code: ", NewPeerSuccess, " message received for a different DHT. Remote address ", addr.String())
		return
	}
	if !SHA1Validation(msg.NodeID) {
		log.Debug("Invalid.nodeID. Code: ", NewPeerSuccess)
		return
	}
	uaddr := addr.String()
	if _, ok := d.newPeerMap.m[uaddr]; !ok {
		log.Error("Code: ", NewPeerSuccess, " message received from an unknown peer. Remote address ", uaddr)
		return
	}
	now := makeTimestamp()
	err := d.addPeer(msg.NodeID, uaddr, now)
	if err == ErrDuplicateItem {
		log.Debugf("Duplicate NodeID: %s Address: %s", msg.NodeID, uaddr)
		return
	}
	if err != nil {
		log.Errorf("Error while adding new peer with NodeID: %s, Addr: %s", msg.NodeID, uaddr)
	}
}

// handlePeerHeartbeat handles heartbeat messages between DHT instances.
func (d *DistributedHashTable) handlePeerHeartbeat(msg PeerHeartbeatMsg, addr fmt.Stringer) {
	now := makeTimestamp()
	paddr := addr.String()
	err := d.updateLastActivity(msg.NodeID, paddr, now)
	switch {
	case err == ErrPeerNotFound:
		if err = d.addPeer(msg.NodeID, paddr, now); err != nil {
			log.Errorf("Error while adding new peer with NodeID: %s, Addr: %s", msg.NodeID, paddr)
		}
	case err != nil:
		log.Error("Error while updating LastActivity field for NodeID: ", msg.NodeID, " Address: ", paddr, " Error: ", err)
	}
}

// handleAnnounceUserClientItem handles AnnounceUserClientItem message, adds a new to the remote clients map or
// updates the existed value.
func (d *DistributedHashTable) handleAnnounceUserClientItem(msg AnnounceClientItemMsg, addr fmt.Stringer) {
	if !SHA1Validation(msg.NodeID) {
		log.Warn("Invalid.nodeID from ", addr.String())
		return
	}

	if !SHA1Validation(msg.UserHash) {
		log.Warn("Invalid UserHash from ", addr.String())
		return
	}

	if ok := d.checkPeer(msg.NodeID); !ok {
		log.Warn("Unknown peer: ", msg.NodeID, " Address: ", addr.String(), " Code: ", AnnounceClientItem)
		return
	}
	// Create or update
	err := d.remoteClients.setClient(msg.UserHash, msg.NodeID, addr.String(), msg.Addr, msg.ClientID)
	if err != nil {
		if err == ErrDuplicateItem {
			d.remoteClients.updateLastAnnounce(msg.UserHash, msg.NodeID, msg.ClientID)
			return
		}
		log.Debug("Setting remote client is failed. ", err)
	}
}

func (d *DistributedHashTable) handleSeed(msg SeedMsg, addr *net.UDPAddr) {
	// FIXME: This is a public endpoint to extract peer addresses and it can be a bottleneck
	// for a newton node. We should use an authentication method or any restrictive method to
	// keep the nodes operational and prevent a possible DDoS attack.
	if ok := d.checkPeer(msg.NodeID); !ok {
		log.Warn("Seed message received from unknown peer. NodeID: ", msg.NodeID, " Addr: ", addr.String())
	}
	log.Info("Seed message received from: ", addr.String())
	pAddrs, err := d.getPeerAddrsRandomly()
	if err != nil {
		log.Error("Error while getting random peers from routing table: ", err)
		return
	}
	for _, pAddr := range pAddrs {
		msg := SeedSuccessMsg{
			Action: SeedSuccess,
			Addr:   pAddr,
		}
		dt, _ := serialize(msg)
		m := udpMessage{Data: dt, UDPAddr: addr}
		c, err := d.writeToUDP(m)
		if err != nil {
			log.Error("Error while writing message to the UDP socket: ", err)
		}
		log.Debug(c, " bytes have written to the UDP socket.")
	}
}

func (d *DistributedHashTable) handleSeedSuccess(msg SeedSuccessMsg, addr fmt.Stringer) {
	// FIXME: This is an public endpoint and it can be abused by attackers. So we should use
	// an authorization method or any restrictive method here to prevent a possible bottleneck or
	// DDoS attack.

	// TODO: Check network identifier here
	if ok := d.checkPeer(msg.NodeID); ok {
		log.Debug("SeedSuccess message received from ", addr.String(), " But I know this NodeID: ", msg.NodeID, " Addr: ", msg.Addr)
		return
	}
	log.Info("SeedSuccess message received from: ", addr.String())
	// Try to add this newton node as peer
	err := d.AddPeer(msg.Addr)
	if err != nil {
		log.Error("Error while processing Code: ", SeedSuccess, " Error: ", err)
	}

}

func (d *DistributedHashTable) handleLookupUserHash(msg LookupUserHashMsg, addr fmt.Stringer) {
	if ok := d.checkPeer(msg.NodeID); !ok {
		log.Warn("Code: ", LookupUserHash, " message received from unknown peer. NodeID: ", msg.NodeID, " Addr: ", addr.String())
		return
	}

	clients, err := d.FindClients(msg.UserHash)
	if err != nil {
		log.Errorf("Error while finding clients for UserHash: %s", msg.UserHash)
		return
	}

	if clients != nil {
		if err := d.sendLookupUserHashSuccess(msg.UserHash, clients, addr.String()); err != nil {
			log.Errorf("Error while sending message with code %d: %s", LookupUserHashSuccess, err)
		}
		return
	}

	if !d.config.Multicast.Enabled {
		// Use a priority queue to send responses to remote newton nodes.
		err := d.asyncLookupRequest(msg.UserHash, addr.String())
		if err != nil {
			log.Error("Error while running async lookup request. ", err)
		}
	}
}

func (d *DistributedHashTable) handleLookupUserHashSuccess(msg LookupUserHashSuccessMsg, addr fmt.Stringer) {
	if ok := d.checkPeer(msg.NodeID); !ok {
		log.Warn("Message from unknown peer Code: ", LookupUserHash, " Addr: ", addr.String())
		return
	}

	d.remoteUserHashListeners.Lock()
	if chans, ok := d.remoteUserHashListeners.c[msg.UserHash]; ok {
		for ch := range chans {
			ch <- msg.Clients
		}
		delete(d.remoteUserHashListeners.c, msg.UserHash)
	}
	d.remoteUserHashListeners.Unlock()

	for _, client := range msg.Clients {
		if ok := d.remoteClients.checkClient(msg.UserHash, client.NodeID, client.ClientID); ok {
			d.remoteClients.updateLastAnnounce(msg.UserHash, client.NodeID, client.ClientID)
			return
		}
		// For caching purpose.
		if err := d.remoteClients.setClient(msg.UserHash, client.NodeID, addr.String(), client.Addr, client.ClientID); err != nil {
			log.Debug("Setting remote client is failed. ", err)
		}

		if d.config.Multicast.Enabled {
			continue
		}
		// Add this node as a new peer. This is a good trick in unicast mode.
		if ok := d.checkPeer(client.NodeID); !ok {
			if err := d.AddPeer(client.Addr); err != nil {
				log.Errorf("Error while adding peer with addr %s: %s", client.Addr, err)
			}
		}
	}
}

func (d *DistributedHashTable) handleDeleteClient(msg DeleteClientMsg, addr *net.UDPAddr) {
	if err := d.remoteClients.delClient(msg.UserHash, msg.NodeID, msg.ClientID); err != nil {
		log.Debugf("Error while removing client UserHash: %s, NodeID: %s, ClientID: %s", msg.UserHash, msg.NodeID, msg.ClientID)
	}
}

func (d *DistributedHashTable) handleDeleteNode(msg DeleteNodeMsg, addr *net.UDPAddr) {
	if err := d.deletePeer(msg.NodeID); err != nil {
		log.Debugf("Error while deleting NodeID: %s from routing table: %s", msg.NodeID, err)
	}
}

func (d *DistributedHashTable) sendLookupUserHashSuccess(userHash string, clients []UserClientItem, addr string) error {
	// Send results for remote clients
	msg := LookupUserHashSuccessMsg{
		Action:   LookupUserHashSuccess,
		NodeID:   d.nodeID,
		UserHash: userHash,
		Clients:  clients,
	}
	data, err := serialize(msg)
	if err != nil {
		log.Error("Error while creating lookupUserHashSuccess message, ", err)
		return err
	}
	if err := d.sendMessage(data, addr); err != nil {
		log.Error("Error while sending LookupUserHashSuccess: ", err)
		return err
	}
	return nil
}
