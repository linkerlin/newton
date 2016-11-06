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
	"net"

	"github.com/purak/newton/log"

	"golang.org/x/net/ipv4"
)

var maxDatagramSize = 1024

// udpMessage is a struct that carries raw data to provide communication DHT instances.
type udpMessage struct {
	Data    []byte
	UDPAddr *net.UDPAddr
}

func (d *DistributedHashTable) listenMulticastUDP() error {
	addr, err := net.ResolveUDPAddr("udp", d.config.Multicast.Address)
	if err != nil {
		return err
	}

	ift, err := net.InterfaceByName(d.config.Multicast.Interface)
	if err != nil {
		return err
	}
	c, err := net.ListenPacket("udp", d.config.Multicast.Address)
	if err != nil {
		return err
	}
	d.multicastSocket = ipv4.NewPacketConn(c)
	if err = d.multicastSocket.JoinGroup(ift, addr); err != nil {
		log.Error("Error while joining multicast group: ", err)
		return err
	}

	if err = d.multicastSocket.SetMulticastLoopback(false); err != nil {
		log.Error("Error while disabling multicast loopback: ", err)
		return err
	}
	go d.readFromMulticastUDP()
	return nil
}

func (d *DistributedHashTable) readFromMulticastUDP() {
	addr := d.config.Listen
	// We need this function to close a healthy UDP server.
	d.serverErrGr.Go(func() error {
		<-d.closeUDPChan

		log.Info("Stop listening multicast enabled UDP socket on ", addr)
		err := d.multicastSocket.Close()
		if err != nil {
			log.Error("Error while closing multicast UDP socket: ", err)
		}
		return err
	})

	log.Info("Listening multicast UDP on ", d.config.Multicast.Address)
	d.startListening()
	for {
		buf := make([]byte, maxDatagramSize)
		nr, _, src, err := d.multicastSocket.ReadFrom(buf)
		if err != nil {
			if _, ok := <-d.closeUDPChan; !ok {
				// Gracefuly shutdown
				break
			}
			log.Error("Error while reading packages from UDP ", addr, ": ", err)
			continue
		}
		sAddr, err := net.ResolveUDPAddr("udp", src.String())
		if err != nil {
			log.Error("Error while resolving source address: ", err)
			continue
		}
		d.requestWG.Add(1)
		go d.dispatchUDPMessages(buf[:nr], sAddr)
	}
}

// Listens a port for incoming UDP packets.
func (d *DistributedHashTable) listenUnicastUDP() error {
	addr, err := net.ResolveUDPAddr("udp", d.config.Unicast.Listen)
	if err != nil {
		return err
	}

	// Listen a port for UDP traffic. We are going to use that port for
	// incoming and outgoing packages.
	d.unicastSocket, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	if err := d.unicastSocket.SetReadBuffer(maxDatagramSize); err != nil {
		return err
	}

	go d.readFromUnicastUDP()
	return nil
}

func (d *DistributedHashTable) readFromUnicastUDP() {
	// We need this function to close a healthy UDP server.
	d.serverErrGr.Go(func() error {
		<-d.closeUDPChan

		log.Info("Stop listening UDP on ", d.config.Unicast.Listen)
		err := d.unicastSocket.Close()
		if err != nil {
			log.Errorf("Error while closing unicast UDP socket: %v", err)
		}
		return err
	})

	log.Info("Listening unicast UDP on ", d.config.Unicast.Listen)
	d.startListening()
	for {
		buf := make([]byte, maxDatagramSize)
		nr, sAddr, err := d.unicastSocket.ReadFromUDP(buf)
		if err != nil {
			if _, ok := <-d.closeUDPChan; !ok {
				// Gracefuly shutdown
				break
			}
			log.Error("Error while reading packages from UDP ", d.config.Unicast.Listen, ": ", err)
			continue
		}
		d.requestWG.Add(1)
		go d.dispatchUDPMessages(buf[:nr], sAddr)
	}
}

// dispatchUDPMessages parses and evaluates incoming messages from the UDP port.
func (d *DistributedHashTable) dispatchUDPMessages(buff []byte, sAddr *net.UDPAddr) {
	defer d.requestWG.Done()
	message, err := deserialize(buff)
	if err != nil {
		log.Errorf("Invalid message from %s: %s", sAddr.String(), err)
		return
	}

	switch message.(type) {
	case PeerHeartbeatMsg:
		msg := message.(PeerHeartbeatMsg)
		d.handlePeerHeartbeat(msg, sAddr)
	case SeedMsg:
		msg := message.(SeedMsg)
		d.handleSeed(msg, sAddr)
	case NewPeerMsg:
		msg := message.(NewPeerMsg)
		d.handleNewPeer(msg, sAddr)
	case NewPeerSuccessMsg:
		msg := message.(NewPeerSuccessMsg)
		d.handleNewPeerSuccess(msg, sAddr)
	case SeedSuccessMsg:
		msg := message.(SeedSuccessMsg)
		d.handleSeedSuccess(msg, sAddr)
	case LookupUserHashMsg:
		msg := message.(LookupUserHashMsg)
		d.handleLookupUserHash(msg, sAddr)
	case AnnounceClientItemMsg:
		msg := message.(AnnounceClientItemMsg)
		d.handleAnnounceUserClientItem(msg, sAddr)
	case LookupUserHashSuccessMsg:
		msg := message.(LookupUserHashSuccessMsg)
		d.handleLookupUserHashSuccess(msg, sAddr)
	case DeleteClientMsg:
		msg := message.(DeleteClientMsg)
		d.handleDeleteClient(msg, sAddr)
	case DeleteNodeMsg:
		msg := message.(DeleteNodeMsg)
		d.handleDeleteNode(msg, sAddr)
	}
}

func (d *DistributedHashTable) sendMessage(data []byte, addr string) error {
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	m := udpMessage{Data: data, UDPAddr: uaddr}
	c, err := d.writeToUDP(m)
	if err != nil {
		return err
	}
	log.Debugf("The message has been written to the UDP socket. Byte count: %d", c)
	return nil
}

// writeToUDP writes a byte slice to the UDP socket.
func (d *DistributedHashTable) writeToUDP(msg udpMessage) (int, error) {
	if d.config.Multicast.Enabled {
		return d.multicastSocket.WriteTo(msg.Data, nil, msg.UDPAddr)
	}
	return d.unicastSocket.WriteToUDP(msg.Data, msg.UDPAddr)
}
