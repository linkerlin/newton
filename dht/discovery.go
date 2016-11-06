package dht

import (
	"errors"
	"strings"
	"time"

	"github.com/purak/newton/log"
)

func (d *DistributedHashTable) startUnicastDiscovery() error {
	select {
	case <-d.listening:
	case <-time.After(5000 * time.Millisecond):
		return errors.New("no UDP socket available")
	}

	// Set peers from local storage, old peers.
	oldAddrs, err := d.setupPeers()
	if err != nil {
		log.Error("Error while setting old peers to the in-memory database.")
		return err
	}
	// After a long outage, we should add them from scratch.
	if len(oldAddrs) != 0 {
		for _, addr := range oldAddrs {
			if err := d.AddPeer(addr); err != nil {
				log.Errorf("Error while setting old peer %s to the in-memory database.", addr)
				return err
			}
		}
	}

	// Set peers from the configuration file or command line.
	for _, addr := range d.config.Unicast.Peers {
		addr = strings.Trim(addr, " ")
		if err := d.AddPeer(addr); err != nil {
			log.Errorf("Error while adding peer with address: %s: %s", addr, err)
			return err
		}
	}

	d.waitGroup.Add(1)
	go d.sendSeedMessagePeriodically()

	d.waitGroup.Add(1)
	go d.addStaticPeersPeriodically()

	return nil
}

func (d *DistributedHashTable) addStaticPeersPeriodically() {
	defer d.waitGroup.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, addr := range d.config.Unicast.Peers {
				addr = strings.Trim(addr, " ")
				if err := d.AddPeer(addr); err != nil {
					log.Errorf("Error while adding peer with address: %s: %s", addr, err)
				}
			}
		case <-d.done:
			return
		}
	}
}

func (d *DistributedHashTable) sendSeedMessagePeriodically() {
	defer d.waitGroup.Done()

	ticker := time.NewTicker(d.unicastDiscoveryInterval)
	defer ticker.Stop()
	// Ask for more peer periodically.
	for {
		select {
		case <-ticker.C:
			err := d.sendSeedMessage()
			if err != nil {
				log.Warn("Error while finding new peers: ", err)
			}
		case <-d.done:
			return
		}
	}
}

// sendSeedMessage selects maximum 8 different peer from the routing
// table and send "seed" message to find more peer.
func (d *DistributedHashTable) sendSeedMessage() error {
	log.Info("Trying to find more peer")
	peers, err := d.getPeerAddrsRandomly()
	if err != nil {
		return err
	}
	msg := SeedMsg{
		Action: Seed,
		NodeID: d.nodeID,
	}
	data, err := serialize(msg)
	if err != nil {
		return err
	}
	for _, addr := range peers {
		log.Debugf("Sending seed message to %s", addr)
		err := d.sendMessage(data, addr)
		if err != nil {
			log.Debug("Error while sending SeedMessage to %s: %s", addr, err)
			continue
		}
	}
	return nil
}

func (d *DistributedHashTable) startMulticastDiscovery() error {
	select {
	case <-d.listening:
	case <-time.After(5000 * time.Millisecond):
		return errors.New("no UDP socket available")
	}
	// Send heartbeat message via UDP tracker
	msg := PeerHeartbeatMsg{
		Action: PeerHeartbeat,
		NodeID: d.nodeID,
	}
	data, err := serialize(msg)
	if err != nil {
		return err
	}
	d.waitGroup.Add(1)
	go d.sendMulticastHeartbeat(data)
	return nil
}

func (d *DistributedHashTable) sendMulticastHeartbeat(data []byte) error {
	defer d.waitGroup.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	log.Info("Start sending multicast heartbeat messages to ", d.config.Listen)
	for {
		select {
		case <-ticker.C:
			if err := d.sendMessage(data, d.config.Listen); err != nil {
				log.Error("Error while sending heartbeat message through multicast enabled UDP socket: ", err)
				continue
			}
		case <-d.done:
			return nil
		}
	}
}
