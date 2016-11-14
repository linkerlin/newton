package partition

import (
	"bytes"
	"encoding/binary"
	"net"

	"github.com/purak/newton/log"
)

const maxDatagramSize = 1024

// udpMessage is a struct that carries raw data to provide communication DHT instances.
type udpMessage struct {
	Data    []byte
	UDPAddr *net.UDPAddr
}

// Listens a port for incoming UDP packets.
func (p *Partition) listenUnicastUDP() error {
	addr, err := net.ResolveUDPAddr("udp", p.config.Listen)
	if err != nil {
		return err
	}

	// Listen a port for UDP traffic. We are going to use that port for
	// incoming and outgoing packages.
	p.unicastSocket, err = net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	if err := p.unicastSocket.SetReadBuffer(maxDatagramSize); err != nil {
		return err
	}

	go p.readFromUnicastUDP()
	return nil
}

func (p *Partition) readFromUnicastUDP() {
	// We need this function to close a healthy UDP server.
	p.serverErrGr.Go(func() error {
		<-p.closeUDPChan

		log.Info("Stop listening UDP on ", p.config.Listen)
		err := p.unicastSocket.Close()
		if err != nil {
			log.Errorf("Error while closing unicast UDP socket: %v", err)
		}
		return err
	})

	log.Info("Listening unicast UDP on ", p.config.Listen)
	p.startListening()
	for {
		buf := make([]byte, maxDatagramSize)
		nr, sAddr, err := p.unicastSocket.ReadFromUDP(buf)
		if err != nil {
			if _, ok := <-p.closeUDPChan; !ok {
				// Gracefuly shutdown
				break
			}
			log.Error("Error while reading packages from UDP ", p.config.Listen, ": ", err)
			continue
		}

		ip := sAddr.String()
		if nr == 0 {
			log.Errorf("Empty package has been received from IP: %s", ip)
			continue
		}

		data := buf[:nr]
		if data[0] == heartbeatMessageFlag {
			p.waitGroup.Add(1)
			go p.processHeartbeat(ip)
		} else if data[0] == joinMessageFlag {
			p.waitGroup.Add(1)
			go p.processJoin(data, ip)
		}
	}
}

func (p *Partition) processJoin(data []byte, ip string) {
	defer p.waitGroup.Done()

	var birthdate int64
	b := bytes.NewBuffer(data[1:9])
	binary.Read(b, binary.LittleEndian, &birthdate)
	addr := string(data[9:])

	log.Infof("Join message received from IP: %s, address: %s", ip, addr)
	err := p.addMember(addr, ip, birthdate)
	if err == errMemberAlreadyExist {
		if uErr := p.updateMember(addr); uErr != nil {
			log.Errorf("Error while adding member IP: %s, address: %s: %s", ip, addr, err)
		}
		return
	}
	if err != nil {
		log.Errorf("Error while adding member IP: %s, address: %s: %s", addr, err)
	}
	select {
	case <-p.nodeInitialized:
		p.waitGroup.Add(1)
		go p.backgroundJoin(addr)
	default:
	}
}

func (p *Partition) processHeartbeat(ip string) {
	defer p.waitGroup.Done()

	select {
	case <-p.nodeInitialized:
		log.Debugf("Heartbeat message received from %s", ip)
		if uErr := p.updateMemberByIP(ip); uErr != nil {
			log.Errorf("Error while processing heartbeat from %s: %s", ip, uErr)
		}
	default:
	}
}

func (p *Partition) backgroundJoin(addr string) {
	defer p.waitGroup.Done()

	mAddr := p.getCoordinatorMemberFromPartitionTable()
	if mAddr != p.config.Address {
		// You're not the master node.
		return
	}
	if err := p.joinCluster(addr); err != nil {
		// TODO: We should re-try this.
		log.Errorf("Error while adding a new member to cluster: %s", err)
		return
	}

}
func (p *Partition) sendMessage(data []byte, addr string) error {
	uaddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return err
	}
	m := udpMessage{Data: data, UDPAddr: uaddr}
	c, err := p.writeToUDP(m)
	if err != nil {
		return err
	}
	log.Debugf("The message has been written to the UDP socket. Byte count: %d", c)
	return nil
}

// writeToUDP writes a byte slice to the UDP socket.
func (p *Partition) writeToUDP(msg udpMessage) (int, error) {
	return p.unicastSocket.WriteToUDP(msg.Data, msg.UDPAddr)
}

func (p *Partition) startListening() {
	select {
	case <-p.listening:
		return
	default:
	}
	close(p.listening)
	return
}
