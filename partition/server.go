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
	addr, err := net.ResolveUDPAddr("udp", p.config.Unicast.Listen)
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

		log.Info("Stop listening UDP on ", p.config.Unicast.Listen)
		err := p.unicastSocket.Close()
		if err != nil {
			log.Errorf("Error while closing unicast UDP socket: %v", err)
		}
		return err
	})

	log.Info("Listening unicast UDP on ", p.config.Unicast.Listen)
	p.startListening()
	for {
		buf := make([]byte, maxDatagramSize)
		nr, sAddr, err := p.unicastSocket.ReadFromUDP(buf)
		if err != nil {
			if _, ok := <-p.closeUDPChan; !ok {
				// Gracefuly shutdown
				break
			}
			log.Error("Error while reading packages from UDP ", p.config.Unicast.Listen, ": ", err)
			continue
		}

		addr := sAddr.String()
		if nr == 0 {
			if err := p.deleteMember(addr); err != nil {
				log.Errorf("Error while deleting member %s: %s", addr, err)
			}
			continue
		}
		var birthdate int64
		data := buf[:nr]
		b := bytes.NewBuffer(data)
		binary.Read(b, binary.LittleEndian, &birthdate)

		err = p.addMember(addr, birthdate)
		if err == errMemberAlreadyExist {
			if uErr := p.updateMember(addr); uErr != nil {
				log.Errorf("Error while adding member: %s: %s", addr, err)
			}
			continue
		}
		if err != nil {
			log.Errorf("Error while adding member: %s: %s", addr, err)
		}
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
