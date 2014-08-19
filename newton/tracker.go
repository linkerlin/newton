package newton

import (
	"fmt"
	"net"
	"time"

	"github.com/cstream/newton/comm"
	"github.com/cstream/newton/cstream"
)

// Listens a port for incoming UDP packets.
func (n *Newton) runUDPServer() {
	addr := net.UDPAddr{
		Port: n.Config.Server.TrackerPort,
		IP:   net.ParseIP("127.0.0.1"), // Temporary Hack
	}

	sock, err := net.ListenUDP("udp4", &addr)
	if err != nil {
		n.Log.Fatal("Tracker failed: %s", err.Error())
	}

	num := 0
	var buff [1024]byte // What about size of that array?
	n.Log.Info("Running tracker on port :%d", n.Config.Server.TrackerPort)
	for {
		sock.ReadFromUDP(buff[:])
		num++
		fmt.Println(string(buff[:]))
		fmt.Println(num)
	}
}

// Makes a query to find currently opened sessions for a particular user.
func (n *Newton) trackUser(usernames chan string, addr string) {
	conn, err := net.Dial("udp4", addr)
	if err != nil {
		n.Log.Error("Failed dial UDP: %s", err.Error())
		return
	}
L:
	for {
		select {
		case username := <-usernames:
			msg := &comm.TrackUser{
				Action:   cstream.TrackUser,
				Username: username,
			}
			buff, err := n.msgToByte(msg)
			ret, err := conn.Write(buff)

			if err != nil {
				n.Log.Warning("UDP write failed: %s", err.Error())
			}

			n.Log.Debug("Tracker: UDP write returned %d", ret)
			if err != nil {
				n.Log.Warning("Tracker: find query failed %s", err.Error())
			}
			time.Sleep(cstream.TrackerWriteInterval * time.Millisecond)
		case <-time.After(cstream.TrackUserThreshold * time.Millisecond):
			break L
		}
	}
}

func (n *Newton) runTracker() {
	tick := time.NewTicker(cstream.TrackerQueryInterval * time.Millisecond)
	defer tick.Stop()

	servers := []string{"127.0.0.1:9090", "127.0.0.1:5555"} // this is a temporary hack

	select {
	case <-tick.C:
		var chans = []chan string{}
		for _, server := range servers {
			ch := make(chan string, 1000)
			chans = append(chans, ch)
			go n.trackUser(ch, server)
		}

		for {
			select {
			case t := <-n.TrackUserQueries:
				for _, ch := range chans {
					ch <- t
				}
			}
		}
	}
}
