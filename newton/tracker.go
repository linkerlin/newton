package newton

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cstream/newton/comm"
	"github.com/cstream/newton/cstream"
)

type TrackerEvent interface {
}

type AddUsernameEvent struct {
	Username   string
	ClientItem *ClientItem
}

type OnlineUserEvent struct {
	Username string
	Clients  []RouteClientItem
}

type UpdateUsernameEvent struct {
	Username string
	ClientID string
	Location string
}

type RouteItem struct {
	ClientItems []RouteClientItem
	Subscribers []*ClientItem
	Processed   bool
	ExpireAt    int64
}

type RouteClientItem struct {
	ClientID string
	Location string
}

type RoutingTable struct {
	sync.RWMutex
	r map[string]*RouteItem
}

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
			case t := <-n.TrackerQueue:
				for _, ch := range chans {
					ch <- t
				}
			}
		}
	}
}

// Rename this
func (n *Newton) routingLoop() {
	for {
		select {
		case event := <-n.RoutingQueue:
			if event == nil {
				continue
			}
			switch event.(type) {
			case *AddUsernameEvent:
				// An event from users
				n.handleAddUser(event.(*AddUsernameEvent))
			case *UpdateUsernameEvent:
				// An event from other newton instances
				n.handleUpdateUser(event.(*UpdateUsernameEvent))
			}
		}
	}
}

func (n *Newton) handleAddUser(event *AddUsernameEvent) {
	username := (*event).Username
	c := (*event).ClientItem
	r, ok := n.RoutingTable.r[username]
	if ok {
		// Add this user to the subscribers slice if required.
		n.addSubscriber(r, c)

		// We need "processed" switch to prevent duplicate items in the query channel.
		if !r.Processed && len(r.ClientItems) == 0 {
			// Don't send that request to the queue because it's already inside the queue.
			return
		}
		// We know the location of that user
		if r.Processed && len(r.ClientItems) != 0 {
			// Run a function from here to pass the availability information
			n.notifyClientItems(username, r)
		}
		// At that point, we may have the location information for that user's clients
		// But we should make a look-up query to update the info or find new clients for
		// subscribers. So we set 'false' to Processed field of the RoutingItem and
		// send it to the queue again.
		r.Processed = false
	} else {
		// FIXME: This looks like a piece of shit. Think about linked list or map for this job.
		var s []*ClientItem
		s = append(s, (*event).ClientItem)
		r = &RouteItem{
			ExpireAt:    time.Now().Unix() + cstream.RouteItemExpireInterval,
			Processed:   false,
			Subscribers: s,
		}
	}
	n.RoutingTable.r[username] = r
	n.TrackerQueue <- username
}

func (n *Newton) handleUpdateUser(e *UpdateUsernameEvent) {}

func (n *Newton) notifyClientItems(username string, r *RouteItem) {
	for _, s := range r.Subscribers {
		c := OnlineUserEvent{
			Username: username,
			Clients:  r.ClientItems,
		}
		s.TrackerEvents <- c
	}
}

func (n *Newton) addSubscriber(r *RouteItem, c *ClientItem) {
	existed := false
	for _, s := range r.Subscribers {
		if s == c {
			existed = true
		}
	}
	if existed {
		r.Subscribers = append(r.Subscribers, c)
	}
}
