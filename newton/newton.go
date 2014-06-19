package newton

import (
	"container/heap"
	"fmt"
	"github.com/purak/gauss/dhash" // Main datastore application
	"sync"
	//"github.com/purak/gauss/gconn" // Client library for Gauss
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/store"
	"net"
	"time"
)

const ReleaseVersion = "0.0.1"

// Newton instance struct
type Newton struct {
	Config        *config.Config
	Log           cstream.Logger
	SetLogLevel   func(cstream.Level)
	ActiveClients *store.PriorityQueue
	ConnTable     *ConnTable
}

type ConnTable struct {
	sync.RWMutex // To protect maps
	m            map[string]*ClientItem
}

// A container structure for active clients
type ClientItem struct {
	Ip           string
	LastAnnounce int64
}

// Create a new Newton instance
func New(c *config.Config) *Newton {
	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// Create a new logger
	l, setlevel := cstream.NewLogger("newton.server")

	// Create a priority queue to hold active client connections
	pq := &store.PriorityQueue{}
	heap.Init(pq)

	// Connection table
	ct := &ConnTable{m: make(map[string]*ClientItem)}

	return &Newton{
		Config:        c,
		Log:           l,
		SetLogLevel:   setlevel,
		ActiveClients: pq,
		ConnTable:     ct,
	}
}

// Extend expire time for active clients.
func (n *Newton) rescheduleClientTimeout(clientId string, ci *ClientItem) bool {
	secondsAgo := time.Now().Unix() - ci.LastAnnounce
	if secondsAgo < n.Config.Server.ClientExpireTime {
		newExpireAt := time.Now().Unix() + (n.Config.Server.ClientExpireTime - secondsAgo)
		item := &store.Item{
			Value: clientId,
			TTL:   newExpireAt,
		}
		heap.Push(n.ActiveClients, item)
		return true
	}
	return false
}

// Maintain currently active clients. This information is required to
// pass messages correctly.
func (n *Newton) maintainActiveClients() {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			fmt.Println(n.ActiveClients.Len())
			if n.ActiveClients.Len() > 0 {
				clientId := n.ActiveClients.Expire()
				if len(clientId) > 0 {
					// Read lock
					n.ConnTable.RLock()
					clientItem := *n.ConnTable.m[clientId]
					fmt.Println(clientId)
					fmt.Println(&clientItem)
					n.ConnTable.RUnlock()
					if reAdd := n.rescheduleClientTimeout(clientId, &clientItem); reAdd != true {
						delete(n.ConnTable.m, clientId)
					}
				}
			}
			/*case h := <-n.ClientQueue:
			// Add new clients
			n.ActiveClients.PushBack(h)*/
		}
	}
}

// Run a new Newton server instance
func (n *Newton) RunServer() {
	addr := n.Config.Server.Addr
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)

	if err != nil {
		n.Log.Fatal(err.Error())
	} else {
		netListen, err := net.Listen(tcpAddr.Network(), tcpAddr.String())

		if err != nil {
			n.Log.Fatal(err.Error())
		}

		// TODO: Override that method
		defer netListen.Close()
		n.Log.Info("Listening on port %s", n.Config.Server.Addr)

		// Start the database server
		go n.startDatabase()

		// Remove expired connections
		go n.maintainActiveClients()

		for {
			conn, err := netListen.Accept()
			if err != nil {
				n.Log.Fatal("Client Error: ", err.Error())
			} else {
				go n.ClientHandler(conn)
			}
		}
	}
}

// Handles client announce sockets
func (n *Newton) ClientHandler(conn net.Conn) {
	buffer := make([]byte, 1024)
	// Read messages from opened connection and
	// send them to incoming messages channel.
	for n.readClientStream(conn, buffer) {
		remoteAddr := conn.RemoteAddr().String()
		clientIp, err := cstream.ParseIP(remoteAddr)
		if err != nil {
			n.Log.Info(err.Error())
		}
		// Client sends its clientId
		// TODO:
		//  * Check this client id
		//  * Close connection if the client ip is non-existent.

		now := time.Now().Unix()
		// Check size and etc.
		clientId := string(buffer)

		// Go's maps are not thread-safe
		n.ConnTable.RLock()
		clientItem, ok := n.ConnTable.m[clientId]
		n.ConnTable.RUnlock()

		if ok {
			// Update clientItem struct
			clientItem.Ip = clientIp
			clientItem.LastAnnounce = now
		} else {
			// Create a new clientItem
			expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
			clientItem = &ClientItem{Ip: clientIp, LastAnnounce: now}

			// Add a new item to priority queue
			// TODO: Use lock mech. to protect that structure
			item := &store.Item{
				Value: clientId,
				TTL:   expireAt,
			}
			heap.Push(n.ActiveClients, item)
		}

		n.ConnTable.Lock()
		n.ConnTable.m[clientId] = clientItem
		n.ConnTable.Unlock()

		// Create or update id&item
		n.Log.Info(clientId)
		n.Log.Info(clientIp)
		conn.Close()
	}
}

// Reads incoming messages from connection and sets the bytes to a byte array
func (n *Newton) readClientStream(conn net.Conn, buffer []byte) bool {
	bytesRead, err := conn.Read(buffer)

	if err != nil {
		conn.Close()
		n.Log.Debug("Client connection closed: ", err.Error())
		return false
	}
	n.Log.Debug("Read %d byte(s)", bytesRead)
	return true
}

// Start a Gauss database node
func (n *Newton) startDatabase() {
	// TODO: Verbose option
	listenAddr := fmt.Sprintf("%s:%d", n.Config.Database.ListenIp, n.Config.Database.Port)
	broadcastAddr := fmt.Sprintf("%s:%d", n.Config.Database.BroadcastIp, n.Config.Database.Port)
	s := dhash.NewNodeDir(listenAddr, broadcastAddr, n.Config.Database.LogDir)
	// Start the database server
	s.MustStart()

	// Join a database cluster.
	if n.Config.Database.JoinIp != "" {
		joinAddr := fmt.Sprintf("%s:%d", n.Config.Database.JoinIp, n.Config.Database.JoinPort)
		s.MustJoin(joinAddr)
	}
}
