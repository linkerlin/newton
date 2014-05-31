package newton

import (
	"container/heap"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/store"
	"math/rand" // This is temporary
	"net"
	"strconv" // This is temporary
	"sync"
	"time"
)

const ReleaseVersion = "0.0.1"

// To protect maps
type ConnTable struct {
	sync.RWMutex
	m map[string]*net.Conn
}

type Newton struct {
	Config        *config.Config
	Log           cstream.Logger
	SetLogLevel   func(cstream.Level)
	ActiveClients *store.PriorityQueue
	ConnTable     *ConnTable
}

func New(c *config.Config) *Newton {
	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// Create a new logger
	l, setlevel := cstream.NewLogger("newton")

	// Create a priority queue to hold active client connections
	pq := &store.PriorityQueue{}
	heap.Init(pq)

	// Connection table
	ct := &ConnTable{m: make(map[string]*net.Conn)}

	return &Newton{
		Config:        c,
		Log:           l,
		SetLogLevel:   setlevel,
		ActiveClients: pq,
		ConnTable:     ct,
	}
}

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

		// Track active client connections and remove expired items
		go n.maintainActiveClients()

		for {
			conn, err := netListen.Accept()
			if err != nil {
				n.Log.Fatal("Client Error: ", err.Error())
			} else {
				// WARNING: This is a temporary hack.
				clientId := strconv.Itoa(rand.Intn(1000000))
				// Go's maps are not thread-safe
				n.ConnTable.Lock()
				n.ConnTable.m[clientId] = &conn
				n.ConnTable.Unlock()

				go n.ClientHandler(conn, clientId)
			}
		}
	}
}

func (n *Newton) ClientHandler(conn net.Conn, clientId string) {
	buffer := make([]byte, 2048)
	// Add this clients to active clients queue
	// Another goroutine maintains this priority queue and removes
	// expired items.
	ttl := time.Now().Unix() + 10
	item := &store.Item{
		Value: clientId,
		TTL:   ttl,
	}
	heap.Push(n.ActiveClients, item)

	// Read messages from opened connection and
	// send them to incoming messages channel.
	for n.readClientStream(conn, buffer) {
		remoteAddr := conn.RemoteAddr().String()
		clientIP, err := cstream.ParseIP(remoteAddr)
		if err != nil {
			n.Log.Info(err.Error())
		}
		n.Log.Warning(string(buffer))
		n.Log.Info(clientIP)
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
	n.Log.Debug("Read ", bytesRead, " bytes")
	return true
}

// Runs the expire function periodically, removes expired items from queue and
// closes expired connections.
func (n *Newton) maintainActiveClients() {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			le := n.ActiveClients.Len()
			n.Log.Info("%d", le)
			if n.ActiveClients.Len() > 0 {
				clientId := n.ActiveClients.Expire()
				n.Log.Info(clientId)
				if len(clientId) > 0 {
					// Go's maps are not thread-safe
					n.ConnTable.RLock()
					conn := *n.ConnTable.m[clientId]
					n.ConnTable.RUnlock()
					conn.Close()
				}
			}
		}
	}
}
