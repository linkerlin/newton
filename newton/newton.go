package newton

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/message"
	"github.com/purak/newton/store"
	"github.com/purak/newton/user"
	"net"
	"sync"
	"time"
)

const ReleaseVersion = "0.0.1"

// 2xx => Success codes
// 1xx => Error codes

const (
	Success                = 200 // Generic success code
	Failed                 = 100 // Generic fail code
	AuthenticationFailed   = 101
	AuthenticationRequired = 102
	ServerError            = 103
	BadMessage             = 104 // Broken message

)

// Newton instance struct
type Newton struct {
	Config          *config.Config
	Log             cstream.Logger
	SetLogLevel     func(cstream.Level)
	ActiveClients   *store.PriorityQueue
	ClientQueue     chan *store.Item
	ConnTable       *ConnTable
	ConnClientTable *ConnClientTable
	UserStore       *user.UserStore
}

type ConnTable struct {
	sync.RWMutex // To protect maps
	m            map[string]*ClientItem
}

type ConnClientTable struct {
	sync.RWMutex // To protect maps
	c            map[*net.Conn]*ClientItem
}

// A container structure for active clients
type ClientItem struct {
	Ip            string
	LastAnnounce  int64
	SessionSecret string
	Conn          *net.Conn
}

// Create a new Newton instance
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
	ct := &ConnTable{m: make(map[string]*ClientItem)}
	cct := &ConnClientTable{c: make(map[*net.Conn]*ClientItem)}

	// ClientQueue for thread safety
	cq := make(chan *store.Item, 1000)

	// For reaching users on the cluster
	us := user.New(c)

	return &Newton{
		Config:          c,
		Log:             l,
		SetLogLevel:     setlevel,
		ActiveClients:   pq,
		ClientQueue:     cq,
		ConnTable:       ct,
		ConnClientTable: cct,
		UserStore:       us,
	}
}

// Extend expire time for active clients.
func (n *Newton) rescheduleClientTimeout(clientId string, ci *ClientItem) bool {
	secondsAgo := time.Now().Unix() - ci.LastAnnounce
	if secondsAgo < n.Config.Server.ClientAnnounceInterval {
		newExpireAt := time.Now().Unix() + (n.Config.Server.ClientAnnounceInterval - secondsAgo)
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
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if n.ActiveClients.Len() > 0 {
				clientId := n.ActiveClients.Expire()
				if len(clientId) > 0 {
					// Read lock
					n.ConnTable.RLock()
					clientItem := n.ConnTable.m[clientId]
					n.ConnTable.RUnlock()

					if clientItem != nil {
						// Check last activity and reschedule the conn if required.
						if reAdd := n.rescheduleClientTimeout(clientId, clientItem); reAdd != true {
							conn := *clientItem.Conn
							delete(n.ConnTable.m, clientId)
							if err := conn.Close(); err != nil {
								n.Log.Warning("TCP conn for %s could not be expired.", clientId)
							}
						}
					}
				}
			}
		case item := <-n.ClientQueue:
			// Add new clients
			heap.Push(n.ActiveClients, item)
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

		// Expire idle connections or reschedule them
		go n.maintainActiveClients()

		for {
			conn, err := netListen.Accept()
			if err != nil {
				n.Log.Fatal("Client Error: ", err.Error())
			} else {
				go n.ClientHandler(&conn)
			}
		}
	}
}

// Handles client announce sockets
func (n *Newton) ClientHandler(conn *net.Conn) {
	// Question: What about the bufio package?
	buff := make([]byte, 1024)

	// Read messages from opened connection and
	// send them to incoming messages channel.
	for n.readClientStream(buff, conn) {
		// Remove NULL characters
		buff = bytes.Trim(buff, "\x00")

		n.ConnClientTable.RLock()
		clientItem, ok := n.ConnClientTable.c[conn]
		n.ConnClientTable.RUnlock()

		now := time.Now().Unix()
		if ok && len(buff) == 0 {
			// Heartbeat message, update clientItem
			clientItem.LastAnnounce = now
		} else {
			if ok {
				// This is an authenticated client, update last activity data.
				clientItem.LastAnnounce = now
			}
			// If the message type is CreateSession, the connection record
			// will be created.
			go n.processIncomingMessage(buff, conn)
		}
		// Clean the buffer
		buff = make([]byte, 1024)
	}
}

// Reads incoming messages from connection and sets the bytes to a byte array
func (n *Newton) readClientStream(buff []byte, conn *net.Conn) bool {
	bytesRead, err := (*conn).Read(buff)
	if err != nil {
		(*conn).Close()
		n.Log.Debug("Client connection closed: ", err.Error())
		return false
	}
	n.Log.Debug("Read %d byte(s)", bytesRead)
	return true
}

// Parse and dispatch incoming messages
func (n *Newton) processIncomingMessage(buff []byte, conn *net.Conn) {
	var request interface{}
	var response []byte
	var err_ string
	var status int
	var typ string
	var ok bool
	closeConn := false

	// Sends error message
	onerror := func() {
		errMsg := &message.ErrorMsg{
			Type:   typ,
			Status: status,
			Body:   err_,
		}
		b, _ := json.Marshal(errMsg)
		// FIXME: Handle serialization errors
		// Return the error message
		(*conn).Write(b)
		if closeConn {
			(*conn).Close()
		}
	}

	err := json.Unmarshal(buff, &request)
	if err != nil {
		err_ = "Unknown message."
		status = BadMessage
		onerror()
	} else {
		items := request.(map[string]interface{})
		// Check type
		typ, ok = items["Type"].(string)
		if !ok {
			err_ = "Unknown message received."
			onerror()
		}

		switch {
		case typ == "Authenticate":
			response, status, err = n.authenticate(items, conn)
			closeConn = true
		case typ == "CreateUser":
			response, status, err = n.createUser(items)
		case typ == "CreateUserClient":
			response, status, err = n.createUserClient(items)
		}
		if err != nil {
			err_ = err.Error()
			onerror()
		} else {
			(*conn).Write(response)
		}
	}
}
