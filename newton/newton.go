package newton

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/store"
)

const ReleaseVersion = "0.0.1"

// Newton instance struct
type Newton struct {
	Config            *config.Config
	Log               cstream.Logger
	SetLogLevel       func(cstream.Level)
	ActiveClients     *store.PriorityQueue
	ClientQueue       chan *store.Item
	ConnTable         *ConnTable
	ConnClientTable   *ConnClientTable
	UserStore         *store.UserStore
	ClusterStore      *store.ClusterStore
	InternalConnTable *InternalConnTable
}

// Stores active client sessions by clientId
type ConnTable struct {
	sync.RWMutex // To protect maps
	m            map[string]*ClientItem
}

// Stores active client sessions by pointer of net.Conn
type ConnClientTable struct {
	sync.RWMutex // To protect maps
	c            map[*net.Conn]*ClientItem
}

// A container structure for active clients
type ClientItem struct {
	LastAnnounce  int64
	SessionSecret string
	Conn          *net.Conn
}

// Stores opened sockets for other newton instances
type InternalConnTable struct {
	sync.RWMutex // To protect maps
	i            map[string]*ServerItem
}

// Stores opened sessions on the other newton instances
type ServerItem struct {
	Conn          *net.Conn
	Outgoing      chan []byte
	SessionSecret string
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
	us := store.NewUserStore(c)

	// For talking to other newton servers
	cl := store.NewClusterStore(c)
	ict := &InternalConnTable{i: make(map[string]*ServerItem)}

	return &Newton{
		Config:            c,
		Log:               l,
		SetLogLevel:       setlevel,
		ActiveClients:     pq,
		ClientQueue:       cq,
		ConnTable:         ct,
		ConnClientTable:   cct,
		UserStore:         us,
		ClusterStore:      cl,
		InternalConnTable: ict,
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
								n.Log.Warning("TCP conn for %s could not be expired: %s", clientId, err.Error())
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

		// go n.internalConnection("lpms")

		// Listen incoming connections and start a goroutine to handle
		// clients
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

	// Close the connection if no message received.
	var isReal bool
	go func() {
		tick := time.NewTicker(5 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if !isReal {
					(*conn).Close()
					return
				}
			}
		}
	}()

	// Read messages from opened connection and
	// send them to incoming messages channel.
	for n.readConn(buff, conn) {
		isReal = true // the clients send messages

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
			// If the message Action is CreateSession, the connection record
			// will be created.
			go n.dispatchMessages(buff, conn)
		}
		// Clean the buffer
		buff = make([]byte, 1024)
	}
}

// Reads incoming messages from connection and sets the bytes to a byte array
func (n *Newton) readConn(buff []byte, conn *net.Conn) bool {
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
func (n *Newton) dispatchMessages(buff []byte, conn *net.Conn) {
	var request interface{}
	var ok, closeConn bool = false, false
	var returnResponse bool = true
	var response []byte
	var action int

	// Send an error message
	onerror := func(status, code int) {
		// FIXME: Handle serialization errors
		m, e := n.returnError(status, code)
		if e != nil {
			n.Log.Error("Internal Server Error: %s", e.Error())
		}
		(*conn).Write(m)
		if closeConn {
			(*conn).Close()
		}
	}

	err := json.Unmarshal(buff, &request)
	if err != nil {
		closeConn = true
		onerror(cstream.BadMessage, 0)
	} else if request != nil {
		items := request.(map[string]interface{})
		// Check Action
		// We need int but encoding/json returns float64 for numbers
		// This is a pretty bad hack but it works :|
		var s float64
		s, ok = items["Action"].(float64)
		if !ok {
			closeConn = true
			onerror(cstream.BadMessage, cstream.ActionRequired)
		}
		action = int(s)

		// TODO: We need a better message and error handling mech.
		// Dispatch incoming messages and run related functions
		switch {
		case action == cstream.AuthenticateUser:
			response, err = n.authenticateUser(items, conn)
			// Close connection on error
			closeConn = true
		case action == cstream.CreateUser:
			response, err = n.createUser(items)
		case action == cstream.CreateUserClient:
			response, err = n.createUserClient(items)
		case action == cstream.AuthenticateServer:
			response, err = n.authenticateServer(items, conn)
			// Close connection on error
			closeConn = true
		case action == cstream.Authenticated:
			// TODO: Think about potential security vulnerables
			// This is an internal connection between newton instances
			n.startInternalCommunication(items, conn)
			returnResponse = false
		}

		if returnResponse {
			if err != nil {
				n.Log.Error("SERVER ERROR: %s", err.Error())
				onerror(action, cstream.ServerError)
			} else {
				(*conn).Write(response)
			}
		}
	}
}

// Opens and listens a socket between newton instances
func (n *Newton) internalConnection(identity string) {
	// Get the instance
	server, ok := n.ClusterStore.Get(identity)
	if !ok {
		n.Log.Warning("%s could not be found on cluster.", identity)
	} else {
		var serverAddr string
		if server.InternalIp != "" {
			serverAddr = server.InternalIp + ":" + server.InternalPort
		} else {
			serverAddr = server.WanIp + ":" + server.WanPort
		}

		// Make a connection between the instance and us.
		conn, err := net.Dial("tcp4", serverAddr)
		if err != nil {
			n.Log.Warning("Dial failed: %s", err.Error())
		} else {
			defer conn.Close()

			// Try to get currently opened session
			n.InternalConnTable.RLock()
			serverItem, ok := n.InternalConnTable.i[identity]
			n.InternalConnTable.RUnlock()

			n.InternalConnTable.Lock()
			if ok {
				// Remove and close existed connection
				// TODO: Handle error cases?
				(*serverItem.Conn).Close()
				delete(n.InternalConnTable.i, identity)
			} else {
				// Create a new ServerItem for the new connection without SessionSecret
				server := &ServerItem{
					Conn:     &conn,
					Outgoing: make(chan []byte, 100), // How about 100?
				}
				n.InternalConnTable.i[identity] = server
			}
			n.InternalConnTable.Unlock()
			// Send authentication credentials
			// Remember that we use identity as clientId for newton instances
			n.Log.Info("Sending authentication request to %s", serverAddr)
			go n.authenticateServerReq(n.Config.Server.Identity, n.Config.Server.Password, &conn)

			// Read chunks from the connection and start a goroutine parse and fire related functions
			n.Log.Info("Waiting data from %s", serverAddr)
			buff := make([]byte, 1024)
			for n.readConn(buff, &conn) {
				buff = bytes.Trim(buff, "\x00")
				go n.dispatchMessages(buff, &conn)
			}
		}
	}
}

// Writes a message throught an opened connection
func (n *Newton) writeMessage(msg interface{}, conn *net.Conn) {
	response, err := json.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}
	(*conn).Write(response)
}
