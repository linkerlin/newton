package newton

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"net"
	"sync"
	"time"

	"github.com/cstream/newton/config"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/store"
)

// RelaseVersion is the program version.
const ReleaseVersion = "0.0.1"

// Defines a function
type actionHandler func(map[string]interface{}) ([]byte, error)

// Newton represents a new instance.
type Newton struct {
	Config            *config.Config
	Log               cstream.Logger
	SetLogLevel       func(cstream.Level)
	ActiveClients     *store.PriorityQueue
	ClientQueue       chan *store.Item
	ClientTable       *ClientTable
	ConnTable         *ConnTable
	UserStore         *store.UserStore
	ClusterStore      *store.ClusterStore
	MessageStore      *store.MessageStore
	InternalConnTable *InternalConnTable
	ActionHandlers    map[int]actionHandler // Ships action handlers
	TrackUserQueries  chan string
	RoutingQueue      chan TrackerEvent
	RoutingTable      *RoutingTable
}

// ClientTable stores active client sessions by clientID.
type ClientTable struct {
	sync.RWMutex // To protect maps
	m            map[string]*ClientItem
}

// ConnTable stores active client sessions by pointer of net.Conn.
type ConnTable struct {
	sync.RWMutex // To protect maps
	c            map[*net.Conn]*ClientItem
}

// ClientItem is a container structure for active clients.
type ClientItem struct {
	LastAnnounce  int64 //TODO: Rename this.
	SessionSecret string
	Conn          *net.Conn
	ActivityEvent chan string // A channel for receiving events about other user's active sessions.
}

// InternalConnTable stores opened sockets for other newton instances.
type InternalConnTable struct {
	sync.RWMutex // To protect maps
	i            map[string]*ServerItem
}

// ServerItem stores opened sessions on the other newton instances.
type ServerItem struct {
	Conn          *net.Conn
	Outgoing      chan []byte
	SessionSecret string
}

type RouteItem struct {
	Processed   bool
	ExpireAt    int64
	ClientItems []RouteClientItem
	Subscribers []*ClientItem
}

type RouteClientItem struct {
	ClientID string
	Location string
}

type RoutingTable struct {
	sync.RWMutex
	r map[string]*RouteItem
}

// New creates a new Newton instance
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
	ct := &ClientTable{m: make(map[string]*ClientItem)}
	cct := &ConnTable{c: make(map[*net.Conn]*ClientItem)}

	// ClientQueue for thread safety
	cq := make(chan *store.Item, 1000)

	// For reaching users on the cluster
	us := store.NewUserStore(c)

	// For talking to other newton servers
	cl := store.NewClusterStore(c)
	ict := &InternalConnTable{i: make(map[string]*ServerItem)}

	// Action handlers map
	act := make(map[int]actionHandler)

	// New MessageStore
	m := store.NewMessageStore(c)

	// Tracker queries
	t := make(chan string, 1000)
	rt := &RoutingTable{r: make(map[string]*RouteItem)}
	rq := make(chan TrackerEvent, 100)

	return &Newton{
		Config:            c,
		Log:               l,
		SetLogLevel:       setlevel,
		ActiveClients:     pq,
		ClientQueue:       cq,
		ClientTable:       ct,
		ConnTable:         cct,
		UserStore:         us,
		ClusterStore:      cl,
		MessageStore:      m,
		InternalConnTable: ict,
		ActionHandlers:    act,
		TrackUserQueries:  t,
		RoutingQueue:      rq,
		RoutingTable:      rt,
	}
}

// Extend expire time for active clients.
func (n *Newton) rescheduleClientTimeout(clientID string, ci *ClientItem) bool {
	secondsAgo := time.Now().Unix() - ci.LastAnnounce
	if secondsAgo < n.Config.Server.ClientAnnounceInterval {
		newExpireAt := time.Now().Unix() + (n.Config.Server.ClientAnnounceInterval - secondsAgo)
		item := &store.Item{
			Value: clientID,
			TTL:   newExpireAt,
		}
		heap.Push(n.ActiveClients, item)
		return true
	}
	return false
}

// Maintain currently active clients. This information is required to pass messages correctly.
func (n *Newton) maintainActiveClients() {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if n.ActiveClients.Len() == 0 {
				break
			}
			clientID := n.ActiveClients.Expire()
			if len(clientID) == 0 {
				break
			}
			// Read lock
			n.ClientTable.RLock()
			clientItem := n.ClientTable.m[clientID]
			n.ClientTable.RUnlock()

			if clientItem != nil {
				// Check last activity and reschedule the conn if required.
				if reAdd := n.rescheduleClientTimeout(clientID, clientItem); reAdd != true {
					conn := *clientItem.Conn
					delete(n.ClientTable.m, clientID)
					if err := conn.Close(); err != nil {
						n.Log.Warning("TCP conn for %s could not be expired: %s", clientID, err.Error())
					}
				}
			}
		case item := <-n.ClientQueue:
			// Add new clients
			heap.Push(n.ActiveClients, item)
		}
	}
}

// RunServer starts a new Newton server instance
func (n *Newton) RunServer() {
	netListen, err := net.Listen("tcp4", n.Config.Server.Port)

	if err != nil {
		n.Log.Fatal(err.Error())
	}

	// TODO: Override that method
	defer netListen.Close()
	n.Log.Info("Listening on port %s", n.Config.Server.Port)

	// Expire idle connections or reschedule them
	go n.maintainActiveClients()

	n.setActionHandlers()
	err = n.createOrUpdateServer()
	if err != nil {
		n.Log.Fatal(err.Error())
	}

	// Run a UDP server for internal lookup requests
	go n.runUDPServer()
	// Run tracker to update currently online user list.
	go n.runTracker()

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

// ClientHandler waits for new messages for opened connections.
func (n *Newton) ClientHandler(conn *net.Conn) {
	// Question: What about the bufio package?
	buff := make([]byte, 1024)

	// Close the connection if no comm received.
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

		n.ConnTable.RLock()
		clientItem, ok := n.ConnTable.c[conn]
		n.ConnTable.RUnlock()

		now := time.Now().Unix()
		if ok && len(buff) == 0 {
			// Heartbeat comm, update clientItem
			clientItem.LastAnnounce = now
		} else {
			if ok {
				// This is an authenticated client, update last activity data.
				clientItem.LastAnnounce = now
			}
			// If the comm Action is CreateSession, the connection record
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

// Maps action handler functions
func (n *Newton) setActionHandlers() {
	n.ActionHandlers[cstream.AuthenticateUser] = n.authenticateUser
	n.ActionHandlers[cstream.CreateUser] = n.createUser
	n.ActionHandlers[cstream.CreateUserClient] = n.createUserClient
	n.ActionHandlers[cstream.AuthenticateServer] = n.authenticateServer
	n.ActionHandlers[cstream.LookupUser] = n.lookupUser
}

// Parse and dispatch incoming messages
func (n *Newton) dispatchMessages(buff []byte, conn *net.Conn) {
	var request interface{}
	var ok bool
	var response []byte
	var action int
	ok = false

	// Send an error comm
	handleError := func(status, code int) {
		// FIXME: Handle serialization errors
		m, e := n.returnError(status, code)
		if e != nil {
			n.Log.Error("Internal Server Error: %s", e.Error())
		}
		(*conn).Write(m)
		for s := range []int{cstream.AuthenticateServer, cstream.AuthenticateUser, cstream.BadMessage} {
			if action == s {
				(*conn).Close()
			}
		}
	}

	err := json.Unmarshal(buff, &request)
	if err != nil {
		handleError(cstream.BadMessage, 0)
	} else if request != nil {
		items := request.(map[string]interface{})
		// We need int but encoding/json returns float64 for numbers
		// This is a pretty bad hack but it works :|
		var s float64
		s, ok = items["Action"].(float64)
		if !ok {
			handleError(cstream.BadMessage, cstream.ActionRequired)
		}
		action = int(s)

		// Set the connection pointer for later use
		items["Conn"] = conn

		// Set the authenticated client if it exists.
		c, ok := n.ConnTable.c[conn]
		if ok {
			items["ClientItem"] = c
		}

		if action == cstream.Authenticated {
			// This is a custom action, no need for a return value
			n.startInternalCommunication(items)
		} else if handler, ok := n.ActionHandlers[action]; ok {
			response, err = handler(items)
			if err != nil {
				n.Log.Error("Internal error: %s", err.Error())
				handleError(action, cstream.ServerError)
			} else {
				(*conn).Write(response)
			}
		} else {
			handleError(cstream.BadMessage, cstream.UnknownAction)
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
		/*if server.InternalIP != "" {
			serverAddr = server.InternalIP + server.Port
		} else {
			serverAddr = server.WanIP + server.Port
		}*/
		// TODO: This is a temporary fix
		serverAddr = "127.0.0.1" + server.Port

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
			// Remember that we use identity as clientID for newton instances
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

// Writes a comm throught an opened connection
func (n *Newton) writeMessage(msg interface{}, conn *net.Conn) {
	response, err := json.Marshal(msg)
	if err != nil {
		panic(err.Error())
	}
	(*conn).Write(response)
}
