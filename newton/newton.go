package newton

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"errors"
	"github.com/nu7hatch/gouuid"
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
	us := user.New()

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
		if ok && len(buff) == 0 {
			// Update clientItem struct
			now := time.Now().Unix()
			clientItem.LastAnnounce = now
		} else {
			err := n.processIncomingMessage(buff, conn)
			if err != nil {
				msg := &message.ErrorMsg{
					Type: "Error",
					Body: err.Error(),
				}
				b, _ := json.Marshal(msg)
				// FIXME: Handle serialization errors
				// Return the error message
				(*conn).Write(b)
			}
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
func (n *Newton) processIncomingMessage(buff []byte, conn *net.Conn) error {
	var msg interface{}
	err := json.Unmarshal(buff, &msg)
	if err != nil {
		return errors.New("Incoming message could not be unmarshaled.")
	}

	items := msg.(map[string]interface{})

	// Check type
	t, ok := items["Type"]
	if !ok {
		return errors.New("Unknown message received.")
	}

	switch {
	case t == "CreateSession":
		m, err := n.createSession(items, conn)
		if err != nil {
			(*conn).Close()
			return err
		}
		(*conn).Write(m)
	case t == "CreateUser":
		m, err := n.createUser(items)
		if err != nil {
			return err
		}
		(*conn).Write(m)
	}

	return nil
}

// Create a new client session
func (n *Newton) createSession(data map[string]interface{}, conn *net.Conn) ([]byte, error) {
	clientId, ok := data["ClientId"].(string)
	if !ok {
		return nil, errors.New("ClientId doesn't exist or invalid.")
	}

	remoteAddr := (*conn).RemoteAddr().String()
	clientIp, err := cstream.ParseIP(remoteAddr)
	if err != nil {
		return nil, err
	}

	now := time.Now().Unix()
	// Go's maps are not thread-safe
	n.ConnTable.RLock()
	_, ok = n.ConnTable.m[clientId]
	n.ConnTable.RUnlock()

	if ok {
		(*conn).Close()
		delete(n.ConnTable.m, clientId)
	}

	// Create a new clientItem
	expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
	secret, err := uuid.NewV4()
	if err != nil {
		(*conn).Close()
		return nil, err
	}

	clientItem := &ClientItem{
		Ip:            clientIp,
		LastAnnounce:  now,
		SessionSecret: secret.String(),
		Conn:          conn,
	}

	// Add a new item to priority queue
	item := &store.Item{
		Value: clientId,
		TTL:   expireAt,
	}
	n.ClientQueue <- item

	// Set to ConnTable
	n.ConnTable.Lock()
	n.ConnTable.m[clientId] = clientItem
	n.ConnTable.Unlock()

	// Set to ConnClientTable
	n.ConnClientTable.Lock()
	n.ConnClientTable.c[conn] = clientItem
	n.ConnClientTable.Unlock()

	msg := &message.Authenticated{
		Type:          "Authenticated",
		SessionSecret: secret.String(),
	}

	b, err := json.Marshal(msg)
	if err != nil {
		(*conn).Close()
		return nil, err
	}

	return b, nil
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return nil, errors.New("Username is required.")
	}

	// Check password
	password, ok := data["Password"].(string)
	if !ok {
		return nil, errors.New("Password is required.")
	}

	_, existed := n.UserStore.Get(username)
	if !existed {
		// Finally, create a new user
		err := n.UserStore.Create(username, password)

		if err != nil {
			return nil, err
		}

		clientId, err := n.UserStore.CreateUserClient(username)
		if err != nil {
			return nil, err
		}

		msg := &message.ClientId{
			Type:     "ClientId",
			ClientId: clientId,
		}

		b, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}

		return b, nil
	} else {
		return nil, errors.New("Already exist.")
	}
}
