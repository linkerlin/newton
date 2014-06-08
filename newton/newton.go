package newton

import (
	"container/list"
	"fmt"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/store"
	"math/rand" // This is temporary
	"net"
	"strconv" // This is temporary
	"time"
)

const ReleaseVersion = "0.0.1"

type Newton struct {
	Config        *config.Config
	Log           cstream.Logger
	SetLogLevel   func(cstream.Level)
	ActiveSockets list.List
	SocketQueue   chan *SocketTimeoutItem
	UserStore     *store.UserStore
}

type Connection struct {
	Conn         *net.Conn
	LastActivity int64
}

type SocketTimeoutItem struct {
	Conn     *Connection
	ExpireAt int64
}

func New(c *config.Config) *Newton {
	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// Create a new logger
	l, setlevel := cstream.NewLogger("newton.server")
	sq := make(chan *SocketTimeoutItem)
	us := store.NewUserStore("data/userstore.gob")
	return &Newton{
		Config:      c,
		Log:         l,
		SetLogLevel: setlevel,
		SocketQueue: sq,
		UserStore:   us,
	}
}

func (n *Newton) isSocketExpired(LastActivity int64, ExpireAt int64) bool {
	return ExpireAt-LastActivity >= 10
}

func (n *Newton) rescheduleSocketTimeout(sc *SocketTimeoutItem) {
	if n.isSocketExpired(sc.Conn.LastActivity, sc.ExpireAt) {
		conn := *sc.Conn.Conn
		conn.Close()
	} else {
		sc.ExpireAt = time.Now().Unix() + 10
		n.ActiveSockets.PushBack(sc)
	}
}

func (n *Newton) maintainActiveSockets() {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			now := time.Now().Unix()
			for e := n.ActiveSockets.Front(); e != nil; e = e.Next() {
				ExpireAt := e.Value.(*SocketTimeoutItem).ExpireAt
				if ExpireAt <= now {
					removedItem := n.ActiveSockets.Remove(e)
					n.rescheduleSocketTimeout(removedItem.(*SocketTimeoutItem))
				} else {
					break
				}
			}
		case h := <-n.SocketQueue:
			n.ActiveSockets.PushBack(h)
		}
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
		go n.maintainActiveSockets()

		for {
			conn, err := netListen.Accept()
			c := new(Connection)
			c.Conn = &conn
			c.LastActivity = time.Now().Unix()

			if err != nil {
				n.Log.Fatal("Client Error: ", err.Error())
			} else {
				// WARNING: This is a temporary hack.
				clientId := strconv.Itoa(rand.Intn(1000000))
				go n.ClientHandler(c, clientId)
			}
		}
	}
}

func (n *Newton) ClientHandler(c *Connection, clientId string) {
	sc := new(SocketTimeoutItem)
	sc.Conn = c
	sc.ExpireAt = time.Now().Unix() + 10
	n.SocketQueue <- sc

	buffer := make([]byte, 1024)
	// Read messages from opened connection and
	// send them to incoming messages channel.
	conn := *c.Conn
	for n.readClientStream(c, buffer) {
		remoteAddr := conn.RemoteAddr().String()
		clientIP, err := cstream.ParseIP(remoteAddr)
		if err != nil {
			n.Log.Info(err.Error())
		}
		n.Log.Debug(string(buffer))
		n.Log.Info(clientIP)
	}
}

// Reads incoming messages from connection and sets the bytes to a byte array
func (n *Newton) readClientStream(c *Connection, buffer []byte) bool {
	conn := *c.Conn
	bytesRead, err := conn.Read(buffer)
	c.LastActivity = time.Now().Unix()

	var v, k string
	k = "key"
	//n.Log.Info("%s %s", &k, &v)
	t := n.UserStore.Put(&v, &k)
	fmt.Println(t)

	if err != nil {
		conn.Close()
		n.Log.Debug("Client connection closed: ", err.Error())
		return false
	}
	n.Log.Debug("Read %d byte(s)", bytesRead)
	return true
}
