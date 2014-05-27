package newton

import (
	"container/heap"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/store"
	"net"
	"time"
)

const ReleaseVersion = "0.0.1"

type Newton struct {
	Config        *config.Config
	Log           cstream.Logger
	SetLogLevel   func(cstream.Level)
	ActiveClients *store.PriorityQueue
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

	return &Newton{
		Config:        c,
		Log:           l,
		SetLogLevel:   setlevel,
		ActiveClients: pq,
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
		go n.MaintainActiveClients()

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

func (n *Newton) ClientHandler(conn net.Conn) {
	buffer := make([]byte, 2048)

	_, err := conn.Read(buffer)
	if err != nil {
		n.Log.Info("Client connection error:", err.Error())
	}

	remoteAddr := conn.RemoteAddr().String()
	clientIP, err := cstream.ParseIP(remoteAddr)
	if err != nil {
		n.Log.Info(err.Error())
	}
	n.Log.Warning(string(buffer))
	n.Log.Info(clientIP)
	// Insert a new item and then modify its priority.
	ttl := time.Now().Unix() + 10
	item := &store.Item{
		Value: "euler_client",
		TTL:   ttl,
	}
	heap.Push(n.ActiveClients, item)
}

func (n *Newton) MaintainActiveClients() {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-tick.C:
			le := n.ActiveClients.Len()
			n.Log.Info("%d", le)
			if n.ActiveClients.Len() > 0 {
				n.ActiveClients.Expire()
			}
		}
	}
}
