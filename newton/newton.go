package newton

import (
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
	"net"
)

const ReleaseVersion = "0.0.1"

type Newton struct {
	Config      *config.Config
	Log         Logger
	SetLogLevel func(Level)
}

func New(c *config.Config) *Newton {
	if c == nil {
		c = config.New()
	}

	l, setlevel := newLogger("newton")
	return &Newton{
		Config:      c,
		Log:         l,
		SetLogLevel: setlevel,
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
}
