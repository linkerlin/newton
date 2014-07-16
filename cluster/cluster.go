package cluster

import (
	"errors"
	"github.com/nu7hatch/gouuid"
	"github.com/purak/gauss/common"
	"github.com/purak/gauss/gconn" // Client library for Gauss"
	"github.com/purak/gauss/murmur"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
)

// ClusterStore is a database object for maintaining newton instances on Gauss
type ClusterStore struct {
	Config      *config.Config
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

// Defines a new Newton instance on the DHT/database
type Server struct {
	Identity     string
	WanIp        string // Outbound interface IP
	WanPort      string // Outbound port
	InternalIp   string // Inbound interface IP, for only rack-aware setups
	InternalPort string // Inbound port
	Salt         string
	Secret       []byte
}

// Creates a new socket for reaching cluster members
func New(c *config.Config) *ClusterStore {
	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// New database connection
	conn := gconn.MustConn(c.Database.Addr)

	// Create a new logger
	l, setlevel := cstream.NewLogger("newton")

	return &ClusterStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
		Config:      c,
	}
}

// Creates a new server item on Gauss database
func (c *ClusterStore) Create(identity, password, wanIp, wanPort, internalIp, internalPort string) error {
	// Create a unique salt string.
	salt, err := uuid.NewV4()
	if err != nil {
		return err
	}

	saltStr := salt.String()
	tmp := saltStr + password
	secret := murmur.HashString(tmp)
	// New server item
	server := &Server{
		Identity:     identity,
		WanIp:        wanIp,
		WanPort:      wanPort,
		InternalIp:   internalIp,
		InternalPort: internalPort,
		Salt:         saltStr,
		Secret:       secret,
	}

	// Serialize the server item
	bytes := common.MustJSONEncode(server)
	// Put it in the database
	// TODO: this function must have a return Action
	c.Conn.Put(murmur.HashString(identity), bytes)

	return nil
}

// Gets a server item from database
func (c *ClusterStore) Get(identity string) (server *Server, existed bool) {
	key := murmur.HashString(identity)
	// Try to fetch the server
	data, existed := c.Conn.Get(key)
	// To unserialize it
	if !existed {
		return nil, existed
	}
	common.MustJSONDecode(data, &server)
	return server, existed
}

// Delete the server item from database
func (c *ClusterStore) Delete(identity string) error {
	key := murmur.HashString(identity)
	c.Conn.Del(key)
	// FIXME: This can be dangerous
	// Check the key
	_, existed := c.Conn.Get(key)
	if existed {
		return errors.New("Deleting is failed.")
	}
	return nil
}
