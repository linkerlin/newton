package store

import (
	"errors"

	"github.com/cstream/gauss/common"
	"github.com/cstream/gauss/gconn" // Client library for Gauss"
	"github.com/cstream/gauss/murmur"
	"github.com/cstream/newton/config"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/utils"
)

// ClusterStore is a database object for maintaining newton instances on Gauss
type ClusterStore struct {
	Config      *config.Config
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

// Server represents a new Newton instance on the DHT/database
type Server struct {
	Identity   string
	WanIP      string // Outbound interface IP
	Port       string
	InternalIP string // Inbound interface IP, for only rack-aware setups
	Salt       string
	Secret     []byte
}

// NewClusterStore creates a new socket for reaching cluster members
func NewClusterStore(c *config.Config) *ClusterStore {
	// Create a new logger
	l, setlevel := cstream.NewLogger("ClusterStore")

	defer func() {
		if r := recover(); r != nil {
			l.Fatal("%s", r)
		}
	}()

	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// New database connection
	conn := gconn.MustConn(c.Database.Addr)

	return &ClusterStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
		Config:      c,
	}
}

// Create creates a new server item on Gauss database
func (c *ClusterStore) Create(identity, password, wanIP, port, internalIP string) {
	// Create a unique salt string.
	salt := utils.NewUUIDv1(c.Config.Server.Identity).String()

	tmp := salt + password
	secret := murmur.HashString(tmp)
	// New server item
	server := &Server{
		Identity:   identity,
		WanIP:      wanIP,
		Port:       port,
		InternalIP: internalIP,
		Salt:       salt,
		Secret:     secret,
	}

	// Serialize the server item
	bytes := common.MustJSONEncode(server)
	// Put it in the database
	// TODO: this function must have a return Action
	c.Conn.Put(murmur.HashString(identity), bytes)
}

// Get is a function that gets server item from database
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

// Delete is a function that deletes the server item from database
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
