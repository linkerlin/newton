package store

import (
	"github.com/cstream/gauss/common"
	"github.com/cstream/gauss/gconn" // Client library for Gauss"
	"github.com/cstream/gauss/murmur"
	"github.com/cstream/newton/config"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/utils"
)

// UserStore stores functions to manage users.
type UserStore struct {
	Config      *config.Config
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

// User represents an user item on the database.
type User struct {
	Username string
	Salt     string
	Secret   []byte
}

// NewUserStore creates a new socket for reaching User items
func NewUserStore(c *config.Config) *UserStore {
	// Create a new logger
	l, setlevel := cstream.NewLogger("UserStore")

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

	return &UserStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
		Config:      c,
	}
}

// Create is a function for creating new user items on Gauss database.
func (u *UserStore) Create(username, password string) {
	// Create a unique salt string.
	salt := utils.NewUUIDv1(u.Config.Server.Identity).String()

	tmp := salt + password
	secret := murmur.HashString(tmp)
	// New user item
	user := &User{
		Username: username,
		Salt:     salt,
		Secret:   secret,
	}

	// Serialize the user
	// TODO: Error cases?
	bytes := common.MustJSONEncode(user)
	// Put it in the database
	// TODO: this function must have a return Action
	u.Conn.Put(murmur.HashString(username), bytes)
}

// Get returns an user item from database.
func (u *UserStore) Get(username string) (user *User, existed bool) {
	key := murmur.HashString(username)
	// Try to fetch the user
	data, existed := u.Conn.Get(key)
	// To unserialize it
	if !existed {
		return nil, existed
	}
	common.MustJSONDecode(data, &user)
	return user, existed
}

// CreateUserClient creates a new clientID
func (u *UserStore) CreateUserClient(username string) string {
	// Create a UUID.
	unique := utils.NewUUIDv1(u.Config.Server.Identity).String()
	clientID := username + "@" + unique
	// Put it in the database
	u.Conn.SubPut(murmur.HashString(username), []byte(clientID), nil)
	return clientID
}

// CheckUserClient checks clientID existence
func (u *UserStore) CheckUserClient(username, clientID string) bool {
	key := murmur.HashString(username)
	items := u.Conn.SliceLen(key, nil, true, u.Config.Database.MaxUserClient)
	for _, item := range items {
		if string(item.Key) == clientID {
			return true
		}
	}
	return false
}

// GetUserClients returns clients for the given key
func (u *UserStore) GetUserClients(username string) []common.Item {
	key := murmur.HashString(username)
	items := u.Conn.SliceLen(key, nil, true, u.Config.Database.MaxUserClient)
	return items
}

// DeleteUserClient deletes client items for the given key
func (u *UserStore) DeleteUserClient(username string, clientID []byte) {
	// TODO: DeleteUserClient must have a return Action
	key := murmur.HashString(username)
	u.Conn.SubDel(key, clientID)
}
