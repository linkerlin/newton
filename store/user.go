package store

import (
	"github.com/cstream/gauss/common"
	"github.com/cstream/gauss/gconn" // Client library for Gauss"
	"github.com/cstream/gauss/murmur"
	"github.com/cstream/newton/config"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/utils"
)

type UserStore struct {
	Config      *config.Config
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

type User struct {
	Username string
	Salt     string
	Secret   []byte
}

// Creates a new socket for reaching User items
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

// Creates a new user item on Gauss database
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

// Gets an user from database
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

// Creates a new ClientId
func (u *UserStore) CreateUserClient(username string) string {
	// Create a UUID.
	unique := utils.NewUUIDv1(u.Config.Server.Identity).String()
	clientId := username + "@" + unique
	// Put it in the database
	u.Conn.SubPut(murmur.HashString(username), []byte(clientId), nil)
	return clientId
}

// Checks clientId existence
func (u *UserStore) CheckUserClient(username, clientId string) bool {
	key := murmur.HashString(username)
	items := u.Conn.SliceLen(key, nil, true, u.Config.Database.MaxUserClient)
	for _, item := range items {
		if string(item.Key) == clientId {
			return true
		}
	}
	return false
}

// Gets UserClient items for the given key
func (u *UserStore) GetUserClients(username string) []common.Item {
	key := murmur.HashString(username)
	items := u.Conn.SliceLen(key, nil, true, u.Config.Database.MaxUserClient)
	return items
}

// TODO: this function must have a return Action
// Gets UserClient items for the given key
func (u *UserStore) DeleteUserClient(username string, clientId []byte) {
	key := murmur.HashString(username)
	u.Conn.SubDel(key, clientId)
}
