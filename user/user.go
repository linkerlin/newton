package user

import (
	"github.com/nu7hatch/gouuid"
	"github.com/purak/gauss/common"
	"github.com/purak/gauss/gconn" // Client library for Gauss"
	"github.com/purak/gauss/murmur"
	"github.com/purak/newton/config"
	"github.com/purak/newton/cstream"
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
func New(c *config.Config) *UserStore {
	// Create a new configuration state
	if c == nil {
		c = config.New()
	}

	// New database connection
	conn := gconn.MustConn(c.Database.Addr)

	// Create a new logger
	l, setlevel := cstream.NewLogger("newton")

	return &UserStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
		Config:      c,
	}
}

// Creates a new user item on Gauss database
func (u *UserStore) Create(username, password string) error {
	// Create a unique salt string.
	salt, err := uuid.NewV4()
	if err != nil {
		return err
	}

	saltStr := salt.String()
	tmp := saltStr + password
	secret := murmur.HashString(tmp)
	// New user item
	user := &User{
		Username: username,
		Salt:     saltStr,
		Secret:   secret,
	}

	// Serialize the user
	bytes := common.MustJSONEncode(user)
	// Put it in the database
	u.Conn.Put(murmur.HashString(username), bytes)

	return nil
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
func (u *UserStore) CreateUserClient(username string) (string, error) {
	// Create a UUID.
	unique, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	tmp := unique.String() + username
	clientId := murmur.HashString(tmp)

	// Serialize the clientId
	bytes := common.MustJSONEncode(clientId)
	// Put it in the database
	u.Conn.SubPut(murmur.HashString(username), bytes, nil)

	return string(clientId), nil
}

// Gets UserClient items for the given key
func (u *UserStore) GetUserClient(username string) []common.Item {
	key := murmur.HashString(username)
	items := u.Conn.SliceLen(key, nil, true, u.Config.Database.MaxUserClient)
	return items
}
