package user

import (
	//"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/purak/gauss/common"
	"github.com/purak/gauss/gconn" // Client library for Gauss"
	"github.com/purak/gauss/murmur"
	"github.com/purak/newton/cstream"
)

type UserStore struct {
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

type User struct {
	Username string
	Salt     string
	Secret   []byte
}

type UserClient struct {
	Username string
	ClientId string
}

// Creates a new socket for reaching User items
func New() *UserStore {
	// Create a new logger
	l, setlevel := cstream.NewLogger("newton")

	// FIXME: Hardcoded, temporarily
	conn := gconn.MustConn("localhost:9191")
	return &UserStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
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

// Gets a user from database
func (u *UserStore) Get(username string) (user User, existed bool) {
	key := murmur.HashString(username)
	// Try to fetch the user
	data, existed := u.Conn.Get(key)
	// To unserialize it
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
	clientId := string(murmur.HashString(tmp))

	// New user item
	uc := &UserClient{
		Username: username,
		ClientId: clientId,
	}

	// Serialize the user
	bytes := common.MustJSONEncode(uc)
	// Put it in the database
	u.Conn.Put(murmur.HashString(username), bytes)

	return clientId, nil
}
