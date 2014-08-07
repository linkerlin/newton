package store

import (
	"github.com/cstream/gauss/gconn" // Client library for Gauss"
	"github.com/cstream/newton/config"
	"github.com/cstream/newton/cstream"
)

// MessageStore is a database object for maintaining messages.
type MessageStore struct {
	Config      *config.Config
	Conn        *gconn.Conn
	SetLogLevel func(cstream.Level)
	Log         cstream.Logger
}

// NewMessageStore returns a new MessageStore item.
func NewMessageStore(c *config.Config) *MessageStore {
	// Create a new logger
	l, setlevel := cstream.NewLogger("MessageStore")

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

	return &MessageStore{
		Conn:        conn,
		Log:         l,
		SetLogLevel: setlevel,
		Config:      c,
	}
}

// SetUserMessage sets a user message with a MsgID.
func (m *MessageStore) SetUserMessage(username string, data map[string]interface{}) error {
	key := username + "@msg"
	subKey := data["MsgID"].(string)
	msg, err := cstream.InterfaceToBytes(data)
	if err != nil {
		return err
	}
	m.Conn.SSubPut([]byte(key), []byte(subKey), msg)
	return nil
}

// GetUserMessage returns a user message with a MsgID.
func (m *MessageStore) GetUserMessage(username, msgID string) ([]byte, bool) {
	//TODO: What about the return value? Conver it into a suitable format.
	key := username + "@msg"
	return m.Conn.SubGet([]byte(key), []byte(msgID))
}

// DelUserMessage deletes a user message with a MsgID.
func (m *MessageStore) DelUserMessage(username, msgID string) {
	key := username + "@msg"
	m.Conn.SubDel([]byte(key), []byte(msgID))
}
