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
