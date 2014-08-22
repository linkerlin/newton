package newton

import (
	"bytes"
	"encoding/json"
	"net"
	"time"

	"github.com/cstream/gauss/murmur"
	"github.com/cstream/newton/comm"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/store"
	"github.com/cstream/newton/utils"
)

// Check password and create and SessionSecret for authenticate the client
func (n *Newton) authenticateConn(salt, password, clientID string, secret []byte, conn *net.Conn) ([]byte, error) {
	// Recreate the secret and compare
	tmpSecret := murmur.HashString(salt + password)
	if !bytes.Equal(secret, tmpSecret) {
		return n.returnError(cstream.AuthenticationFailed)
	}

	// Authentication is done after that time

	// Go's maps are not thread-safe
	n.ClientTable.RLock()
	_, ok := n.ClientTable.m[clientID]
	n.ClientTable.RUnlock()

	if ok {
		// FIXME: Remove the previous connection, why?
		n.ConnTable.Lock()
		delete(n.ConnTable.c, conn)
		n.ConnTable.Unlock()

		n.ClientTable.Lock()
		delete(n.ClientTable.m, clientID)
		n.ClientTable.Unlock()
		return n.returnError(cstream.AuthenticationFailed, cstream.HasAnotherConnection)
	}

	// Create a new clientItem
	expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
	sessionSecret := utils.NewUUIDv1(n.Config.Server.Identity)

	now := time.Now().Unix()
	clientItem := &ClientItem{
		LastAnnounce:  now,
		SessionSecret: sessionSecret.String(),
		Conn:          conn,
		TrackerEvents: make(chan OnlineUserEvent, 10),
	}

	// Add a new item to priority queue
	item := &store.Item{
		Value: clientID,
		TTL:   expireAt,
	}
	n.ClientQueue <- item

	// Set to ClientTable
	n.ClientTable.Lock()
	n.ClientTable.m[clientID] = clientItem
	n.ClientTable.Unlock()

	// Set to ConnTable
	n.ConnTable.Lock()
	n.ConnTable.c[conn] = clientItem
	n.ConnTable.Unlock()

	msg := &comm.Authenticated{
		Action:        cstream.Authenticated,
		SessionSecret: sessionSecret.String(),
	}

	return n.msgToByte(msg)
}

// Generates an error comm
func (n *Newton) returnError(args ...int) ([]byte, error) {
	code := 0
	if len(args) == 2 {
		code = args[1]
	}
	msg := comm.Error{
		Action: args[0],
		Code:   code,
	}
	return n.msgToByte(msg)
}

// Converts json to byte slice
func (n *Newton) msgToByte(msg interface{}) ([]byte, error) {
	r, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return r, nil
}
