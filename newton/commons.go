package newton

import (
	"bytes"
	"encoding/json"
	"net"
	"time"

	"github.com/cstream/gauss/murmur"
	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/message"
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
	n.ConnTable.RLock()
	_, ok := n.ConnTable.m[clientID]
	n.ConnTable.RUnlock()

	if ok {
		// FIXME: Remove the previous connection, why?
		n.ConnClientTable.Lock()
		delete(n.ConnClientTable.c, conn)
		n.ConnClientTable.Unlock()

		n.ConnTable.Lock()
		delete(n.ConnTable.m, clientID)
		n.ConnTable.Unlock()
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
	}

	// Add a new item to priority queue
	item := &store.Item{
		Value: clientID,
		TTL:   expireAt,
	}
	n.ClientQueue <- item

	// Set to ConnTable
	n.ConnTable.Lock()
	n.ConnTable.m[clientID] = clientItem
	n.ConnTable.Unlock()

	// Set to ConnClientTable
	n.ConnClientTable.Lock()
	n.ConnClientTable.c[conn] = clientItem
	n.ConnClientTable.Unlock()

	msg := &message.Authenticated{
		Action:        cstream.Authenticated,
		SessionSecret: sessionSecret.String(),
	}

	return n.msgToByte(msg)
}

// Generates an error message
func (n *Newton) returnError(args ...int) ([]byte, error) {
	code := 0
	if len(args) == 2 {
		code = args[1]
	}
	msg := message.Error{
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
