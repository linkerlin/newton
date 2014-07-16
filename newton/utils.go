package newton

import (
	"bytes"
	"encoding/json"
	"github.com/nu7hatch/gouuid"
	"github.com/purak/gauss/murmur"
	"github.com/purak/newton/message"
	"github.com/purak/newton/store"
	"net"
	"time"
)

// Check password and create and SessionSecret for authenticate the client
func (n *Newton) authenticateConn(salt, password, clientId string, secret []byte, conn *net.Conn) ([]byte, error) {
	// Recreate the secret and compare
	tmpSecret := murmur.HashString(salt + password)
	if !bytes.Equal(secret, tmpSecret) {
		return n.errorMessage(AuthenticationFailed, "Authentication Failed")
	}

	// Authentication is done after that time

	// Go's maps are not thread-safe
	n.ConnTable.RLock()
	_, ok := n.ConnTable.m[clientId]
	n.ConnTable.RUnlock()

	if ok {
		// FIXME: Remove the previous connection, why?
		n.ConnClientTable.Lock()
		delete(n.ConnClientTable.c, conn)
		n.ConnClientTable.Unlock()

		n.ConnTable.Lock()
		delete(n.ConnTable.m, clientId)
		n.ConnTable.Unlock()
		return n.errorMessage(Failed, "Client has an active connection.")
	}

	// Create a new clientItem
	expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
	ss, err := uuid.NewV4()
	if err != nil {
		return n.errorMessage(ServerError, err.Error())
	}

	now := time.Now().Unix()
	clientItem := &ClientItem{
		LastAnnounce:  now,
		SessionSecret: ss.String(),
		Conn:          conn,
	}

	// Add a new item to priority queue
	item := &store.Item{
		Value: clientId,
		TTL:   expireAt,
	}
	n.ClientQueue <- item

	// Set to ConnTable
	n.ConnTable.Lock()
	n.ConnTable.m[clientId] = clientItem
	n.ConnTable.Unlock()

	// Set to ConnClientTable
	n.ConnClientTable.Lock()
	n.ConnClientTable.c[conn] = clientItem
	n.ConnClientTable.Unlock()

	msg := &message.Authenticated{
		Action:        "Authenticated",
		Status:        Success,
		SessionSecret: ss.String(),
	}

	return n.msgToByte(msg)
}

func (n *Newton) errorMessage(status int, body string) ([]byte, error) {
	msg := message.Dummy{
		Action: "Error",
		Status: status,
		Body:   body,
	}
	return n.msgToByte(msg)
}

func (n *Newton) msgToByte(msg interface{}) ([]byte, error) {
	r, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	return r, nil
}
