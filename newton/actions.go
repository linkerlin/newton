package newton

import (
	"encoding/json"
	"errors"
	"github.com/nu7hatch/gouuid"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/message"
	"github.com/purak/newton/store"
	"net"
	"time"
)

// Create a new client session
func (n *Newton) createSession(data map[string]interface{}, conn *net.Conn) ([]byte, error) {
	clientId, ok := data["ClientId"].(string)
	if !ok {
		return nil, errors.New("ClientId doesn't exist or invalid.")
	}

	remoteAddr := (*conn).RemoteAddr().String()
	clientIp, err := cstream.ParseIP(remoteAddr)
	if err != nil {
		return nil, err
	}

	// AUTHENTICATION HERE

	now := time.Now().Unix()
	// Go's maps are not thread-safe
	n.ConnTable.RLock()
	_, ok = n.ConnTable.m[clientId]
	n.ConnTable.RUnlock()

	if ok {
		// Remove the previous connection
		n.ConnClientTable.Lock()
		delete(n.ConnClientTable.c, conn)
		n.ConnClientTable.Unlock()

		n.ConnTable.Lock()
		delete(n.ConnTable.m, clientId)
		n.ConnTable.Unlock()

		return nil, errors.New("Client has an active connection.")
	}

	// Create a new clientItem
	expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
	secret, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}

	clientItem := &ClientItem{
		Ip:            clientIp,
		LastAnnounce:  now,
		SessionSecret: secret.String(),
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
		Type:          "Authenticated",
		SessionSecret: secret.String(),
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return nil, errors.New("Username is required.")
	}

	// Check password
	password, ok := data["Password"].(string)
	if !ok {
		return nil, errors.New("Password is required.")
	}

	_, existed := n.UserStore.Get(username)
	if !existed {
		// Finally, create a new user
		err := n.UserStore.Create(username, password)

		if err != nil {
			return nil, err
		}

		clientId, err := n.UserStore.CreateUserClient(username)
		if err != nil {
			return nil, err
		}

		msg := &message.ClientId{
			Type:     "ClientId",
			ClientId: clientId,
		}

		// FIXME: Remove boilterplate code
		b, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}

		return b, nil
	} else {
		return nil, errors.New("Already exist.")
	}
}

// Creates a new client for the user
func (n *Newton) createUserClient(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return nil, errors.New("Username is required.")
	}

	clients := n.UserStore.GetUserClient(username)
	if len(clients) >= n.Config.Database.MaxUserClient {
		return nil, errors.New("MaxUserClient limit exceeded.")
	}

	clientId, err := n.UserStore.CreateUserClient(username)
	if err != nil {
		return nil, err
	}

	msg := &message.ClientId{
		Type:     "ClientId",
		ClientId: clientId,
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return b, nil
}
