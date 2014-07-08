package newton

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nu7hatch/gouuid"
	"github.com/purak/gauss/gconn" // Client library for Gauss"
	"github.com/purak/gauss/murmur"
	"github.com/purak/newton/cstream"
	"github.com/purak/newton/message"
	"github.com/purak/newton/store"
	"net"
	"time"
)

// Authenticate and create a new client session for the client
func (n *Newton) authenticate(data map[string]interface{}, conn *net.Conn) ([]byte, int, error) {
	clientId, ok := data["ClientId"].(string)
	if !ok {
		return nil, BadMessage, errors.New("ClientId doesn't exist or invalid.")
	}

	username, ok := data["Username"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Username doesn't exist or invalid.")
	}

	password, ok := data["Password"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Password doesn't exist or invalid.")
	}

	remoteAddr := (*conn).RemoteAddr().String()
	clientIp, err := cstream.ParseIP(remoteAddr)
	if err != nil {
		return nil, ServerError, err
	}

	// AUTHENTICATION HERE
	user, ok := n.UserStore.Get(username)
	if !ok {
		return nil, Failed, errors.New("User could not be found.")
	} else {
		existed := n.UserStore.CheckUserClient(username, clientId)
		if existed {
			tmp := user.Salt + password
			secret := murmur.HashString(tmp)
			if !bytes.Equal(secret, user.Secret) {
				return nil, AuthenticationFailed, errors.New("Authentication Failed")
			}

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

				return nil, Failed, errors.New("Client has an active connection.")
			}

			// Create a new clientItem
			expireAt := time.Now().Unix() + n.Config.Server.ClientAnnounceInterval
			ss, err := uuid.NewV4()
			if err != nil {
				return nil, ServerError, err
			}

			clientItem := &ClientItem{
				Ip:            clientIp,
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
				Type:          "Authenticated",
				Status:        Success,
				SessionSecret: ss.String(),
			}

			// FIXME: Remove boilerplate code
			b, err := json.Marshal(msg)
			if err != nil {
				return nil, ServerError, err
			}

			// Event Listening experient
			r := n.UserStore.Conn.RegisterListener()
			r.Put(gconn.Key("bar"), "foo")
			h, _ := r.Get(gconn.Key("bar"))
			fmt.Println(h)

			return b, Success, nil
		} else {
			return nil, Failed, errors.New("ClientId could not be found.")
		}
	}
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) ([]byte, int, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Username is required.")
	}

	// Check password
	password, ok := data["Password"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Password is required.")
	}

	_, existed := n.UserStore.Get(username)
	if !existed {
		// Finally, create a new user
		err := n.UserStore.Create(username, password)

		if err != nil {
			return nil, ServerError, err
		}

		clientId, err := n.UserStore.CreateUserClient(username)
		if err != nil {
			return nil, ServerError, err
		}

		msg := &message.ClientId{
			Type:     "ClientId",
			Status:   Success,
			ClientId: clientId,
		}

		// FIXME: Remove boilterplate code
		b, err := json.Marshal(msg)
		if err != nil {
			return nil, ServerError, err
		}

		return b, Success, nil
	} else {
		return nil, Failed, errors.New("Already exist.")
	}
}

// Creates a new client for the user
func (n *Newton) createUserClient(data map[string]interface{}) ([]byte, int, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Username is required.")
	}

	clients := n.UserStore.GetUserClients(username)
	if len(clients) >= n.Config.Database.MaxUserClient {
		return nil, Failed, errors.New("MaxUserClient limit exceeded.")
	}

	clientId, err := n.UserStore.CreateUserClient(username)
	if err != nil {
		return nil, ServerError, err
	}

	msg := &message.ClientId{
		Type:     "ClientId",
		Status:   Success,
		ClientId: clientId,
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, ServerError, err
	}

	return b, Success, nil
}
