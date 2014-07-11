package newton

import (
	"errors"
	"github.com/purak/newton/message"
	"net"
)

// Authenticate and create a new client session for the client
func (n *Newton) authenticateUser(data map[string]interface{}, conn *net.Conn) (interface{}, int, error) {
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

	// Start authentication
	user, ok := n.UserStore.Get(username)
	if !ok {
		return nil, Failed, errors.New("User could not be found.")
	} else {
		existed := n.UserStore.CheckUserClient(username, clientId)
		if existed {
			return n.authenticateConn(user.Salt, password, clientId, user.Secret, conn)
		} else {
			return nil, Failed, errors.New("ClientId could not be found.")
		}
	}
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) (interface{}, int, error) {
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

		return msg, Success, nil
	} else {
		return nil, Failed, errors.New("Already exist.")
	}
}

// Creates a new client for the user
func (n *Newton) createUserClient(data map[string]interface{}) (interface{}, int, error) {
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

	return msg, Success, nil
}
