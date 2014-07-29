package newton

import (
	"net"

	"github.com/cstream/newton/cstream"
	"github.com/cstream/newton/message"
)

// Authenticate and create a new client session for the client
func (n *Newton) authenticateUser(data map[string]interface{}) ([]byte, error) {
	clientId, ok := data["ClientId"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.ClientIdRequired)
	}

	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.UsernameRequired)
	}

	password, ok := data["Password"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.PasswordRequired)
	}

	// Start authentication
	user, ok := n.UserStore.Get(username)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.UsernameNotFound)
	} else {
		existed := n.UserStore.CheckUserClient(username, clientId)
		if existed {
			conn := data["Conn"].(*net.Conn)
			return n.authenticateConn(user.Salt, password, clientId, user.Secret, conn)
		} else {
			return n.returnError(cstream.AuthenticationFailed, cstream.ClientIdNotFound)
		}
	}
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.UsernameRequired)
	}

	// Check password
	password, ok := data["Password"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.PasswordRequired)
	}

	_, existed := n.UserStore.Get(username)
	if !existed {
		// Finally, create a new user
		n.UserStore.Create(username, password)
		clientId := n.UserStore.CreateUserClient(username)
		msg := &message.ClientId{
			Action:   cstream.SetClientId,
			ClientId: clientId,
		}
		return n.msgToByte(msg)
	} else {
		return n.returnError(cstream.AuthenticationFailed, cstream.AlredyExist)
	}
}

// Creates a new client for the user
func (n *Newton) createUserClient(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(cstream.CreateUserClientFailed, cstream.UsernameRequired)
	}

	clients := n.UserStore.GetUserClients(username)
	if len(clients) >= n.Config.Database.MaxUserClient {
		return n.returnError(cstream.CreateUserClientFailed, cstream.MaxClientCountExceeded)
	}

	clientId := n.UserStore.CreateUserClient(username)

	msg := &message.ClientId{
		Action:   cstream.SetClientId,
		ClientId: clientId,
	}
	return n.msgToByte(msg)
}
