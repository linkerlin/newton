package newton

import (
	"github.com/purak/newton/message"
	"net"
)

// Authenticate and create a new client session for the client
func (n *Newton) authenticateUser(data map[string]interface{}, conn *net.Conn) ([]byte, error) {
	clientId, ok := data["ClientId"].(string)
	if !ok {
		return n.returnError(BadMessage, "ClientId doesn't exist or invalid.")
	}

	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(BadMessage, "Username doesn't exist or invalid.")
	}

	password, ok := data["Password"].(string)
	if !ok {
		return n.returnError(BadMessage, "Password doesn't exist or invalid.")
	}

	// Start authentication
	user, ok := n.UserStore.Get(username)
	if !ok {
		return n.returnError(BadMessage, "User could not be found.")
	} else {
		existed := n.UserStore.CheckUserClient(username, clientId)
		if existed {
			return n.authenticateConn(user.Salt, password, clientId, user.Secret, conn)
		} else {
			return n.returnError(BadMessage, "ClientId could not be found.")
		}
	}
}

// Creates a new user
func (n *Newton) createUser(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(BadMessage, "Username is required.")
	}

	// Check password
	password, ok := data["Password"].(string)
	if !ok {
		return n.returnError(BadMessage, "Password is required.")
	}

	_, existed := n.UserStore.Get(username)
	if !existed {
		// Finally, create a new user
		err := n.UserStore.Create(username, password)

		if err != nil {
			return n.returnError(ServerError, err.Error())
		}

		clientId, err := n.UserStore.CreateUserClient(username)
		if err != nil {
			return n.returnError(ServerError, err.Error())
		}

		msg := &message.ClientId{
			Action:   "ClientId",
			Status:   Success,
			ClientId: clientId,
		}
		return n.msgToByte(msg)
	} else {
		return n.returnError(Failed, "Already Exist.")
	}
}

// Creates a new client for the user
func (n *Newton) createUserClient(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(BadMessage, "Username is required.")
	}

	clients := n.UserStore.GetUserClients(username)
	if len(clients) >= n.Config.Database.MaxUserClient {
		return n.returnError(Failed, "MaxUserClient limit exceeded.")
	}

	clientId, err := n.UserStore.CreateUserClient(username)
	if err != nil {
		return n.returnError(ServerError, err.Error())
	}

	msg := &message.ClientId{
		Action:   n.Actions.ClientId,
		ClientId: clientId,
	}
	return n.msgToByte(msg)
}
