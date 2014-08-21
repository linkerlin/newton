package newton

import (
	"net"

	"github.com/cstream/newton/comm"
	"github.com/cstream/newton/cstream"
)

// Authenticate and create a new client session for the client
func (n *Newton) authenticateUser(data map[string]interface{}) ([]byte, error) {
	clientID, ok := data["ClientID"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.ClientIDRequired)
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
	}
	existed := n.UserStore.CheckUserClient(username, clientID)
	if existed {
		conn := data["Conn"].(*net.Conn)
		res, err := n.authenticateConn(user.Salt, password, clientID, user.Secret, conn)
		if err == nil {
			n.UserStore.SetClientHost(username, clientID, n.Config.Server.Identity)
		}
		return res, err
	}
	return n.returnError(cstream.AuthenticationFailed, cstream.ClientIDNotFound)

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
		clientID := n.UserStore.CreateUserClient(username)
		msg := &comm.ClientID{
			Action:   cstream.SetClientID,
			ClientID: clientID,
		}
		return n.msgToByte(msg)
	}
	return n.returnError(cstream.AuthenticationFailed, cstream.AlredyExist)
}

// Creates a new client for the user.
func (n *Newton) createUserClient(data map[string]interface{}) ([]byte, error) {
	// Check username
	username, ok := data["Username"].(string)
	if !ok {
		return n.returnError(cstream.CreateUserClientFailed, cstream.UsernameRequired)
	}

	clients := n.UserStore.GetUserClients(username)
	if len(clients) >= n.Config.Database.MaxUserClient {
		return n.returnError(cstream.CreateUserClientFailed, cstream.ThresholdExceeded)
	}

	clientID := n.UserStore.CreateUserClient(username)

	msg := &comm.ClientID{
		Action:   cstream.SetClientID,
		ClientID: clientID,
	}
	return n.msgToByte(msg)
}

// Triggers the tracker to find the user's currently opened sessions on the cluster.
func (n *Newton) lookupUser(data map[string]interface{}) ([]byte, error) {
	// Check username
	usernames, ok := data["Usernames"].([]string)
	// FIXME: Do we need a database request to check that user is real?
	if !ok {
		return n.returnError(cstream.LookupUserFailed, cstream.UsernameRequired)
	}
	if len(usernames) > cstream.MaxUsernameCount {
		return n.returnError(cstream.LookupUserFailed, cstream.ThresholdExceeded)
	}
	// Dont block the request
	n.manageRoutingTable(usernames)

	msg := &comm.Success{
		Action: cstream.LookupUserSuccess,
	}
	return n.msgToByte(msg)
}
