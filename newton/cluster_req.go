package newton

import (
	"github.com/purak/newton/message"
	"net"
)

// Functions to manipulate ClusterStore data on a remote server

// Sends a message to create a new server
func (n *Newton) createServerReq(conn *net.Conn) {
	// Dummy values for test
	/*msg := &message.CreateServer{
		Action:         "CreateServer",
		Idendity:     "lpms",
		Password:     "hadron",
		InternalIp:   "127.0.0.1", // Inbound interface IP, for only rack-aware setups
		InternalPort: "8888",      // Inbound port
	}*/
	//n.createServer(msg)
}

// Sends a message to delete the server from cluster
func (n *Newton) deleteServerReq(identity string) (interface{}, int, error) {
	// Dummy values for test
	msg := &message.DeleteServer{
		Action:   "DeleteServer",
		Identity: identity,
	}

	return msg, Success, nil
}

// Sends a message for authentication
func (n *Newton) authenticateServerReq(identity, password string, conn *net.Conn) {
	msg := &message.AuthenticateServer{
		Action:   "AuthenticateServer",
		Identity: identity,
		Password: password,
	}
	n.writeMessage(msg, conn)
}
