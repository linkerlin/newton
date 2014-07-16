package newton

import (
	"github.com/purak/newton/message"
	"net"
	"time"
)

// Functions to manipulate and manage ClusterStore data.

// Creates a new server item on the cluster
func (n *Newton) createServer() ([]byte, error) {
	// Sending the identity and password is a necessity.
	/*identity, ok := data["Identity"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Identity is required.")
	}
	password, ok := data["Password"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Password is required.")
	}

	// The following variables is optional but to define a server properly
	// the system admininstrator has to send one of that kind of IP addresses at least.

	// WanIP is the external IP address for the server
	wanIp, _ := data["WanIp"].(string)
	wanPort, _ := data["WanPort"].(string)
	// InternalIp is the in-rack/in-datacenter IP address for the server
	internalIp, _ := data["InternalIp"].(string)
	internalPort, _ := data["InternalPort"].(string)

	if wanIp == "" || wanPort == "" {
		if internalIp == "" || internalPort == "" {
			return nil, BadMessage, errors.New("Missing IP or port data.")
		}
	}*/

	// Fake parameters
	identity := "lpms"
	password := "hadron"
	internalIp := "127.0.0.1"
	internalPort := "8080"
	wanIp := "8.8.8.8"
	wanPort := "5000"

	// Firstly, check the key existence
	_, existed := n.ClusterStore.Get(identity)
	if !existed {
		// Finally, create a new server on the cluster
		err := n.ClusterStore.Create(identity, password, wanIp, wanPort, internalIp, internalPort)

		if err != nil {
			return n.errorMessage(ServerError, err.Error())
		}

		msg := &message.Dummy{
			Action: "Dummy",
			Status: Success,
		}

		return n.msgToByte(msg)
	} else {
		return n.errorMessage(Failed, "Already exist.")
	}
}

// Deletes a server from cluster
func (n *Newton) deleteServer(data map[string]interface{}) ([]byte, error) {
	identity, ok := data["Identity"].(string)
	if !ok {
		return n.errorMessage(BadMessage, "Identity required.")
	}
	err := n.ClusterStore.Delete(identity)
	if err != nil {
		return n.errorMessage(ServerError, err.Error())
	}

	msg := &message.Dummy{
		Action: "Dummy",
		Status: Success,
	}

	return n.msgToByte(msg)
}

// Authenticates servers to communicate with each others
func (n *Newton) authenticateServer(data map[string]interface{}, conn *net.Conn) ([]byte, error) {
	identity, ok := data["Identity"].(string)
	if !ok {
		return n.errorMessage(BadMessage, "Identity doesn't exist or invalid.")
	}
	n.Log.Info("Received authentication request from %s", identity)
	password, ok := data["Password"].(string)
	if !ok {
		return n.errorMessage(BadMessage, "Password doesn't exist or invalid.")
	}
	server, ok := n.ClusterStore.Get(identity)
	if !ok {
		return n.errorMessage(Failed, "Identity could not be found.")
	} else {
		// We use identity as clientId for servers
		clientId := identity
		response, err := n.authenticateConn(server.Salt, password, clientId, server.Secret, conn)
		return response, err
	}
}

// Sets some basic variables and other things for internal communication between newton instances
func (n *Newton) startInternalCommunication(data map[string]interface{}, conn *net.Conn) ([]byte, error) {
	// FIXME: Exception handling?
	status, ok := data["Status"].(int)
	if !ok {
		return n.errorMessage(BadMessage, "Broken authentication message.")
	}
	if status != Success {
		return n.errorMessage(AuthenticationFailed, "Authentication failed.")
	}

	// TODO: Check existence
	secret, _ := data["SessionSecret"].(string)
	var identity string
	var value *ServerItem

	// Find the table item
	n.InternalConnTable.RLock()
	for identity, value = range n.InternalConnTable.i {
		if value.Conn == conn {
			break
		}
	}
	n.InternalConnTable.RUnlock()

	// Set SessionSecret and update the table
	n.InternalConnTable.Lock()
	value.SessionSecret = secret
	n.InternalConnTable.i[identity] = value
	n.InternalConnTable.Unlock()

	n.Log.Info("Session opened on %s", identity)
	// Write outgoing messages to connection
	go n.consumeOutgoingChannel(value.Outgoing, conn)
	// I'm alive
	go n.heartbeat(value.Outgoing)
	msg := &message.Dummy{
		Action: "Dummy",
		Status: Success,
	}
	return n.msgToByte(msg)
}

// Consume outgoing messages channel for internal connections
func (n *Newton) consumeOutgoingChannel(outgoing chan []byte, conn *net.Conn) {
	for {
		select {
		case buff := <-outgoing:
			(*conn).Write(buff)
		}
	}
}

// I'm alive function
func (n *Newton) heartbeat(outgoing chan []byte) {
	tick := time.NewTicker(4 * time.Second)
	defer tick.Stop()
	// Empty message
	hb := make([]byte, 1)
	for {
		select {
		case <-tick.C:
			outgoing <- hb
		}
	}
}
