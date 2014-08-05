package newton

import (
	"errors"
	"net"
	"time"

	"github.com/cstream/newton/cstream"
)

// Creates a new server item on the cluster
func (n *Newton) createOrUpdateServer() error {
	// TODO: remove this if blocks after adding configration checking function.
	if n.Config.Server.Identity == "" {
		return errors.New("Identity is required.")
	}
	if n.Config.Server.Password == "" {
		return errors.New("Password is required.")
	}

	if n.Config.Server.WanIP == "" && n.Config.Server.InternalIP == "" {
		return errors.New("Missing IP address.")
	}

	createServer := func() error {
		// Finally, create a new server on the cluster
		n.ClusterStore.Create(n.Config.Server.Identity, n.Config.Server.Password,
			n.Config.Server.WanIP, n.Config.Server.Port, n.Config.Server.InternalIP)
		return nil
	}

	// Firstly, check the key existence
	server, existed := n.ClusterStore.Get(n.Config.Server.Identity)
	if !existed {
		return createServer()
	}

	if n.Config.Server.WanIP != server.WanIP || n.Config.Server.InternalIP !=
		server.InternalIP || n.Config.Server.Port != server.Port {
		err := n.ClusterStore.Delete(n.Config.Server.Identity)
		if err != nil {
			return err
		}
		return createServer()
	}
	return nil
}

/*
// Deletes a server from cluster
func (n *Newton) deleteServer(data map[string]interface{}) ([]byte, error) {
	identity, ok := data["Identity"].(string)
	if !ok {
		return n.returnError(cstream.DeleteServerFailed, cstream.IdentityRequired)
	}
	err := n.ClusterStore.Delete(identity)
	if err != nil {
		n.Log.Error(err.Error())
		return n.returnError(cstream.DeleteServerFailed, cstream.ServerError)
	}
	return nil, nil
}*/

// Authenticates servers to communicate with each others
func (n *Newton) authenticateServer(data map[string]interface{}) ([]byte, error) {
	identity, ok := data["Identity"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.IdentityRequired)
	}

	n.Log.Info("Received authentication request from %s", identity)
	password, ok := data["Password"].(string)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.PasswordRequired)
	}

	server, ok := n.ClusterStore.Get(identity)
	if !ok {
		return n.returnError(cstream.AuthenticationFailed, cstream.IdentityNotFound)
	}
	// We use identity as clientID for servers
	clientID := identity
	conn := data["Conn"].(*net.Conn)
	response, err := n.authenticateConn(server.Salt, password, clientID, server.Secret, conn)
	return response, err

}

// Sets some basic variables and other things for internal communication between newton instances
func (n *Newton) startInternalCommunication(data map[string]interface{}) {
	// TODO: Check existence
	secret, _ := data["SessionSecret"].(string)
	var identity string
	var value *ServerItem
	conn := data["Conn"].(*net.Conn)

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
