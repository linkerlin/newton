package newton

import (
	"encoding/json"
	"errors"
	"github.com/purak/newton/message"
	"net"
)

// Creates a new server item on the cluster
func (n *Newton) createServer(data map[string]interface{}) ([]byte, int, error) {
	// Sending the identity and password is a necessity.
	identity, ok := data["Identity"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Identity is required.")
	}
	password, ok := data["Password"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Password is required.")
	}

	// TODO: Handle error conditions for that parameters

	// The following variables is optional but to define a server properly
	// the system admininstrator has to send one of that kind of IP addresses at least
	// WanIP is the external IP address for the server
	wanIp, _ := data["WanIp"].(string)
	wanPort, _ := data["WanPort"].(string)
	// InternalIp is the in-rack/in-datacenter IP address for the server
	internalIp, _ := data["InternalIp"].(string)
	internalPort, _ := data["InternalPort"].(string)

	_, existed := n.ClusterStore.Get(identity)
	if !existed {
		// Finally, create a new server on the cluster
		err := n.ClusterStore.Create(identity, password, wanIp, wanPort, internalIp, internalPort)

		if err != nil {
			return nil, ServerError, err
		}

		msg := &message.Dummy{
			Type:   "Dummy",
			Status: Success,
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

// Removes a server from cluster
func (n *Newton) deleteServer(data map[string]interface{}) ([]byte, int, error) {
	identity, ok := data["Identity"].(string)
	if !ok {
		return nil, BadMessage, errors.New("Identity is required.")
	}
	err := n.ClusterStore.Delete(identity)
	if err != nil {
		return nil, ServerError, err
	}

	msg := &message.Dummy{
		Type:   "Dummy",
		Status: Success,
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, ServerError, err
	}

	return b, Success, nil
}

func (n *Newton) authenticateServer(data map[string]interface{}, conn *net.Conn) ([]byte, int, error) {
	return nil, Success, nil
}
