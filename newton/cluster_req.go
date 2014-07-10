package newton

import (
	"encoding/json"
	"github.com/purak/newton/message"
)

// Functions to manipulate ClusterStore data on a remote server

// Sends a message to create a new server
func (n *Newton) newServerReq() ([]byte, int, error) {
	// Dummy values for test
	msg := &message.CreateServer{
		Type:         "CreateServer",
		Idendity:     "lpms",
		Password:     "hadron",
		InternalIp:   "127.0.0.1", // Inbound interface IP, for only rack-aware setups
		InternalPort: "8888",      // Inbound port
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, ServerError, err
	}

	return b, Success, nil
}

// Sends a message to delete the server from cluster
func (n *Newton) deleteServerReq() ([]byte, int, error) {
	// Dummy values for test
	msg := &message.DeleteServer{
		Type:     "DeleteServer",
		Identity: "lpms",
	}

	// FIXME: Remove boilterplate code
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, ServerError, err
	}

	return b, Success, nil
}