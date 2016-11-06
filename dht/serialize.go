// Copyright 2014-2016 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dht

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
)

func serializeUserClientItem(msg UserClientItem) ([]byte, error) {
	n, err := hex.DecodeString(msg.NodeID)
	if err != nil {
		return nil, err
	}

	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, msg.ClientID)

	addr := []byte(msg.Addr)
	res := make([]byte, 24+len(addr))
	copy(res[:20], n)
	copy(res[20:24], bs)
	copy(res[24:], addr)
	return res, nil
}

func serializeSeedSuccess(msg SeedSuccessMsg) ([]byte, error) {
	p, err := serializeActionNodeID(msg.Action, msg.NodeID)
	if err != nil {
		return nil, err
	}
	lp := len(p)
	addr := []byte(msg.Addr)
	res := make([]byte, len(addr)+lp)
	res[0] = msg.Action
	copy(res[1:lp], p)
	copy(res[lp:], addr)
	return res, nil
}

func serializeActionNodeID(action uint8, nodeID string) ([]byte, error) {
	res := make([]byte, 21)
	res[0] = action
	n, err := hex.DecodeString(nodeID)
	if err != nil {
		return nil, err
	}
	copy(res[1:], n)
	return res, nil
}

func serializeActionNodeIDUserHash(action uint8, nodeID, userHash string) ([]byte, error) {
	p, err := serializeActionNodeID(action, nodeID)
	if err != nil {
		return nil, err
	}
	u, err := hex.DecodeString(userHash)
	if err != nil {
		return nil, err
	}
	res := make([]byte, 41)
	copy(res[:21], p)
	copy(res[21:], u)
	return res, nil
}

func serializeDeleteClient(msg DeleteClientMsg) ([]byte, error) {
	p, err := serializeActionNodeIDUserHash(msg.Action, msg.NodeID, msg.UserHash)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, msg.ClientID)
	res := make([]byte, len(bs)+len(p))
	copy(res[:41], p)
	copy(res[41:45], bs)
	return res, nil
}

func serializeAnnounceUserClientItem(msg AnnounceClientItemMsg) ([]byte, error) {
	p, err := serializeActionNodeIDUserHash(msg.Action, msg.NodeID, msg.UserHash)
	if err != nil {
		return nil, err
	}
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, msg.ClientID)
	addr := []byte(msg.Addr)
	res := make([]byte, 45+len(addr))
	copy(res[:41], p)
	copy(res[41:45], bs)
	copy(res[45:], addr)
	return res, nil
}

func serializeNewPeerMessages(action uint8, identifier, nodeID string) ([]byte, error) {
	res := make([]byte, 37)
	res[0] = action

	idt, err := hex.DecodeString(identifier)
	if err != nil {
		return nil, err
	}
	copy(res[1:17], idt)

	n, err := hex.DecodeString(nodeID)
	if err != nil {
		return nil, err
	}
	copy(res[17:], n)
	return res, nil
}

type container struct {
	data []byte
}

func serializeLookupUserHashSuccess(msg LookupUserHashSuccessMsg) ([]byte, error) {
	p, err := serializeActionNodeIDUserHash(msg.Action, msg.NodeID, msg.UserHash)
	if err != nil {
		return nil, err
	}
	var (
		containers []container
		size       int
	)
	for _, client := range msg.Clients {
		c, err := serializeUserClientItem(client)
		if err != nil {
			return nil, err
		}

		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(len(c)))
		rc := make([]byte, len(c)+4)
		copy(rc[:4], bs)
		copy(rc[4:], c)
		size = size + len(rc)
		containers = append(containers, container{data: rc})
	}
	lp := len(p)
	res := make([]byte, size+lp)
	copy(res[:lp], p)
	cr := lp
	for _, item := range containers {
		copy(res[cr:cr+len(item.data)], item.data)
		cr = cr + len(item.data)
	}
	return res, nil
}

func serializeDeleteNode(msg DeleteNodeMsg) ([]byte, error) {
	res, err := serializeActionNodeID(msg.Action, msg.NodeID)
	if err != nil {
		return nil, err
	}
	return res, nil
}

var errInvalidMessage = errors.New("Invalid message")

// serialize converts a protocol message into bytes array.
func serialize(message interface{}) ([]byte, error) {
	switch message.(type) {
	case PeerHeartbeatMsg:
		msg := message.(PeerHeartbeatMsg)
		return serializeActionNodeID(msg.Action, msg.NodeID)
	case SeedMsg:
		msg := message.(SeedMsg)
		return serializeActionNodeID(msg.Action, msg.NodeID)
	case NewPeerMsg:
		msg := message.(NewPeerMsg)
		return serializeNewPeerMessages(msg.Action, msg.Identifier, msg.NodeID)
	case NewPeerSuccessMsg:
		msg := message.(NewPeerSuccessMsg)
		return serializeNewPeerMessages(msg.Action, msg.Identifier, msg.NodeID)
	case SeedSuccessMsg:
		msg := message.(SeedSuccessMsg)
		return serializeSeedSuccess(msg)
	case LookupUserHashMsg:
		msg := message.(LookupUserHashMsg)
		return serializeActionNodeIDUserHash(msg.Action, msg.NodeID, msg.UserHash)
	case AnnounceClientItemMsg:
		msg := message.(AnnounceClientItemMsg)
		return serializeAnnounceUserClientItem(msg)
	case LookupUserHashSuccessMsg:
		msg := message.(LookupUserHashSuccessMsg)
		return serializeLookupUserHashSuccess(msg)
	case DeleteClientMsg:
		msg := message.(DeleteClientMsg)
		return serializeDeleteClient(msg)
	case DeleteNodeMsg:
		msg := message.(DeleteNodeMsg)
		return serializeDeleteNode(msg)
	default:
		return nil, errInvalidMessage
	}
}
