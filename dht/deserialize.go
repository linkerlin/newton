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
	"bytes"
	"encoding/binary"
	"encoding/hex"
)

func deserializePeerHearbeatMsg(data []byte) (PeerHeartbeatMsg, error) {
	p := PeerHeartbeatMsg{}
	if len(data) != 21 {
		return p, errInvalidMessage
	}
	p.Action = PeerHeartbeat
	p.NodeID = hex.EncodeToString(data[1:])
	return p, nil
}

func deserializeSeedMsg(data []byte) (SeedMsg, error) {
	s := SeedMsg{}
	if len(data) != 21 {
		return s, errInvalidMessage
	}
	s.Action = Seed
	s.NodeID = hex.EncodeToString(data[1:])
	return s, nil
}

func deserializeNewPeerMsg(data []byte) (NewPeerMsg, error) {
	n := NewPeerMsg{}
	if len(data) != 37 {
		return n, errInvalidMessage
	}
	n.Action = NewPeer
	n.Identifier = hex.EncodeToString(data[1:17])
	n.NodeID = hex.EncodeToString(data[17:])
	return n, nil
}

func deserializeNewPeerSuccessMsg(data []byte) (NewPeerSuccessMsg, error) {
	ns := NewPeerSuccessMsg{}
	if len(data) != 37 {
		return ns, errInvalidMessage
	}
	ns.Action = NewPeerSuccess
	ns.Identifier = hex.EncodeToString(data[1:17])
	ns.NodeID = hex.EncodeToString(data[17:])
	return ns, nil
}

func deserializeSeedSuccessMsg(data []byte) (SeedSuccessMsg, error) {
	ss := SeedSuccessMsg{}
	ss.Action = SeedSuccess
	ss.NodeID = hex.EncodeToString(data[1:21])
	ss.Addr = string(data[21:])
	return ss, nil
}

func deserializeLookupUserHashMsg(data []byte) (LookupUserHashMsg, error) {
	p := LookupUserHashMsg{}
	if len(data) != 41 {
		return p, errInvalidMessage
	}
	p.Action = LookupUserHash
	p.NodeID = hex.EncodeToString(data[1:21])
	p.UserHash = hex.EncodeToString(data[21:])
	return p, nil
}

func deserializeAnnounceUserClientItemMsg(data []byte) (AnnounceClientItemMsg, error) {
	p := AnnounceClientItemMsg{}
	p.Action = AnnounceClientItem
	p.NodeID = hex.EncodeToString(data[1:21])
	p.UserHash = hex.EncodeToString(data[21:41])

	var clientID uint32
	cl := data[41:45]
	buf := bytes.NewBuffer(cl)
	if err := binary.Read(buf, binary.BigEndian, &clientID); err != nil {
		return p, err
	}
	p.ClientID = clientID

	p.Addr = string(data[45:])
	return p, nil
}

func deserializeDeleteClientMsg(data []byte) (DeleteClientMsg, error) {
	p := DeleteClientMsg{}
	p.Action = DeleteClient
	p.NodeID = hex.EncodeToString(data[1:21])
	p.UserHash = hex.EncodeToString(data[21:41])
	var clientID uint32
	cl := data[41:]
	buf := bytes.NewBuffer(cl)
	if err := binary.Read(buf, binary.BigEndian, &clientID); err != nil {
		return p, err
	}
	p.ClientID = clientID
	return p, nil
}

func deserializeUserClientItem(data []byte) (UserClientItem, error) {
	u := UserClientItem{}
	u.NodeID = hex.EncodeToString(data[0:20])
	var clientID uint32
	cl := data[20:24]
	buf := bytes.NewBuffer(cl)
	if err := binary.Read(buf, binary.BigEndian, &clientID); err != nil {
		return u, err
	}
	u.ClientID = clientID
	u.Addr = string(data[24:])
	return u, nil
}

func deserializeLookupUserHashSuccessMsg(data []byte) (LookupUserHashSuccessMsg, error) {
	ls := LookupUserHashSuccessMsg{}
	ls.Action = LookupUserHashSuccess
	ls.NodeID = hex.EncodeToString(data[1:21])
	ls.UserHash = hex.EncodeToString(data[21:41])
	clients := data[41:]
	var cls []UserClientItem
	for len(clients) != 0 {
		d := clients[0:4]
		var dl uint32
		buf := bytes.NewBuffer(d)
		if err := binary.Read(buf, binary.BigEndian, &dl); err != nil {
			return ls, err
		}
		cl := clients[4 : dl+4]
		u, err := deserializeUserClientItem(cl)
		if err != nil {
			return ls, err
		}
		cls = append(cls, u)
		clients = clients[dl+4:]
	}
	ls.Clients = cls
	return ls, nil
}

func deserializeDeleteNodeMsg(data []byte) (DeleteNodeMsg, error) {
	d := DeleteNodeMsg{}
	if len(data) != 21 {
		return d, errInvalidMessage
	}
	d.Action = DeleteNode
	d.NodeID = hex.EncodeToString(data[1:])
	return d, nil
}

// deserialize extracts a protocol message from raw socket data.
func deserialize(data []byte) (interface{}, error) {
	typ := data[0]
	switch {
	case typ == PeerHeartbeat:
		return deserializePeerHearbeatMsg(data)
	case typ == Seed:
		return deserializeSeedMsg(data)
	case typ == NewPeer:
		return deserializeNewPeerMsg(data)
	case typ == NewPeerSuccess:
		return deserializeNewPeerSuccessMsg(data)
	case typ == SeedSuccess:
		return deserializeSeedSuccessMsg(data)
	case typ == LookupUserHash:
		return deserializeLookupUserHashMsg(data)
	case typ == AnnounceClientItem:
		return deserializeAnnounceUserClientItemMsg(data)
	case typ == LookupUserHashSuccess:
		return deserializeLookupUserHashSuccessMsg(data)
	case typ == DeleteClient:
		return deserializeDeleteClientMsg(data)
	case typ == DeleteNode:
		return deserializeDeleteNodeMsg(data)
	}
	return nil, errInvalidMessage
}
