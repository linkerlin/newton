// Copyright 2015 Burak Sezer
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

// Action codes
const (
	PeerHeartbeat uint8 = 1 + iota
	AnnounceClientItem
	NewPeer
	Seed
	LookupUserHash
	DeleteClient
	DeleteNode
)

// Acknowledge codes
const (
	NewPeerSuccess uint8 = 127 + iota
	SeedSuccess
	LookupUserHashSuccess
)

// Protocol messages

// NewPeerMsg represents a message for adding a new peer in unicast mode.
type NewPeerMsg struct {
	Action     uint8
	Identifier string
	NodeID     string // Current NodeID
}

// NewPeerSuccessMsg represents a message that's returned as a response for NewPeerMsg
type NewPeerSuccessMsg struct {
	Action     uint8
	Identifier string
	NodeID     string
}

// PeerHeartbeatMsg represents heartbeat message.
type PeerHeartbeatMsg struct {
	Action uint8
	NodeID string
}

type AnnounceClientItemMsg struct {
	Action   uint8
	ClientID uint32
	UserHash string
	NodeID   string
	Addr     string
}

type SeedMsg struct {
	Action uint8
	NodeID string
}

type SeedSuccessMsg struct {
	Action uint8
	NodeID string
	Addr   string
}

// LookupUserHashMsg represents a message that
type LookupUserHashMsg struct {
	Action   uint8
	NodeID   string
	UserHash string
}

type LookupUserHashSuccessMsg struct {
	Action   uint8
	NodeID   string
	UserHash string
	Clients  []UserClientItem
}

// UserClientItem represents a data structure that ships all the information of a client in the DHT.
type UserClientItem struct {
	NodeID   string
	ClientID uint32
	Addr     string
}

type DeleteClientMsg struct {
	Action   uint8
	ClientID uint32
	UserHash string
	NodeID   string
}

// DeleteNode
type DeleteNodeMsg struct {
	Action uint8
	NodeID string
}
