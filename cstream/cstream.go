package cstream

import (
	"net"
)

// Index of rightmost occurrence of b in s.
func Last(s string, b byte) int {
	i := len(s)
	for i--; i >= 0; i-- {
		if s[i] == b {
			break
		}
	}
	return i
}

// Removes port from a network address of the form "host:port"
func ParseIP(rawIP string) (string, error) {
	i := Last(rawIP, ':')
	if i < 0 {
		return rawIP, nil
	} else {
		ip, _, err := net.SplitHostPort(rawIP)
		if err != nil {
			return "", nil
		}
		return ip, nil
	}
}

// Action codes are defined here

// Success codes
const (
	Authenticated int = 100 + iota
	AuthenticateUser
	AuthenticateServer
	CreateServer
	DeleteServer
	CreateUser
	CreateUserClient
	GetClientId // Rename this
	PostMessage
	PostMessageSuccess
	DeleteMessage
	DeleteMessageSuccess
)

// Fail codes
const (
	AuthenticationFailed int = 200 + iota
	AuthenticationRequired
	CreateServerFailed
	DeleteServerFailed
	CreateUserFailed
	CreateUserClientFailed
	PostMessageFailed
	DeleteMessageFailed
	BadMessage
)

// More spesific fail codes
const (
	ServerError int = 300 + iota
	IdentityRequired
	UsernameRequired
	PasswordRequired
	ActionRequired
	IdentityNotFound
	UsernameNotFound
	ClientIdNotFound
	HasAnotherConnection
	MaxClientCountExceeded
	AlredyExist
	ClientIdRequired
)
