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

// Operation Codes are defined here

// 2xx => Success codes
// 1xx => Error codes
const (
	Failed                 = 100 // Generic fail code
	AuthenticationFailed   = 101
	AuthenticationRequired = 102
	ServerError            = 103
	BadMessage             = 104 // Broken message
)

const (
	Success            = 200 // Generic success code
	Authenticated      = 201
	AuthenticateUser   = 202
	AuthenticateServer = 203
	DeleteServer       = 204
	CreateUser         = 205
	CreateUserClient   = 206
	GetClientId        = 207
)
