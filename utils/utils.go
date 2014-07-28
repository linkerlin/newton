package utils

import (
	"code.google.com/p/go-uuid/uuid"
)

// NewUUIDv1 returns a new UUID (Version1) as a string.
func NewUUIDv1(args ...string) uuid.UUID {
	// TODO: Manipulate clock sequence
	if len(args) != 0 {
		id := []byte(args[0])
		uuid.SetNodeID(id)
	}
	return uuid.NewUUID()
}
