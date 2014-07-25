package cstream

// Action codes
const (
	Authenticated int = 100 + iota
	AuthenticateUser
	AuthenticateServer
	CreateServer
	DeleteServer
	CreateUser
	CreateUserClient
	SetClientId
	PostMessage
	DeleteMessage
)

// Success Codes
const (
	PostMessageSuccess int = 200 + iota
	DeleteMessageSuccess
)

// Fail codes
const (
	AuthenticationFailed int = 300 + iota
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
	ServerError int = 400 + iota
	IdentityRequired
	UsernameRequired
	PasswordRequired
	IdentityNotFound
	UsernameNotFound
	ClientIdNotFound
	ActionRequired
	HasAnotherConnection
	MaxClientCountExceeded
	AlredyExist
	ClientIdRequired
)
