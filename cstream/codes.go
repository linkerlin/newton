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
	SetClientID
	PostMessage
	DeleteMessage
	LookupUser
	UserOnline
)

// Success Codes
const (
	PostMessageSuccess int = 200 + iota
	DeleteMessageSuccess
	LookupUserSuccess
)

// Fail codes for actions
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
	LookupUserFailed
)

// More spesific fail codes
const (
	ServerError int = 400 + iota
	IdentityRequired
	UsernameRequired
	PasswordRequired
	IdentityNotFound
	UsernameNotFound
	ClientIDNotFound
	ActionRequired
	HasAnotherConnection
	ThresholdExceeded
	AlredyExist
	ClientIDRequired
	UnknownAction
)

// Action codes for tracker
const (
	TrackUser int = 500 + iota
	AnnounceUser
)
