package message

// Defines a message for SessionSecret
type Authenticated struct {
	Type          string
	SessionSecret string
}

// Defines a message for ClientId
type ClientId struct {
	Type     string
	ClientId []byte
}

// Defines an error message
type ErrorMsg struct {
	Type string
	Body string
}
