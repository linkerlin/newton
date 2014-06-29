package message

// Defines a message for SessionSecret
type Authenticated struct {
	Type          string
	Status        int
	SessionSecret string
}

// Defines a message for ClientId
type ClientId struct {
	Type     string
	Status   int
	ClientId []byte
}

// Defines an error message
type ErrorMsg struct {
	Type   string
	Status int
	Body   string
}
