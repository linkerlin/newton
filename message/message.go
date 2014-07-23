package message

// Defines a message for SessionSecret
type Authenticated struct {
	Action        int
	SessionSecret string
}

// Defines a message for ClientId
type ClientId struct {
	Action   int
	ClientId string
}

// Defines an error message
type Error struct {
	Action int
	Code   int
}

// Defines a success message
type Success struct {
	Action int
}

// Defines an authentication request message between newton servers
type AuthenticateServer struct {
	Action   int
	Identity string
	Password string
}

// Defines a membership message for newton servers
type CreateServer struct {
	Action       int
	Idendity     string
	Password     string
	WanIp        string // Outbound interface IP
	WanPort      string // Outbound port
	InternalIp   string // Inbound interface IP, for only rack-aware setups
	InternalPort string // Inbound port
}

// Defines a message for deleting a server
type DeleteServer struct {
	Action   int
	Identity string
}
