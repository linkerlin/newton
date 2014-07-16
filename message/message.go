package message

// Defines a message for SessionSecret
type Authenticated struct {
	Action        string
	Status        int
	SessionSecret string
}

// Defines a message for ClientId
type ClientId struct {
	Action   string
	Status   int
	ClientId string
}

// Defines dummy message with any kind of status
type Dummy struct {
	Action string
	Status int
	Body   string
}

// Defines an authentication request message between newton servers
type AuthenticateServer struct {
	Action   string
	Identity string
	Password string
}

// Defines a membership message for newton servers
type CreateServer struct {
	Action       string
	Idendity     string
	Password     string
	WanIp        string // Outbound interface IP
	WanPort      string // Outbound port
	InternalIp   string // Inbound interface IP, for only rack-aware setups
	InternalPort string // Inbound port
}

// Defines a message for deleting a server
type DeleteServer struct {
	Action   string
	Identity string
}
