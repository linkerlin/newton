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
	ClientId string
}

// Defines dummy message with any kind of status
type Dummy struct {
	Type   string
	Status int
	Body   string
}

// Defines an authentication request message between newton servers
type AuthenticateServer struct {
	Type     string
	Idendity string
	Password string
}

// Defines a membership message for newton servers
type CreateServer struct {
	Type         string
	Idendity     string
	Password     string
	WanIp        string // Outbound interface IP
	WanPort      string // Outbound port
	InternalIp   string // Inbound interface IP, for only rack-aware setups
	InternalPort string // Inbound port
}

// Defines a message for deleting a server
type DeleteServer struct {
	Type     string
	Identity string
}
