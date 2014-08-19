package comm

// Authenticated represents a message for SessionSecret
type Authenticated struct {
	Action        int
	SessionSecret string
}

// ClientID represents a message for ClientID
type ClientID struct {
	// TODO: Rename this
	Action   int
	ClientID string
}

// Error represents an error message
type Error struct {
	Action int
	Code   int
}

// Success represents a success message
type Success struct {
	Action int
}

// AuthenticateServer represents an authentication request message between newton servers
type AuthenticateServer struct {
	Action   int
	Identity string
	Password string
}

// CreateServer represents a membership message for newton servers
type CreateServer struct {
	Action       int
	Idendity     string
	Password     string
	WanIP        string // Outbound interface IP
	WanPort      string // Outbound port
	InternalIP   string // Inbound interface IP, for only rack-aware setups
	InternalPort string // Inbound port
}

// DeleteServer represents a message for deleting a server
type DeleteServer struct {
	Action   int
	Identity string
}

// TrackUser represents a message for making a query to find a particular user who has active connetions on the cluster.
type TrackUser struct {
	Action   int
	Username string
}

// AnnounceUser reprents a message to inform other members of the cluster about a users opened sessions.
type AnnounceUser struct {
	Action   int
	Location []byte
}

// LookupUser represents a messsage to check the user's currently opened sessions for sending and receiving messages.
type LookupUser struct {
	Action   int
	Username string
}

// UserOnline represents a message to inform the client about his subscriber's availability.
type UserOnline struct {
	Action   int
	Username string
}
