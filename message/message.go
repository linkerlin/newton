package message

type Authenticated struct {
	Type          string
	SessionSecret string
}

type ClientId struct {
	Type     string
	ClientId string
}
