package message

//import "github.com/nu7hatch/gouuid"

type Authenticated struct {
	Type          string
	SessionSecret string
}
