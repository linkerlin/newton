package newton

import (
	_ "github.com/purak/newton/cluster"
	"net"
)

func (n *Newton) authenticateNewton(data map[string]interface{}, conn *net.Conn) ([]byte, int, error) {
	return nil, Success, nil
}
