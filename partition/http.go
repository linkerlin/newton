package partition

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

// ErrorMsg represents an error message
type ErrorMsg struct {
	Description string `json:"description"`
}

func (p *Partition) partitionSetHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.jsonErrorResponse(w, fmt.Sprintf("Error while reading request body: %s", err), http.StatusInternalServerError)
	}

	fmt.Println(string(b))
}

// jsonErrorResponse prepares a json response with the given description and  status code variables then returns the response.
func (p *Partition) jsonErrorResponse(w http.ResponseWriter, desc string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	msg := &ErrorMsg{
		Description: desc,
	}
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		log.Errorf("Error while returning error message: %s", err)
	}
}
