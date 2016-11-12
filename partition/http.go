package partition

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

func (p *Partition) alivenessHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	// Dummy handler. Just returns 200 OK
}

// ErrorMsg represents an error message
type ErrorMsg struct {
	Description string `json:"description"`
}

func (p *Partition) partitionSetHandler(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	dec := gob.NewDecoder(r.Body) // Will read from network.
	var table partitionTable
	err := dec.Decode(&table)
	if err != nil {
		p.jsonErrorResponse(w, fmt.Sprintf("Error while setting partition table: %s", err), http.StatusInternalServerError)
		return
	}
	partitionTableLock.Lock()
	p.table = &table
	partitionTableLock.Unlock()
	log.Infof("Received partition table from the oldest node.")
	select {
	case <-p.nodeInitialized:
		return
	default:
	}
	close(p.nodeInitialized)
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
