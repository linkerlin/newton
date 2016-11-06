package newton

import (
	"crypto/sha1"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
)

type LookupResponse struct {
	Result map[string][]uint32 `json:"result"`
}

func (n *Newton) lookupHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var userHash string
	name := ps.ByName("name")
	hash := r.URL.Query().Get("hash")
	if hash == "1" || n.config.Hash {
		userHash = fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	} else {
		userHash = name
	}

	response := LookupResponse{
		Result: make(map[string][]uint32),
	}

	localClients := n.clientStore.GetClients(userHash)
	if len(localClients) != 0 {
		response.Result[n.config.Address] = []uint32{}
		for _, clientID := range localClients {
			response.Result[n.config.Address] = append(response.Result[n.config.Address], clientID)
		}
	}
	/*
		// Run a query on DHT for the given UserHashes
		clients, err := n.dht.FindClients(userHash)
		if err == dht.ErrNoClient {
			log.Debugf("No client found for %s on DHT: ", name)
			err = nil
		}
		if err == dht.ErrNoClosePeer {
			log.Debugf("No close peer found for %s on DHT: ", name)
			err = nil
		}
		if err != nil {
			desc := fmt.Sprintf("Error while finding name: %s on DHT: %s", name, err)
			n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
			return
		}
		if len(clients) != 0 {
			for _, item := range clients {
				if _, ok := response.Result[item.Addr]; !ok {
					response.Result[item.Addr] = []uint32{}
				}
				response.Result[item.Addr] = append(response.Result[item.Addr], item.ClientID)
			}
		}
		if len(response.Result) == 0 {
			desc := fmt.Sprintf("No client found for name: %s", name)
			n.jsonErrorResponse(w, desc, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			desc := fmt.Sprintf("Error while returning error message to the client: %s", err)
			n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
			return
		}*/
}
