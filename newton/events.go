package newton

import (
	"crypto/sha1"
	"fmt"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

type connection struct {
	autoACK  bool
	clientID uint32
	events   chan event
}

func (n *Newton) eventsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		desc := "Error while getting http.Flusher."
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		log.Error(desc)
		return
	}

	var userHash string
	name := ps.ByName("name")
	hash := r.URL.Query().Get("hash")
	if hash == "1" || n.config.Hash {
		userHash = fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	} else {
		userHash = name
	}

	conn := &connection{
		events:  make(chan event, 56),
		autoACK: n.config.AutoACK,
	}
	autoACK := r.URL.Query().Get("autoACK")
	if autoACK == "1" {
		conn.autoACK = true
	} else if autoACK == "0" {
		conn.autoACK = true
	}
	clientID, err := n.clientStore.AddClient(userHash, conn)
	if err != nil {
		desc := fmt.Sprintf("Error while adding client: %s", err)
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		log.Error(desc)
		return
	}

	conn.clientID = clientID
	sc := SessionCreatedMsg{
		ClientID: clientID,
	}
	dt, err := MsgToByte(sc)
	if err != nil {
		desc := fmt.Sprintf("Error while preparing SessionCreated message: %s", err)
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		log.Error(desc)
		return
	}
	evt := event{
		event: "session-created",
		id:    0,
		data:  dt,
	}
	conn.events <- evt
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	// Returns a channel that blocks until the connection is closed
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		desc := "Closing not supported"
		n.jsonErrorResponse(w, desc, http.StatusNotImplemented)
		log.Error(desc)
		return
	}
	closeChan := cn.CloseNotify()
	log.Infof("New connection with Name: %s, ClientID: %d", name, clientID)
	n.waitGroup.Add(1)
	cancel := make(chan struct{})
	for {
		select {
		case <-closeChan:
			n.clientStore.DelClient(userHash, conn.clientID)
			log.Infof("Deleted connection with Name: %s, ClientID: %d", name, clientID)
			close(cancel)
			return
		case msg := <-conn.events:
			fmt.Fprintf(w, "event: %s\n\n", msg.event)
			fmt.Fprintf(w, "id: %d\n\n", msg.id)
			fmt.Fprintf(w, "data: %s\n\n", msg.data)
			// Flush the data immediately instead of buffering it for later.
			flusher.Flush()
		}
	}
}
