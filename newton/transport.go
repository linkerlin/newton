package newton

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

var (
	ErrInvalidClientID = errors.New("Invalid ClientID")
	ErrClientNotFound  = errors.New("Client could not be found")
)

type event struct {
	event string
	id    uint32
	data  []byte
}

type data struct {
	Method  string
	Headers map[string][]string
}

func (n *Newton) filterHeaders(headers map[string][]string) map[string][]string {
	if len(n.config.WhitelistedHeaders) == 0 {
		// It's disabled by the user. Return all the headers from the original request.
		return headers
	}
	filtered := make(map[string][]string)
	for _, header := range n.config.WhitelistedHeaders {
		if value, ok := headers[header]; ok {
			filtered[header] = value
		}
	}
	return filtered
}

func (n *Newton) processTransportRequest(r *http.Request, ps httprouter.Params) (*transport, uint32, bool, error) {
	var userHash string
	name := ps.ByName("name")
	hash := r.URL.Query().Get("hash")
	if hash == "1" || n.config.Hash {
		userHash = fmt.Sprintf("%x", sha1.Sum([]byte(name)))
	} else {
		userHash = name
	}

	cID := ps.ByName("clientID")
	clientID, err := strconv.ParseUint(cID, 10, 32)
	if err != nil {
		log.Debugf("Error while processing clientID %s: %s", cID, err)
		return nil, 0, false, ErrInvalidClientID
	}

	conn := n.clientStore.GetClient(userHash, uint32(clientID))
	if conn == nil {
		log.Debugf("No client found with this name %s and clientID %d", name, clientID)
		return nil, 0, false, ErrClientNotFound
	}

	mID, ch, err := n.getMessageID()
	if err != nil {
		log.Errorf("Error while generating messageID: %s", err)
		return nil, 0, false, err
	}
	dt := data{
		Method:  r.Method,
		Headers: n.filterHeaders(r.Header),
	}
	msg, err := MsgToByte(dt)
	if err != nil {
		log.Errorf("Error while generating event for your request: %s", err)
		return nil, 0, false, err
	}
	evt := event{
		event: ps.ByName("routingKey"),
		id:    mID,
		data:  msg,
	}

	conn.events <- evt
	return ch, mID, conn.autoACK, nil
}

func (n *Newton) messageIDHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	messageID := ps.ByName("messageID")
	mID, err := strconv.ParseInt(messageID, 10, 32)
	if err != nil {
		desc := fmt.Sprintf("Error while processing messageID: %s", err)
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		return
	}

	n.messageIDStore.mu.RLock()
	tr, ok := n.messageIDStore.transports[uint32(mID)]
	n.messageIDStore.mu.RUnlock()
	if !ok {
		desc := fmt.Sprintf("No waiting request found with this messageID: %d", mID)
		n.jsonErrorResponse(w, desc, http.StatusNotFound)
		return
	}
	log.Debugf("Incoming connection with messageID: %s", messageID)

	// Returns a channel that blocks until the connection is closed
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		desc := "Closing not supported"
		n.jsonErrorResponse(w, desc, http.StatusNotImplemented)
		log.Error(desc)
		return
	}
	closeChan := cn.CloseNotify()
	tr.request <- r
	tr.responseWriter <- &w

	select {
	case <-tr.done:
		log.Debugf("Connection has been closed for messageID: %s by done channel.", messageID)
		return
	case <-closeChan:
		log.Debugf("Connection has been closed for messageID: %s by close channel.", messageID)
		return
	}
}

func (n *Newton) closeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	messageID := ps.ByName("messageID")
	mID, err := strconv.ParseInt(messageID, 10, 32)
	if err != nil {
		desc := fmt.Sprintf("Error while processing messageID: %s", err)
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		return
	}

	n.messageIDStore.mu.RLock()
	tr, ok := n.messageIDStore.transports[uint32(mID)]
	n.messageIDStore.mu.RUnlock()
	if !ok {
		desc := fmt.Sprintf("No waiting request found with this messageID: %d", mID)
		n.jsonErrorResponse(w, desc, http.StatusNotFound)
		return
	}
	rCode := ps.ByName("returnCode")
	rc, err := strconv.Atoi(rCode)
	if err != nil {
		desc := fmt.Sprintf("Error while processing return code: %s", err)
		n.jsonErrorResponse(w, desc, http.StatusInternalServerError)
		return
	}
	// TODO: check content-length
	// TODO: We may need to validate return code before sending it to the receiver goroutine.
	res := &result{
		code: rc,
		body: r.Body,
		done: make(chan struct{}),
	}
	tr.resultCh <- res
	select {
	case <-time.After(n.writeTimeout):
		log.Debugf("Timeout exceeded for connection with MessageID: %s, return code: %d", messageID, rc)
	case <-res.done:
	}

	log.Debugf("Closing connection with MessageID: %s, return code: %d", messageID, rc)
	w.WriteHeader(http.StatusNoContent)
}

func (n *Newton) writeHeaders(dst func() http.Header, src http.Header) {
	for header, values := range src {
		if !strings.HasPrefix(header, "X-Newton") {
			continue
		}
		// Trim the prefix: X-Newton-Trim-Content-Length
		if strings.HasPrefix(header, "X-Newton-Trim") {
			header = strings.TrimPrefix(header, "X-Newton-Trim-")
		}
		for _, value := range values {
			dst().Add(header, value)
		}
	}
}
