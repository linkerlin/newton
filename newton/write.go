package newton

import (
	"io"
	"net/http"
	"time"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

// writeHandler processes a POST request to transfer the request body.
func (n *Newton) writeHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Returns a channel that blocks until the connection is closed
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		desc := "Closing not supported"
		n.jsonErrorResponse(w, desc, http.StatusNotImplemented)
		log.Error(desc)
		return
	}
	closeChan := cn.CloseNotify()
	ch, messageID, autoACK, err := n.processTransportRequest(r, ps)
	defer n.deleteMessageID(messageID)
	if err == ErrInvalidClientID {
		n.jsonErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err == ErrClientNotFound {
		n.jsonErrorResponse(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		n.jsonErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	select {
	case <-closeChan:
		return
	case <-time.After(n.writeTimeout):
		n.jsonErrorResponse(w, "Request timeout", http.StatusRequestTimeout)
		return
	case responseWriter := <-ch.responseWriter:
		rw := *responseWriter
		n.writeHeaders(rw.Header, r.Header)

		if rw.Header().Get("Content-Range") != "" {
			rw.WriteHeader(http.StatusPartialContent)
		} else {
			rw.WriteHeader(http.StatusOK)
		}
		reader := readerThrottler(r.Body, n.dataTransferRate, n.dataTransferBurstLimit)
		// Get ResponseWriter of the GET request
		nr, err := io.Copy(rw, reader)
		if err != nil {
			log.Errorf("Error while transfering data: %s", err)
			n.jsonErrorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Debugf("%d byte(s) have been written for MessageID: %d", nr, messageID)
	}

	if autoACK {
		// It's just OK.
		w.WriteHeader(http.StatusNoContent)
		return
	}

	select {
	case <-time.After(n.writeTimeout):
		n.jsonErrorResponse(w, "Request timeout", http.StatusRequestTimeout)
		return
	case rc := <-ch.resultCh:
		defer close(rc.done)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rc.code)
		if _, err := io.Copy(w, rc.body); err != nil {
			log.Errorf("Error while copying request body: %s", err)
		}
	}
}
