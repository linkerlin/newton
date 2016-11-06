package newton

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/juju/ratelimit"
	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
)

func (n *Newton) readHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	// Returns a channel that blocks until the connection is closed
	cn, ok := w.(http.CloseNotifier)
	if !ok {
		desc := "Closing not supported"
		n.jsonErrorResponse(w, desc, http.StatusNotImplemented)
		log.Error(desc)
		return
	}
	closeChan := cn.CloseNotify()
	ch, messageID, _, err := n.processTransportRequest(r, ps)
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

	defer n.deleteMessageID(messageID)

	select {
	case <-closeChan:
		return
	case <-time.After(n.readTimeout):
		n.jsonErrorResponse(w, "Request timeout", http.StatusRequestTimeout)
		return
	case request := <-ch.request:
		defer request.Body.Close()
		n.writeHeaders(w.Header, request.Header)
		if w.Header().Get("Content-Range") != "" {
			w.WriteHeader(http.StatusPartialContent)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		reader := readerThrottler(request.Body, n.dataTransferRate, n.dataTransferBurstLimit)
		nr, err := io.Copy(w, reader)
		if err != nil {
			rw := <-ch.responseWriter
			n.jsonErrorResponse(*rw, fmt.Sprintf("Error while copying request body: %s", err), http.StatusInternalServerError)
			return
		}
		log.Debugf("%d byte(s) have been written for MessageID: %d", nr, messageID)
	case rc := <-ch.resultCh:
		defer close(rc.done)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(rc.code)
		if _, err := io.Copy(w, rc.body); err != nil {
			log.Errorf("Error while copying request body: %s", err)
		}
	}
}

func readerThrottler(r io.Reader, rate float64, capacity int64) io.Reader {
	bucket := ratelimit.NewBucketWithRate(rate, capacity)
	return ratelimit.Reader(r, bucket)
}
