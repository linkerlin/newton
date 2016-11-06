package newton

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"sync"

	"github.com/purak/newton/store"
)

const maxMessageID uint32 = 4294967295

var messageIDKey = []byte("messageID")

type transport struct {
	request        chan *http.Request
	responseWriter chan *http.ResponseWriter
	resultCh       chan *result
	done           chan struct{}
}

type result struct {
	code int
	body io.ReadCloser
	done chan struct{}
}

type messageIDStore struct {
	mu         sync.RWMutex
	head       uint32
	transports map[uint32]*transport
	store      *store.LevelDB
}

func newMessageIDStore(str *store.LevelDB) (*messageIDStore, error) {
	mID, err := str.Get(messageIDKey)
	if err != nil {
		return nil, err
	}

	var head uint32
	if mID != nil {
		buf := bytes.NewReader(mID)
		err := binary.Read(buf, binary.LittleEndian, &head)
		if err != nil {
			return nil, err
		}
	}

	if head == maxMessageID {
		head = 0
	}
	head++
	return &messageIDStore{
		store:      str,
		head:       head,
		transports: make(map[uint32]*transport),
	}, nil
}

func (n *Newton) getMessageID() (uint32, *transport, error) {
	n.messageIDStore.mu.Lock()
	defer n.messageIDStore.mu.Unlock()

	messageID := n.messageIDStore.head
	tr := &transport{
		request:        make(chan *http.Request, 1),
		responseWriter: make(chan *http.ResponseWriter, 1),
		resultCh:       make(chan *result, 1),
		done:           make(chan struct{}),
	}
	n.messageIDStore.transports[messageID] = tr
	if messageID == maxMessageID {
		n.messageIDStore.head = 0
	} else {
		n.messageIDStore.head++
	}

	// Save the HEAD value to stable storage
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, messageID)
	if err != nil {
		return 0, nil, err
	}
	if err := n.messageIDStore.store.Set(messageIDKey, buf.Bytes()); err != nil {
		return 0, nil, err
	}
	return messageID, tr, nil
}

func (n *Newton) deleteMessageID(messageID uint32) {
	n.messageIDStore.mu.Lock()
	defer n.messageIDStore.mu.Unlock()
	tr, ok := n.messageIDStore.transports[messageID]
	if ok {
		close(tr.done)
		delete(n.messageIDStore.transports, messageID)
	}
}
