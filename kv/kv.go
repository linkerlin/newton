package kv

import (
	"errors"
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"github.com/spaolacci/murmur3"
)

type item struct {
	mu sync.RWMutex

	value []byte
}

// KV defines a distributed key-value store.
type KV struct {
	waitGroup sync.WaitGroup
	done      chan struct{}
	partman   *partition.Partition

	partitions *partitions
	backups    *partitions
}

type partitions struct {
	mu sync.RWMutex

	m map[int32]*kv
}

type kv struct {
	mu sync.RWMutex

	m map[string]*item
}

func New(p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]*kv),
	}
	k := &KV{
		done:       make(chan struct{}),
		partman:    p,
		partitions: parts,
	}
	router.POST("/kv/set/:key", k.setHandler)
	router.GET("/kv/get/:key", k.getHandler)
	return k
}

func (k *KV) Set(key string, value []byte) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	// TODO: find responsible node for that key. Redirect or set locally.

	k.partitions.mu.RLock()
	part, ok := k.partitions.m[partID]
	if !ok {
		part = &kv{
			m: make(map[string]*item),
		}
		k.partitions.m[partID] = part
	}
	k.partitions.mu.RUnlock()

	part.mu.RLock()
	i, ok := part.m[key]
	part.mu.RUnlock()
	if ok {
		// Update the value in source an its backups.
		i.mu.Lock()
		i.value = value
		// TODO: Update backups
		i.mu.Unlock()
		return nil
	}
	// Create a new record.
	i = &item{
		value: value,
	}
	i.mu.Lock()

	part.mu.Lock()
	part.m[key] = i
	part.mu.Unlock()

	// TODO: update backups
	i.mu.Unlock()
	log.Infof("Set operation has been done for key %s", key)
	return nil
}

var ErrKeyNotFound = errors.New("No value found for given key")

func (k *KV) Get(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	// TODO: find responsible node for that key. Redirect or get locally.

	k.partitions.mu.RLock()
	part, ok := k.partitions.m[partID]
	if !ok {
		k.partitions.mu.RUnlock()
		// Partition could not be found for that key.
		return nil, ErrKeyNotFound
	}
	k.partitions.mu.RUnlock()

	part.mu.RLock()
	i, ok := part.m[key]
	part.mu.RUnlock()
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Create a thread-safe copy and unlock the data structure
	i.mu.RLock()
	value := make([]byte, len(i.value))
	copy(value, i.value)
	i.mu.RUnlock()

	log.Infof("Get operation has been done for key %s", key)
	return value, nil
}

func (k *KV) Stop() {
	select {
	case <-k.done:
		// Already closed
		return
	default:
	}
	close(k.done)
}

func getPartitionID(key string) int32 {
	data := []byte(key)
	hasher := murmur3.New64()
	hasher.Write(data)
	h := hasher.Sum64()
	// TODO: Get partition count from configuration
	partID := h % 23
	return int32(partID)
}
