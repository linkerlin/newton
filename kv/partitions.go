package kv

import (
	"errors"
	"sync"

	"github.com/purak/newton/log"
)

var ErrKeyNotFound = errors.New("No value found for given key")

type partitions struct {
	mu sync.RWMutex

	m map[int32]*kv
}

type kv struct {
	mu sync.RWMutex

	m map[string]*item
}

func (p *partitions) set(key string, value []byte) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	// TODO: find responsible node for that key. Redirect or set locally.

	p.mu.RLock()
	part, ok := p.m[partID]
	if !ok {
		part = &kv{
			m: make(map[string]*item),
		}
		p.m[partID] = part
	}
	p.mu.RUnlock()

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

func (p *partitions) get(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	// TODO: find responsible node for that key. Redirect or get locally.

	p.mu.RLock()
	part, ok := p.m[partID]
	if !ok {
		p.mu.RUnlock()
		// Partition could not be found for that key.
		return nil, ErrKeyNotFound
	}
	p.mu.RUnlock()

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

func (p *partitions) delete(key string) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	// TODO: find responsible node for that key. Redirect or delete locally.

	p.mu.RLock()
	part, ok := p.m[partID]
	if !ok {
		p.mu.RUnlock()
		// Partition could not be found for that key.
		return ErrKeyNotFound
	}
	p.mu.RUnlock()

	part.mu.RLock()

	i, ok := part.m[key]
	if !ok {
		part.mu.RUnlock()
		return ErrKeyNotFound
	}
	i.mu.RLock()
	delete(part.m, key)
	i.mu.RUnlock()

	part.mu.RUnlock()

	return nil
}
