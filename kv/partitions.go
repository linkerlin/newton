package kv

import (
	"errors"
	"sync"
)

var ErrKeyNotFound = errors.New("No value found for given key")

type item struct {
	mu sync.RWMutex

	value []byte
	ttl   int64
}

type partitions struct {
	mu sync.RWMutex

	m map[int32]*kv
}

type kv struct {
	mu sync.RWMutex

	m map[string]*item
}

func (pt *partitions) set(key string, value []byte, partID int32, ttl int64) (*item, *item) {
	pt.mu.Lock()
	part, ok := pt.m[partID]
	if !ok {
		part = &kv{
			m: make(map[string]*item),
		}
		pt.m[partID] = part
	}
	pt.mu.Unlock()

	part.mu.Lock()
	defer part.mu.Unlock()
	i, ok := part.m[key]
	var oldItem *item
	if ok {
		oldItem = i
		/*
			// Update the value in source an its backups.
			i.mu.Lock()
			i.value = value
			i.ttl = ttl
			// Unlock the item in KV.Set
			return i*/
	}
	// Create a new record.
	i = &item{
		value: value,
	}
	i.mu.Lock()
	part.m[key] = i
	// Unlock the item in KV.Set
	return i, oldItem
}

func (pt *partitions) get(key string, partID int32) ([]byte, error) {
	pt.mu.RLock()
	part, ok := pt.m[partID]
	if !ok {
		pt.mu.RUnlock()
		// Partition could not be found for that key.
		return nil, ErrKeyNotFound
	}
	pt.mu.RUnlock()

	part.mu.RLock()
	i, ok := part.m[key]
	if !ok {
		part.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	// Create a thread-safe copy and unlock the data structure
	i.mu.RLock()
	part.mu.RUnlock()
	value := make([]byte, len(i.value))
	copy(value, i.value)
	i.mu.RUnlock()
	return value, nil
}

func (pt *partitions) delete(key string, partID int32) error {
	pt.mu.RLock()
	part, ok := pt.m[partID]
	if !ok {
		pt.mu.RUnlock()
		// Partition could not be found for that key.
		return ErrKeyNotFound
	}
	part.mu.RLock()
	pt.mu.RUnlock()
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
