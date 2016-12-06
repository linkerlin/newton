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
	stale bool
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

	createNewRecord := func() *item {
		ni := &item{
			value: value,
			ttl:   ttl,
		}
		ni.mu.Lock()
		part.m[key] = ni
		part.mu.Unlock()
		return ni
	}

	i, ok := part.m[key]
	if ok {
		i.mu.Lock()
		if i.stale {
			// Partition is locked. So we can remove this key safely.
			delete(part.m, key)
		}

		nr := createNewRecord()
		if !i.stale {
			return nr, i
		}
		i.mu.Unlock()
		return nr, nil
	}
	return createNewRecord(), nil
}

func (pt *partitions) get(key string, partID int32) ([]byte, error) {
	pt.mu.RLock()
	part, ok := pt.m[partID]
	if !ok {
		pt.mu.RUnlock()
		// Partition could not be found for that key.
		return nil, ErrPartitionNotFound
	}
	pt.mu.RUnlock()

	part.mu.RLock()
	i, ok := part.m[key]
	if !ok {
		part.mu.RUnlock()
		return nil, ErrKeyNotFound
	}

	if i.stale {
		// Partition is locked. So we can remove this key safely.
		delete(part.m, key)

		// Garbage value. It will be removed by garbage collector after some time.
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

func (pt *partitions) delete(key string, partID int32) (*item, error) {
	pt.mu.RLock()
	part, ok := pt.m[partID]
	if !ok {
		pt.mu.RUnlock()
		// Partition could not be found for that key.
		return nil, ErrPartitionNotFound
	}
	part.mu.RLock()
	pt.mu.RUnlock()
	i, ok := part.m[key]
	if !ok {
		part.mu.RUnlock()
		return nil, ErrKeyNotFound
	}
	i.mu.Lock()
	if i.stale {
		// The item is already locked by delete function of partitions struct. We can delete it
		// safely from the partition.
		delete(part.m, key)
		part.mu.RUnlock()
		i.mu.Unlock()
		return nil, ErrKeyNotFound
	}

	part.mu.RUnlock()
	i.stale = true
	return i, nil
}

func (pt *partitions) deleteCommit(key string, partID int32) error {
	pt.mu.RLock()
	part, ok := pt.m[partID]
	if !ok {
		pt.mu.RUnlock()
		// Partition could not be found for that key.
		return ErrPartitionNotFound
	}
	part.mu.RLock()
	defer part.mu.RUnlock()
	pt.mu.RUnlock()
	_, ok = part.m[key]
	if !ok {
		return ErrKeyNotFound
	}
	// The item is already locked by delete function of partitions struct. We can delete it
	// safely from the partition.
	delete(part.m, key)
	return nil
}
