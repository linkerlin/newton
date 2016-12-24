package kv

import (
	"errors"
	"sync"

	"github.com/purak/ghash"
	"github.com/spaolacci/murmur3"
)

type hasher struct{}

func (h hasher) Sum64(key string) uint64 {
	data := []byte(key)
	return murmur3.Sum64(data)
}

var ErrKeyNotFound = errors.New("No value found for given key")

type partitions struct {
	mu sync.RWMutex

	m map[int32]*ghash.GHash
}

func (pt *partitions) set(key string, value []byte, partID int32) error {
	var err error
	// Lock all kv store to find the responsible partition.
	pt.mu.Lock()
	gh, ok := pt.m[partID]
	if !ok {
		cfg := newDefaultGHashConfig()
		gh, err = ghash.New(cfg)
		if err != nil {
			return err
		}
		pt.m[partID] = gh
	}
	pt.mu.Unlock()
	return gh.Insert(key, value)
}

func (pt *partitions) get(key string, partID int32) ([]byte, error) {
	pt.mu.RLock()
	gh, ok := pt.m[partID]
	pt.mu.RUnlock()
	if !ok {
		// Partition could not be found for that key.
		return nil, ErrPartitionNotFound
	}
	return gh.Find(key)
}

func (pt *partitions) delete(key string, partID int32) error {
	pt.mu.RLock()
	gh, ok := pt.m[partID]
	pt.mu.RUnlock()
	if !ok {
		// Partition could not be found for that key.
		return ErrPartitionNotFound
	}
	return gh.Delete(key)
}
