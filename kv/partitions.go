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

func (pt *partitions) getGHash(partID int32, create bool) (*ghash.GHash, error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	var err error
	gh, ok := pt.m[partID]
	if !ok {
		if !create {
			return nil, ErrPartitionNotFound
		}
		cfg := newDefaultGHashConfig()
		gh, err = ghash.New(cfg)
		if err != nil {
			return nil, err
		}
		pt.m[partID] = gh
	}
	return gh, nil
}

func (pt *partitions) insert(key string, value []byte, partID int32) error {
	gh, err := pt.getGHash(partID, true)
	if err != nil {
		return err
	}
	return gh.Insert(key, value)
}

func (pt *partitions) find(key string, partID int32) ([]byte, error) {
	gh, err := pt.getGHash(partID, false)
	if err != nil {
		return nil, err
	}
	return gh.Find(key)
}

func (pt *partitions) delete(key string, partID int32) error {
	gh, err := pt.getGHash(partID, false)
	if err != nil {
		return err
	}
	return gh.Delete(key)
}

func (pt *partitions) check(key string, partID int32) error {
	gh, err := pt.getGHash(partID, false)
	if err != nil {
		return err
	}
	return gh.Check(key)
}

func (pt *partitions) findWithRange(key, rrange string, partID int32) ([]byte, error) {
	gh, err := pt.getGHash(partID, false)
	if err != nil {
		return nil, err
	}
	return gh.FindWithRange(key, rrange)
}

func (pt *partitions) modify(key, rrange string, chunk []byte, partID int32) error {
	gh, err := pt.getGHash(partID, false)
	if err != nil {
		return err
	}
	return gh.Modify(key, rrange, chunk)
}
