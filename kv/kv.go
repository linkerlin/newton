package kv

import (
	"sync"

	"github.com/purak/newton/partition"
)

type item struct {
	mu sync.RWMutex

	body      []byte
	updatedAt int64
}

// KV defines a distributed key-value store.
type KV struct {
	waitGroup sync.WaitGroup
	done      chan struct{}
	partman   *partition.Partition

	parts   *partitions
	backups *partitions
}

type kv struct {
	mu sync.RWMutex

	m map[string]*item
}

type partitions struct {
	mu sync.RWMutex

	m map[int32]*kv
}

func New(p *partition.Partition) *KV {
	parts := &partitions{
		m: make(map[int32]*kv),
	}
	return &KV{
		done:    make(chan struct{}),
		partman: p,
		parts:   parts,
	}
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
