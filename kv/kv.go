package kv

import (
	"sync"

	"github.com/julienschmidt/httprouter"
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
	router.PUT("/kv/set/:key", k.setHandler)
	router.GET("/kv/get/:key", k.getHandler)
	router.DELETE("/kv/delete/:key", k.deleteHandler)
	return k
}

func (k *KV) Set(key string, value []byte) error {
	return k.partitions.set(key, value)
}

func (k *KV) Get(key string) ([]byte, error) {
	return k.partitions.get(key)
}

func (k *KV) Delete(key string) error {
	return k.partitions.delete(key)
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
