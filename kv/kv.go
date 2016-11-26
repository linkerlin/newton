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
	// Find partition number for the given key
	partID := getPartitionID(key)
	_, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return err
	}
	if local {
		item := k.partitions.set(key, value, partID)
		defer item.mu.Unlock()
		// TODO: set the key to backups
		return nil
	}
	// TODO: write the key through gRPC endpoint to responsible node.
	return nil
}

func (k *KV) Get(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	_, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return nil, err
	}
	if local {
		return k.partitions.get(key, partID)
	}
	// TODO: get the key via gRPC endpoint to responsible node.
	return nil, nil

}

func (k *KV) Delete(key string) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	_, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return err
	}
	if local {
		return k.partitions.delete(key, partID)
	}
	// TODO delete the key from cluster via gRPC endpoint
	return nil
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
