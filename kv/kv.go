package kv

import (
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/partition"
	"github.com/spaolacci/murmur3"
)

// KV defines a distributed key-value store.
type KV struct {
	waitGroup sync.WaitGroup
	done      chan struct{}
	partman   *partition.Partition

	partitions   *partitions
	transactions *transactions
	backups      *partitions
	Grpc         *Grpc
}

func New(p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]*kv),
	}
	backups := &partitions{
		m: make(map[int32]*kv),
	}
	transactions := &transactions{
		set:    make(map[int32]*kv),
		delete: make(map[int32]*tDelete),
	}

	k := &KV{
		done:         make(chan struct{}),
		partman:      p,
		partitions:   parts,
		backups:      backups,
		transactions: transactions,
	}

	g := &Grpc{
		kv: k,
	}
	k.Grpc = g

	router.POST("/kv/:key", k.setHandler)
	router.PUT("/kv/:key", k.setHandler)
	router.GET("/kv/:key", k.getHandler)
	router.DELETE("/kv/:key", k.deleteHandler)

	k.waitGroup.Add(1)
	k.garbageCollector()

	return k
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
