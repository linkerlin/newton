package kv

import (
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/ghash"
	"github.com/purak/newton/locker"
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
	locker       *locker.Locker
	Grpc         *Grpc
}

func New(p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]*ghash.GHash),
	}
	backups := &partitions{
		m: make(map[int32]*ghash.GHash),
	}
	transactions := &transactions{
		set:    make(map[int32]*ghash.GHash),
		delete: make(map[int32]*ghash.GHash),
	}

	k := &KV{
		done:         make(chan struct{}),
		partman:      p,
		partitions:   parts,
		transactions: transactions,
		backups:      backups,
		locker:       locker.New(),
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
	h := murmur3.Sum64(data)
	// TODO: Get partition count from configuration
	partID := h % 23
	return int32(partID)
}

func newDefaultGHashConfig() *ghash.Config {
	return &ghash.Config{
		Hasher: hasher{},
		// In bytes
		InitialGroupSize:  1024,
		InitialGroupCount: 32,
		ShardCount:        31,
	}
}
