package kv

import (
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/ghash"
	"github.com/purak/newton/config"
	"github.com/purak/newton/locker"
	"github.com/purak/newton/partition"
	"github.com/spaolacci/murmur3"
)

// KV defines a distributed key-value store.
type KV struct {
	config    *config.KV
	waitGroup sync.WaitGroup
	done      chan struct{}
	partman   *partition.Partition

	eviction     eviction
	partitions   *partitions
	transactions *transactions
	backups      *partitions
	time         *clusterTime
	locker       *locker.Locker
	Grpc         *Grpc
}

func New(cfg *config.KV, p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]ghash.GHash),
	}
	backups := &partitions{
		m: make(map[int32]ghash.GHash),
	}
	transactions := &transactions{
		set:    make(map[int32]ghash.GHash),
		delete: make(map[int32]ghash.GHash),
	}

	k := &KV{
		config:       cfg,
		done:         make(chan struct{}),
		partman:      p,
		partitions:   parts,
		transactions: transactions,
		backups:      backups,
		time:         &clusterTime{},
		locker:       locker.New(),
	}
	k.time.insertClusterTime(p.GetClusterTime())

	if k.config.Eviction {
		k.eviction = eviction{
			source: lru{partitions: make(map[int32]*fifo)},
			backup: lru{partitions: make(map[int32]*fifo)},
		}
	}

	g := &Grpc{
		kv: k,
	}
	k.Grpc = g

	router.POST("/kv/:key", k.setHandler)
	router.PUT("/kv/:key", k.setHandler)
	router.GET("/kv/:key", k.getHandler)
	router.DELETE("/kv/:key", k.deleteHandler)

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
