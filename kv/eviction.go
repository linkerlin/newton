package kv

import (
	"sync"
	"time"
)

type eviction struct {
	mu sync.RWMutex

	m map[int32]chan struct{}
}

func (k *KV) garbageCollector() {
	defer k.waitGroup.Done()

	// Wait for repartitioning events from partition manager
	// Stop garbage collection goroutines or start new ones.

}

func (k *KV) applyEvictionPolicy(partID int32, done chan struct{}) {
	defer k.waitGroup.Done()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Pick 100 item from partition randomly and check TTL values.
			// Remove the key from partition and its backups in a different goroutine
			// if it's expired.
		case <-done:
			return
		}
	}
}
