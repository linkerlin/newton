package kv

import (
	"errors"
	"sync"
	"time"
)

type clusterTime struct {
	mu sync.RWMutex

	base  int64
	start int64
}

func (c *clusterTime) getClusterTime() (time.Duration, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.start <= 0 {
		return 0, errors.New("no cluster time to use")
	}
	now := time.Now().UnixNano()
	return time.Duration(c.base + (now - c.start)), nil
}

func (c *clusterTime) insertClusterTime(base int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.base = base
	c.start = time.Now().UnixNano()
}
