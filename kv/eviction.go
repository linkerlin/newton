package kv

import (
	"encoding/binary"
	"sync"

	"github.com/purak/ghash"
)

type eviction struct {
	lru lru
}

type lru struct {
	mu         sync.Mutex
	partitions map[int32]*fifo
}

// pushBack adds a fresh item to the fifo instance.
func (l *lru) pushBack(item []byte, partID int32) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	f, ok := l.partitions[partID]
	var err error
	if !ok {
		f, err = newFIFO()
		if err != nil {
			return 0, err
		}
		l.partitions[partID] = f
	}
	return f.pushBack(item)
}

// moveToBack removes the given item with the given pos and adds it again to back of the queue.
func (l *lru) moveToBack(item []byte, pos uint64, partID int32) (uint64, error) {
	l.mu.Lock()
	f, ok := l.partitions[partID]
	if !ok {
		l.mu.Unlock()
		return l.pushBack(item, partID)
	}
	defer l.mu.Unlock()
	return f.moveToBack(item, pos)
}

// remove drops an element from fifo with given item and pos.
func (l *lru) remove(item []byte, pos uint64, partID int32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if f, ok := l.partitions[partID]; ok {
		f.remove(item, pos)
	}
}

// removes an element from the front of the queue.
func (l *lru) pop(partID int32) []byte {
	l.mu.Lock()
	f, ok := l.partitions[partID]
	if !ok {
		l.mu.Unlock()
		return nil
	}
	defer l.mu.Unlock()
	return f.pop()
}

func (k *KV) setLRUItem(key string, partID int32) (uint64, error) {
	mkey := []byte(key)
	rrange := "-8"
	rawPos, err := k.partitions.findWithRange(key, rrange, partID)
	if err != nil && (err == ghash.ErrKeyNotFound || err == ErrPartitionNotFound) {
		newPos, err := k.eviction.lru.pushBack(mkey, partID)
		if err != nil {
			return 0, err
		}
		return newPos, nil
	}
	if err != nil {
		return 0, err
	}
	pos := binary.LittleEndian.Uint64(rawPos)
	return k.eviction.lru.moveToBack(mkey, pos, partID)
}
