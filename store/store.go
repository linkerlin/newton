package store

import (
	"container/heap"
	"time"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Value string // The value of the item; arbitrary.
	TTL   int64  // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	Index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].TTL > pq[j].TTL
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) Update(item *Item, Value string, TTL int64) {
	heap.Remove(pq, item.Index)
	item.Value = Value
	item.TTL = TTL
	heap.Push(pq, item)
}

// Cleans expired items in the queue
func (pq *PriorityQueue) Expire() {
	old := *pq
	n := len(old)
	item := old[n-1]

	epoch := time.Now().Unix()
	if item.TTL < epoch {
		item.Index = -1 // for safety
		*pq = old[0 : n-1]
	}
}
