// Copyright 2015 Burak Sezer
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"container/heap"
	"time"
)

type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

type PriorityQueue []*Item

func (pq *PriorityQueue) Add(value interface{}, priority int64) {
	epoch := time.Now().UnixNano() / 1000000
	i := &Item{
		Value:    value,
		Priority: epoch + priority,
	}
	pq.Push(i)
}

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Priority > pq[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

// Push is a function for adding items to the queue
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.Index = n
	*pq = append(*pq, item)
}

// Pop removes an element from PriorityQueue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// Remove removes an item from the queue
func (pq *PriorityQueue) Remove(index int) {
	heap.Remove(pq, index)
}

// Expire removes expired items from the queue
func (pq *PriorityQueue) Expire() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	epoch := time.Now().UnixNano() / 1000000
	if item.Priority <= epoch {
		item.Index = -1 // for safety
		*pq = old[0 : n-1]
		return item.Value
	}
	return nil
}

// NewPriorityQueue creates a new PriorityQueue instance
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	heap.Init(pq)
	return pq
}
