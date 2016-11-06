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

package dht

import (
	"sync"
	"time"

	"github.com/purak/newton/log"
	"github.com/purak/newton/store"
)

type asyncStore struct {
	sync.RWMutex

	maxCheckTime   int
	expireInterval int64
	pq             *store.PriorityQueue
	req            map[string]map[string]struct{}
	counter        map[string]int
}

func (d *DistributedHashTable) checkAsyncRequests() {
	ticker := time.NewTicker(1000 * time.Millisecond)
	defer func() {
		ticker.Stop()
		d.waitGroup.Done()
	}()
	for {
		select {
		case <-ticker.C:
			err := d.checkAsyncRequest()
			if err != nil {
				log.Error("Error while checking async requests: ", err)
			}
		case <-d.done:
			return
		}
	}
}

func (d *DistributedHashTable) checkAsyncRequest() error {
	d.asyncStore.Lock()
	defer d.asyncStore.Unlock()

	if d.asyncStore.pq.Len() == 0 {
		// Queue is empty
		return nil
	}
	u := d.asyncStore.pq.Expire()
	if u == nil {
		// There is nothing to expire
		log.Debug("Nothing to expire.")
		return nil
	}

	userHash := u.(string)
	if counter, ok := d.asyncStore.counter[userHash]; ok {
		if counter >= d.asyncStore.maxCheckTime {
			log.Debugf("No more async check for %s", userHash)
			delete(d.asyncStore.req, userHash)
			delete(d.asyncStore.counter, userHash)
			return nil
		}
		d.asyncStore.counter[userHash] = counter + 1
	}
	clients, err := d.FindClients(userHash)
	if err == ErrNoClient {
		err = nil
	}
	if err != nil {
		return err
	}
	if clients != nil {
		addrs := d.asyncStore.req[userHash]
		go d.sendAsyncResponse(userHash, addrs, clients)
		delete(d.asyncStore.req, userHash)
		delete(d.asyncStore.counter, userHash)
		return nil
	}

	// Use a priority queue to send responses to remote newton nodes.
	d.asyncStore.pq.Add(userHash, d.asyncStore.expireInterval)
	return nil
}

func (d *DistributedHashTable) sendAsyncResponse(userHash string, addrs map[string]struct{}, clients []UserClientItem) {
	for addr := range addrs {
		if err := d.sendLookupUserHashSuccess(userHash, clients, addr); err != nil {
			log.Errorf("Error while sending lookukUserHashSuccess message to address: %s", addr)
			continue
		}
	}
}
