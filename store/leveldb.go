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
	"encoding/hex"
	"sync"

	"github.com/purak/newton/log"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type LevelDB struct {
	mu   sync.RWMutex
	conn *leveldb.DB
}

// Opens or creates a leveldb database at the given path and returns the db object
func OpenLevelDB(path string) (*LevelDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}

	// This db object will be freed by a shutdown handler.
	ldb := &LevelDB{
		conn: db,
	}
	return ldb, nil
}

// Get is a function for getting the value for a given key.
func (l *LevelDB) Get(key []byte) ([]byte, error) {
	// Remember that the contents of the returned slice should not be modified.
	data, err := l.conn.Get(key, nil)
	if err != nil {
		if err == errors.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return data, err
}

// Put is a function for setting a key/value pair to the db.
func (l *LevelDB) Set(key []byte, value []byte) error {
	err := l.conn.Put(key, value, nil)
	if err != nil {
		log.Warn("An error occured while setting ", key, " Error: ", err)
		return err
	}
	return nil
}

// Delete is a function for deleting a key/value pair from the db.
func (l *LevelDB) Delete(key []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	err := l.conn.Delete(key, nil)
	if err != nil {
		log.Warn("An error occured while deleting ", key, " Error: ", err)
		return err
	}
	return nil
}

// BatchPut inserts a bunch of key/value pairs to persistent database.
func (l *LevelDB) BatchPut(bunch map[string][]byte, args ...bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	batch := new(leveldb.Batch)
	for key, value := range bunch {
		if args[0] == true {
			h, err := hex.DecodeString(key)
			if err != nil {
				log.Error("Error while decoding string ", key, " to byte representation, Skipping: ", err)
				continue
			}
			batch.Put(h, value)
		} else {
			batch.Put([]byte(key), value)
		}
	}
	err := l.conn.Write(batch, nil)
	if err != nil {
		log.Warn("An error occured while inserting a bunch of key/value pair: ", err)
	}
	return err
}

// BatchDelete deletes a bunch of key/value pairs to persistent database.
func (l *LevelDB) BatchDelete(bunch []string, args ...bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	batch := new(leveldb.Batch)
	for _, key := range bunch {
		if args[0] == true {
			h, err := hex.DecodeString(key)
			if err != nil {
				log.Error("Error while decoding string ", key, " to byte representation, Skipping: ", err)
				continue
			}
			batch.Delete(h)
		} else {
			batch.Delete([]byte(key))
		}
	}
	err := l.conn.Write(batch, nil)
	if err != nil {
		log.Warn("Error while deleting a bunch of key/value pair: ", err)
	}
	return err
}

// NewIterator is just a wrapper for LevelDB's NewItetator.
func (l *LevelDB) NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator {
	return l.conn.NewIterator(slice, ro)
}

// Close closes a LeveLDB database. It's just a wrapper of original Close method of LevelDB.
func (l *LevelDB) Close() error {
	err := l.conn.Close()
	if err == leveldb.ErrClosed {
		err = nil
	}
	return err
}
