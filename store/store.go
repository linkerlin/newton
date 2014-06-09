/*
User related structs(IN-MEMORY)
====================
    * UserPublicKeys -- Hash Table
    * UserSecrets    -- Hash Table
    * UserClients    -- Hash Table & Linked List
    * Relations      -- Hash Table & Hash Table


User Clients related structs(IN-MEMORY)
=======================================
    * UserClientMessages -- HashTable(stores message list id as value)


Message Lists(DISK-BASED)
=========================
    * Messages -- Hash Table & Linked List

Messages
========
 The messages will be stored in small text files on the hard disk drive.
*/

package store

import (
	"bufio"
	"encoding/gob"
	"errors"
	"github.com/purak/newton/cstream"
	"io"
	"os"
	"sync"
	"time"
)

const (
	saveTimeout     = 100
	saveQueueLength = 1000
)

type UserStore struct {
	mu          sync.RWMutex
	users       map[string]string
	save        chan record
	Log         cstream.Logger
	SetLogLevel func(cstream.Level)
}

type record struct {
	Key, Value string
}

func NewUserStore(filename string) *UserStore {
	l, setlevel := cstream.NewLogger("newton.store")
	s := &UserStore{
		users:       make(map[string]string),
		Log:         l,
		SetLogLevel: setlevel,
	}
	if filename != "" {
		s.save = make(chan record, saveQueueLength)
		if err := s.load(filename); err != nil {
			s.Log.Warning("An error occcured loading %s: %s", filename, err)
		}
		go s.stream(filename)
	}
	return s
}

func (s *UserStore) Get(key, value *string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if u, ok := s.users[*key]; ok {
		*value = u
		return nil
	}
	return errors.New("Item not found")
}

func (s *UserStore) Set(key, value *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, present := s.users[*key]; present {
		return errors.New("Item already exists")
	}
	s.users[*key] = *value
	return nil
}

// Creates a new key-value pair using Set
func (s *UserStore) Put(key, value *string) error {
	if err := s.Set(key, value); err != nil {
		return err
	}

	// Send to queue for writing to disk
	if s.save != nil {
		s.save <- record{*key, *value}
	}
	return nil
}

/* Initializes related data structes, reads from dump and load into the RAM */
func (s *UserStore) load(filename string) error {
	fh, err := os.Open(filename)
	defer fh.Close()
	if err != nil {
		return err
	}

	buff := bufio.NewReader(fh)
	dec := gob.NewDecoder(buff)
	for {
		var r record
		if err := dec.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if err = s.Set(&r.Key, &r.Value); err != nil {
			return err
		}
	}
	return nil
}

/* Writes the data structure to disk periodically */
func (s *UserStore) stream(filename string) {
	fh, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	defer fh.Close()
	if err != nil {
		s.Log.Error("%s could not be read.", filename)
	}

	buff := bufio.NewWriter(fh)
	defer fh.Close()

	enc := gob.NewEncoder(buff)
	defer buff.Flush()

	tick := time.NewTicker(saveTimeout * time.Millisecond)
	for {
		var err error
		select {
		case r := <-s.save:
			err = enc.Encode(r)
		case <-tick.C:
			err = buff.Flush()
		}
		if err != nil {
			s.Log.Error(err.Error())
		}
	}
}
