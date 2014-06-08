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
	"fmt"
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
			fmt.Println(err)
			s.Log.Warning("%s could not be loaded.", filename)
		}
		go s.saveLoop(filename)
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

func (s *UserStore) Put(value, key *string) error {
	//for {
	if err := s.Set(key, value); err == nil {
		return nil
	}
	//}
	if s.save != nil {
		s.save <- record{*key, *value}
	}
	return nil
}

func (s *UserStore) load(filename string) error {
	f, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	b := bufio.NewReader(f)
	d := gob.NewDecoder(b)
	for {
		var r record
		if err := d.Decode(&r); err == io.EOF {
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

func (s *UserStore) saveLoop(filename string) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		// Produce an error message
		//log.Println("UserStore:", err)
		return
	}
	b := bufio.NewWriter(f)
	e := gob.NewEncoder(b)
	t := time.NewTicker(saveTimeout * time.Millisecond)
	defer f.Close()
	defer b.Flush()
	for {
		var err error
		select {
		case r := <-s.save:
			err = e.Encode(r)
		case <-t.C:
			err = b.Flush()
		}
		if err != nil {

		}
	}
}
