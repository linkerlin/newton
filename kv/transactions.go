package kv

import (
	"errors"
	"sync"

	"github.com/purak/newton/log"
)

var ErrPartitionNotFound = errors.New("Partition not found")

type tDelete struct {
	mu sync.RWMutex

	m map[string]struct{}
}

type transactions struct {
	mu sync.RWMutex

	set    map[int32]*kv
	delete map[int32]*tDelete
}

func (k *KV) setTransaction(key string, value []byte, ttl int64, partID int32) error {
	ok, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrWrongBackupMember
	}
	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()
	part, ok := k.transactions.set[partID]
	if !ok {
		part = &kv{
			m: make(map[string]*item),
		}
		k.transactions.set[partID] = part
	}
	part.mu.Lock()
	defer part.mu.Unlock()

	part.m[key] = &item{
		value: value,
		ttl:   ttl,
	}
	log.Debugf("New transaction has been set for %s", key)
	return nil
}

func (k *KV) commitSetTransaction(key string, partID int32) error {
	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()

	part := k.transactions.set[partID]
	part.mu.Lock()
	defer part.mu.Unlock()
	i, ok := part.m[key]
	if !ok {
		return ErrKeyNotFound
	}
	if err := k.setBackup(key, i.value, i.ttl); err != nil {
		return err
	}

	// Clean garbage on transaction struct.
	delete(part.m, key)
	if len(part.m) == 0 {
		delete(k.transactions.set, partID)
	}

	log.Debugf("Transaction has been committed for %s", key)
	return nil
}

func (k *KV) rollbackSetTransaction(key string, partID int32) error {
	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()
	part, ok := k.transactions.set[partID]
	if !ok {
		return ErrPartitionNotFound
	}

	part.mu.Lock()
	defer part.mu.Unlock()
	delete(part.m, key)
	if len(part.m) == 0 {
		delete(k.transactions.set, partID)
	}
	log.Debugf("Set transaction has been deleted(rollback) for %s", key)
	return nil
}

func (k *KV) deleteTransaction(key string, partID int32) error {
	ok, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrWrongBackupMember
	}

	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()
	part, ok := k.transactions.delete[partID]
	if !ok {
		part = &tDelete{
			m: make(map[string]struct{}),
		}
		k.transactions.delete[partID] = part
	}
	part.mu.Lock()
	defer part.mu.Unlock()
	part.m[key] = struct{}{}
	return nil
}

func (k *KV) commitDeleteTransaction(key string, partID int32) error {
	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()

	part, ok := k.transactions.delete[partID]
	if !ok {
		return ErrPartitionNotFound
	}
	part.mu.Lock()
	defer part.mu.Unlock()
	if _, ok := part.m[key]; !ok {
		return ErrKeyNotFound
	}
	if err := k.deleteBackup(key); err != nil {
		return err
	}
	delete(part.m, key)
	if len(part.m) == 0 {
		delete(k.transactions.delete, partID)
	}
	return nil
}

func (k *KV) rollbackDeleteTransaction(key string, partID int32) error {
	k.transactions.mu.Lock()
	defer k.transactions.mu.Unlock()

	part, ok := k.transactions.delete[partID]
	if !ok {
		return ErrPartitionNotFound
	}
	part.mu.Lock()
	defer part.mu.Unlock()
	if _, ok := part.m[key]; !ok {
		return ErrKeyNotFound
	}
	delete(part.m, key)
	if len(part.m) == 0 {
		delete(k.transactions.delete, partID)
	}

	log.Debugf("Delete transaction has been deleted(rollback) for %s", key)
	return nil
}