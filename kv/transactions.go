package kv

import (
	"context"
	"errors"
	"sync"

	"github.com/purak/newton/log"

	ksrv "github.com/purak/newton/proto/kv"
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

func (k *KV) transactionForSet(key string, value []byte, ttl int64, partID int32) error {
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

func (k *KV) callTransactionForSetOn(address, key string, value []byte, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionForSetRequest{
		Key:         key,
		Value:       value,
		PartitionID: partID,
	}
	_, err = c.TransactionForSet(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) commitTransactionForSet(key string, partID int32) error {
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

func (k *KV) callCommitTransactionForSetOn(address, key string, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionQueryRequest{
		Key:         key,
		PartitionID: partID,
	}
	_, err = c.CommitTransactionForSet(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) rollbackTransactionForSet(key string, partID int32) error {
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

func (k *KV) callRollbackTransactionForSetOn(address, key string, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionQueryRequest{
		Key:         key,
		PartitionID: partID,
	}
	_, err = c.CommitTransactionForSet(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) transactionForDelete(key string, partID int32) error {
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

func (k *KV) callTransactionForDeleteOn(address, key string, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionForDeleteRequest{
		Key:         key,
		PartitionID: partID,
	}
	_, err = c.TransactionForDelete(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) commitTransactionForDelete(key string, partID int32) error {
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

func (k *KV) callCommitTransactionForDeleteOn(address, key string, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionQueryRequest{
		Key:         key,
		PartitionID: partID,
	}
	_, err = c.CommitTransactionForDelete(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) rollbackTransactionForDelete(key string, partID int32) error {
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

func (k *KV) callRollbackTransactionForDeleteOn(address, key string, partID int32) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.TransactionQueryRequest{
		Key:         key,
		PartitionID: partID,
	}
	_, err = c.CommitTransactionForDelete(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}
