package kv

import (
	"context"
	"errors"
	"sync"

	"github.com/purak/ghash"
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

	set    map[int32]*ghash.GHash
	delete map[int32]*ghash.GHash
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
	gh, ok := k.transactions.set[partID]
	if !ok {
		cfg := newDefaultGHashConfig()
		gh, err := ghash.New(cfg)
		if err != nil {
			return err
		}
		k.transactions.set[partID] = gh
	}

	log.Debugf("New transaction has been set for %s", key)
	return gh.Insert(key, value)
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

	gh := k.transactions.set[partID]
	value, err := gh.Find(key)
	if err != nil {
		return err
	}
	if err := k.setBackup(key, value, 0); err != nil {
		return err
	}

	// Clean garbage on transaction struct.
	// TODO: Add Len() method to ghash
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
	gh, ok := k.transactions.set[partID]
	if !ok {
		return ErrPartitionNotFound
	}

	if err := gh.Delete(key); err != nil {
		return err
	}
	// TODO: Add Len() method to ghash
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
	gh, ok := k.transactions.delete[partID]
	if !ok {
		cfg := newDefaultGHashConfig()
		gh, err := ghash.New(cfg)
		if err != nil {
			return err
		}
		k.transactions.delete[partID] = gh
	}
	// TODO: Add nil value support to GHash
	return gh.Insert(key, []byte{})
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

	gh, ok := k.transactions.delete[partID]
	if !ok {
		return ErrPartitionNotFound
	}
	// TODO: add an "IsExist" method to GHash
	if _, err := gh.Find(key); err != nil {
		return err
	}
	if err := k.deleteBackup(key); err != nil {
		return err
	}

	// TODO: Add Len() method to ghash
	return gh.Delete(key)
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

	gh, ok := k.transactions.delete[partID]
	if !ok {
		return ErrPartitionNotFound
	}
	if _, err := gh.Find(key); err != nil {
		return err
	}

	log.Debugf("Delete transaction has been deleted(rollback) for %s", key)
	// TODO: Add Len() method to ghash
	return gh.Delete(key)
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
