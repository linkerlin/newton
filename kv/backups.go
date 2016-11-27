package kv

import (
	"errors"

	"github.com/purak/newton/log"
	ksrv "github.com/purak/newton/proto/kv"
	"golang.org/x/net/context"
)

var ErrWrongBackupMember = errors.New("Wrong backup member")

func (k *KV) SetBackup(key string, value []byte) error {
	partID := getPartitionID(key)
	local, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return err
	}
	if !local {
		return ErrWrongBackupMember
	}
	item := k.backups.set(key, value, partID)
	defer item.mu.Unlock()
	log.Debugf("Backup has been set for %s", key)
	return nil
}

func (k *KV) callSetBackupOn(address, key string, value []byte) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.SetRequest{
		Key:   key,
		Value: value,
	}
	_, err = c.SetBackup(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) GetBackup(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	local, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return nil, err
	}
	if !local {
		return nil, ErrWrongBackupMember
	}
	log.Debugf("Extracting value for %s from backup.", key)
	return k.backups.get(key, partID)
}

func (k *KV) callGetBackupOn(address, key string) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	gr := &ksrv.GetRequest{
		Key: key,
	}
	_, err = c.GetBackup(context.Background(), gr)
	if err != nil {
		return err
	}
	return nil
}

func (k *KV) DeleteBackup(key string) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	local, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return err
	}
	if !local {
		return ErrWrongBackupMember
	}
	log.Debugf("Deleting %s from backup.", key)
	return k.backups.delete(key, partID)
}

func (k *KV) callDeleteBackupOn(address, key string) error {
	conn, err := k.partman.GetMemberConn(address)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	dr := &ksrv.DeleteRequest{
		Key: key,
	}
	_, err = c.DeleteBackup(context.Background(), dr)
	if err != nil {
		return err
	}
	return nil
}
