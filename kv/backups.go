package kv

import (
	"errors"

	"github.com/purak/newton/log"
)

var ErrWrongBackupMember = errors.New("Wrong backup member")

func (k *KV) setBackup(key string, value []byte, ttl int64) error {
	partID := getPartitionID(key)
	local, err := k.partman.AmIBackupOwner(partID)
	if err != nil {
		return err
	}
	if !local {
		return ErrWrongBackupMember
	}
	i, _ := k.backups.set(key, value, partID, ttl)
	defer i.mu.Unlock()
	log.Debugf("Backup has been set for %s", key)
	return nil
}

func (k *KV) deleteBackup(key string) error {
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
	i, err := k.backups.delete(key, partID)
	if err != nil {
		return err
	}
	defer i.mu.Unlock()
	return k.backups.deleteCommit(key, partID)
}
