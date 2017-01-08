package kv

import (
	"encoding/binary"

	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"golang.org/x/net/context"

	"github.com/purak/ghash"
	ksrv "github.com/purak/newton/proto/kv"
)

func (k *KV) tryUndoTransactionForSet(addresses []string, key string, partID int32) []string {
	tmp := []string{}
	for _, address := range addresses {
		ok, err := k.partman.IsBackupMember(partID, address)
		if err != nil {
			log.Errorf("Error while validating backup owner: %s", err)
			// Retry this. This should be an inconsistency in partition table
			// and it must be fixed by the cluster coordinator.
			tmp = append(tmp, address)
			continue
		}
		if !ok {
			log.Debugf("%s is no longer a participant for PartitionID: %d.", address, partID)
			continue
		}

		log.Debugf("Calling RollbackTransactionForSet for %s on %s", key, address)
		if err := k.callRollbackTransactionForSetOn(address, key, partID); err != nil {
			log.Errorf("Error while calling RollbackTransactionForSet on %s: %s", address, err)
			// Retry this until this member is removed from partition table by the cluster coordinator.
			tmp = append(tmp, address)
			continue
		}
	}
	return tmp
}

// Undo the transaction in a safe manner. We assume that the underlying partition manager works consistently.
func (k *KV) undoTransactionForSet(addresses []string, key string, partID int32) {
	for {
		addresses = k.tryUndoTransactionForSet(addresses, key, partID)
		if len(addresses) == 0 {
			break
		}
	}
	log.Infof("TransactionForSet: %s on %d has been rolled back succesfully.", key, partID)
}

func (k *KV) startTransactionForSet(addresses []string, partID int32, key string, value []byte) error {
	s := []string{}
	for _, bAddr := range addresses {
		log.Debugf("Calling TransactionForSet for %s on %s", key, bAddr)
		if err := k.callTransactionForSetOn(bAddr, key, value, partID); err != nil {
			// This function tries to undo the transaction in a for loop. That logic
			// may seem silly but we assume that underlying partition manager works
			// consistently and we eventually access all the members in the s slice to undo
			// the transaction. We have to do that to keep the partition in a consistent
			// state.
			k.undoTransactionForSet(s, key, partID)
			return err
		}
		s = append(s, bAddr)
	}
	return nil
}

func (k *KV) Set(key string, value []byte, ttl int64) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return err
	}
	if !local {
		return k.redirectSet(key, value, ttl, oAddr)
	}

	k.locker.Lock(key)
	defer func() {
		if err = k.locker.Unlock(key); err != nil {
			log.Errorf("Error while unlocking key %s: %s", key, err)
		}
	}()

	currentPos := make([]byte, 8)
	var pos uint64
	if k.config.Eviction {
		pos, err = k.setLRUItemOnSource(key, partID)
		if err != nil {
			return err
		}
		binary.LittleEndian.PutUint64(currentPos, pos)
		value = append(value, currentPos...)
	}

	addresses, err := k.partman.FindBackupMembers(partID)
	if err == partition.ErrNoBackupMemberFound {
		return k.partitions.insert(key, value, partID)
	}
	if err != nil {
		return err
	}

	if err = k.startTransactionForSet(addresses, partID, key, value); err != nil {
		return err
	}

	// Start to commit the new item
	currentValue, gErr := k.partitions.find(key, partID)
	alreadyExist := !(gErr == ghash.ErrKeyNotFound || gErr == ErrPartitionNotFound)
	if gErr != nil && alreadyExist {
		return gErr
	}
	if k.config.Eviction && alreadyExist {
		lg := len(currentValue)
		copy(currentValue[lg-8:lg], currentPos)
	}

	if err = k.partitions.insert(key, value, partID); err != nil {
		// It will do its job. Wait.
		k.undoTransactionForSet(addresses, key, partID)
		return err
	}

	sAddrs, err := k.tryToCommitTransactionForSet(addresses, key, partID)
	if err != nil {
		if gErr == nil {
			k.setOldValue(key, currentValue, partID, sAddrs)
			if iErr := k.partitions.insert(key, currentValue, partID); iErr != nil {
				return iErr
			}
		}
		if gErr == ghash.ErrKeyNotFound || gErr == ErrPartitionNotFound {
			k.deleteKeyFromBackups(key, partID, sAddrs)
			if dErr := k.partitions.delete(key, partID); err != nil {
				return dErr
			}
		}
		// Delete idle transactions
		tAddrs := diffStringSlices(sAddrs, addresses)
		// wait until done. if partition manager works properly, everything gonna be OK.
		k.undoTransactionForSet(tAddrs, key, partID)
		return err
	}
	return nil
}

func (k *KV) tryToCommitTransactionForSet(addresses []string, key string, partID int32) ([]string, error) {
	// Commit changes now. If you encountner a problem during commit phase,
	// remove committed data or set the old one again.
	success := []string{}
	for _, bAddr := range addresses {
		log.Debugf("Calling CommitTransactionForSet for %s on %s", key, bAddr)
		if err := k.callCommitTransactionForSetOn(bAddr, key, partID); err != nil {
			log.Errorf("Failed CallTransactionForSet: %s on %s: %s", key, bAddr, err)
			return success, err
		}
		success = append(success, bAddr)
	}
	return success, nil
}

func (k *KV) clearParticipantList(addresses, failed []string, partID int32) ([]string, []string) {
	tmp := []string{}
	// We need to keep participant list of the partition clean.
	for _, bAddr := range addresses {
		ok, err := k.partman.IsBackupMember(partID, bAddr)
		if err != nil {
			log.Errorf("Error while validating backup owner: %s", err)
			// Retry this. This should be an inconsistency in partition table
			// and it must be fixed by the cluster coordinator.
			failed = append(failed, bAddr)
			continue
		}
		if ok {
			tmp = append(tmp, bAddr)
		}
	}
	return tmp, failed
}

func (k *KV) setOldValue(key string, value []byte, partID int32, addresses []string) {
	for {
		failed := []string{}
		addresses, failed = k.clearParticipantList(addresses, failed, partID)
		if len(addresses) == 0 {
			return
		}
		if err := k.startTransactionForSet(addresses, partID, key, value); err != nil {
			log.Errorf("Failed to set a new transaction to set the old value again: %s", err)
			// If one of the participant nodes removed from the list, the above loop will catch
			// this event and remove immediately it from the list. A fresh transaction will eventually
			// be started for this operation.
			continue
		}

		for _, bAddr := range addresses {
			log.Debugf("Calling CommitTransactionForSet for %s on %s", key, bAddr)
			if err := k.callCommitTransactionForSetOn(bAddr, key, partID); err != nil {
				log.Errorf("Failed CallTransactionForSet: %s on %s: %s", key, bAddr, err)
				failed = append(failed, bAddr)
				continue
			}
		}
		// It's OK. Terminate this function.
		if len(failed) == 0 {
			return
		}
		addresses = failed
	}
}

func (k *KV) deleteKeyFromBackups(key string, partID int32, addresses []string) {
	for {
		failed := []string{}
		success := []string{}
		addresses, failed = k.clearParticipantList(addresses, failed, partID)
		// Start a new transaction and undo it if one of the participants of partition sends negative acknowledgement.
		for _, addr := range addresses {
			log.Debugf("Calling TransactionForDelete for %s on %s", key, addr)
			if err := k.callTransactionForDeleteOn(addr, key, partID); err != nil {
				failed = append(failed, addr)
				log.Errorf("Error while running callTransactionForDeleteOn on %s: %s", addr, err)
				continue
			}
			success = append(success, addr)
		}
		for _, addr := range success {
			log.Debugf("Calling CommitTransactionForDelete for %s on %s", key, addr)
			if err := k.callCommitTransactionForDeleteOn(addr, key, partID); err != nil {
				failed = append(failed, addr)
			}
		}
		// It's OK. Terminate this function.
		if len(failed) == 0 {
			break
		}
		addresses = failed
	}
}

func (k *KV) redirectSet(key string, value []byte, ttl int64, oAddr string) error {
	// Redirect the request to responsible node.
	conn, err := k.partman.GetMemberConn(oAddr)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	sr := &ksrv.SetRequest{
		Key:   key,
		Value: value,
	}
	_, err = c.Set(context.Background(), sr)
	if err != nil {
		return err
	}
	return nil
}

func diffStringSlices(slice1 []string, slice2 []string) []string {
	var diff []string

	for _, s1 := range slice1 {
		found := false
		for _, s2 := range slice2 {
			if s1 == s2 {
				found = true
				break
			}
		}
		// String not found. We add it to return slice
		if !found {
			diff = append(diff, s1)
		}
	}
	return diff
}
