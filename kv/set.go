package kv

import (
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"golang.org/x/net/context"

	"github.com/purak/ghash"
	ksrv "github.com/purak/newton/proto/kv"
)

func (k *KV) tryUndoTransactionForSet(addresses []string, key string, partID int32) []string {
	tmp := []string{}
	for _, address := range addresses {
		ok, err := k.partman.IsBackupOwner(partID, address)
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
	defer k.locker.Unlock(key)

	// FIXME: k.partitions.set may return an error about memory allocation.
	addresses, err := k.partman.FindBackupOwners(partID)
	if err == partition.ErrNoBackupMemberFound {
		return k.partitions.set(key, value, partID)
	}
	if err != nil {
		return err
	}

	if err = k.startTransactionForSet(addresses, partID, key, value); err != nil {
		return err
	}

	sAddrs, err := k.tryToCommitTransactionForSet(addresses, key, partID)
	if err != nil {
		// Undo the commit and set old value
		currentValue, gErr := k.partitions.get(key, partID)
		if gErr == nil {
			k.setOldValue(key, currentValue, partID, sAddrs)
		}
		if gErr == ghash.ErrKeyNotFound {
			k.deleteKeyFromBackups(key, partID, sAddrs)
			gErr = nil
		}
		if gErr != nil {
			return gErr
		}
		return err
	}
	return k.partitions.set(key, value, partID)
}

func (k *KV) tryToCommitTransactionForSet(addresses []string, key string, partID int32) ([]string, error) {
	// Commit changes now. If you encountner a problem during commit phase,
	// remove committed data or set the old one again.
	success := []string{}
	for _, bAddr := range addresses {
		log.Debugf("Calling CommitTransactionForSet for %s on %s", key, bAddr)
		if err := k.callCommitTransactionForSetOn(bAddr, key, partID); err != nil {
			log.Errorf("Failed CallTransactionForSet: %s on %d", key, bAddr)
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
		ok, err := k.partman.IsBackupOwner(partID, bAddr)
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
				log.Errorf("Failed CallTransactionForSet: %s on %d", key, bAddr)
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
