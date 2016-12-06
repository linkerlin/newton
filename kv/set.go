package kv

import (
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"golang.org/x/net/context"

	ksrv "github.com/purak/newton/proto/kv"
)

// Undo the transaction in a safe manner. We assume that the underlying partition manager works consistently.
func (k *KV) undoTransactionForSet(addresses []string, key string, partID int32) {
	for {
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
				log.Debugf("%s is no longer a participant for PartitionID: %d. Skip rollbackTransactionForSet.", address, partID)
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
		if len(tmp) == 0 {
			break
		}
		addresses = tmp
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
			// consistently and we eventually access all the members in s slice to undo
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

	item, oldItem := k.partitions.set(key, value, partID, ttl)
	defer item.mu.Unlock()

	addresses, err := k.partman.FindBackupOwners(partID)
	if err == partition.ErrNoBackupMemberFound {
		log.Debugf("No backup member found for Partition: %d", partID)
		return nil
	}
	if err != nil {
		// Something wrong with partition manager. It should be a rare incident in
		// development phase and it shouldn't be encountered in production.
		// Mark the item as stale.
		item.stale = true
		return err
	}
	if err := k.startTransactionForSet(addresses, partID, key, value); err != nil {
		// Stale item should be removed by garbage collector component of KV, if a client
		// does not try to set the key again shortly after the failure.
		item.stale = true
		return err
	}

	sAddrs, err := k.tryToCommitTransactionForSet(addresses, key, partID)
	if err != nil {
		// Undo the commit and set old value
		if oldItem != nil {
			k.setOldItem(key, ttl, partID, item, oldItem, sAddrs)
			oldItem.mu.Unlock()
			return err
		}
		// TODO: remove the key/value from backups.
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

func (k *KV) setOldItem(key string, ttl int64, partID int32, item *item, oldItem *item, addresses []string) {
	// Set old value and TTL on local item.
	item.value = oldItem.value
	item.ttl = ttl

	for {
		failed := []string{}
		addresses, failed = k.clearParticipantList(addresses, failed, partID)
		if err := k.startTransactionForSet(addresses, partID, key, item.value); err != nil {
			log.Errorf("Failed to set a new transaction to set the old item again: %s", err)
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
