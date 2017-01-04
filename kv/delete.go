package kv

import (
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	"golang.org/x/net/context"

	ksrv "github.com/purak/newton/proto/kv"
)

func (k *KV) tryUndoTransactionForDelete(addresses []string, key string, partID int32) []string {
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

		log.Debugf("Calling RollbackTransactionForDelete for %s on %s", key, address)
		if err := k.callRollbackTransactionForDeleteOn(address, key, partID); err != nil {
			log.Errorf("Error while calling RollbackTransactionForDelete on %s: %s", address, err)
			// Retry this until this member is removed from partition table by the cluster coordinator.
			tmp = append(tmp, address)
			continue
		}
	}
	return tmp
}

// Undo the transaction in a safe manner. We assume that the underlying partition manager works consistently.
func (k *KV) undoTransactionForDelete(addresses []string, key string, partID int32) {
	for {
		addresses = k.tryUndoTransactionForDelete(addresses, key, partID)
		if len(addresses) == 0 {
			break
		}
	}
	log.Infof("TransactionForDelete: %s on %d has been rolled back succesfully.", key, partID)
}

func (k *KV) Delete(key string) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return err
	}
	if !local {
		return k.redirectDelete(key, oAddr)
	}

	k.locker.Lock(key)
	defer k.locker.Unlock(key)

	// Ignore to run if given key doesn't exist in data store.
	if k.partitions.check(key, partID); err != nil {
		return err
	}

	addresses, err := k.partman.FindBackupMembers(partID)
	if err == partition.ErrNoBackupMemberFound {
		return k.partitions.delete(key, partID)
	}
	if err != nil {
		return err
	}

	// Start a new transaction and undo it if one of the participants of partition sends negative acknowledgement.
	succesfullyDeleted := []string{}
	for _, addr := range addresses {
		log.Debugf("Calling TransactionForDelete for %s on %s", key, addr)
		if err := k.callTransactionForDeleteOn(addr, key, partID); err != nil {
			// Undo transaction for delete. It should remove the transaction from cluster
			// if the underlying partition manager works consistently.
			k.undoTransactionForDelete(succesfullyDeleted, key, partID)
			return err
		}
		succesfullyDeleted = append(succesfullyDeleted, addr)
	}
	// Commit the transaction. If it's fails, re-set the key/value.
	succesfullyDeleted = []string{}
	for _, addr := range addresses {
		log.Debugf("Calling CommitTransactionForDelete for %s on %s", key, addr)
		if err := k.callCommitTransactionForDeleteOn(addr, key, partID); err != nil {
			// re-set the key.
			currentValue, gErr := k.partitions.find(key, partID)
			if gErr == nil {
				log.Debugf("Commit operation on delete transaction failed on key: %s", key, err)
				k.setOldValue(key, currentValue, partID, succesfullyDeleted)
			}
			return err
		}
		succesfullyDeleted = append(succesfullyDeleted, addr)
	}
	// delete the key from partition table, now
	return k.partitions.delete(key, partID)
}

func (k *KV) redirectDelete(key, oAddr string) error {
	conn, err := k.partman.GetMemberConn(oAddr)
	if err != nil {
		return err
	}
	c := ksrv.NewKVClient(conn)
	dr := &ksrv.DeleteRequest{
		Key: key,
	}
	_, err = c.Delete(context.Background(), dr)
	if err != nil {
		return err
	}

	return nil
}
