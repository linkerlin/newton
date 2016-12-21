package kv

import (
	"github.com/purak/newton/log"
	"golang.org/x/net/context"

	ksrv "github.com/purak/newton/proto/kv"
)

// Undo the transaction in a safe manner. We assume that the underlying partition manager works consistently.
func (k *KV) undoTransactionForDelete(addresses []string, key string, partID int32) {
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
				log.Debugf("%s is no longer a participant for PartitionID: %d. Skip rollbackTransactionForDelete.", address, partID)
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
		if len(tmp) == 0 {
			break
		}
		addresses = tmp
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

	ms, err := k.partman.FindBackupOwners(partID)
	if err != nil {
		return err
	}

	err = k.partitions.delete(key, partID)
	if err != nil {
		return err
	}

	// Start a new transaction and undo it if one of the participants of partition sends negative acknowledgement.
	s := []string{}
	for _, bAddr := range ms {
		log.Debugf("Calling TransactionForDelete for %s on %s", key, bAddr)
		if err := k.callTransactionForDeleteOn(bAddr, key, partID); err != nil {
			// Undo transaction for delete. It should remove the transaction from cluster
			// if the underlying partition manager works consistently.
			k.undoTransactionForDelete(s, key, partID)
			return err
		}
		s = append(s, bAddr)
	}
	// Commit the transaction. If it's fails, re-set the key/value.
	c := []string{}
	for _, bAddr := range ms {
		log.Debugf("Calling CommitTransactionForDelete for %s on %s", key, bAddr)
		if err := k.callCommitTransactionForDeleteOn(bAddr, key, partID); err != nil {
			// re-set the key.
			log.Debugf("Commit operation on delete transaction failed on key: %s Re-setting the old value: %s", key, err)
			//k.setOldItem(key, i.ttl, partID, i, i, c)
			return err
		}
		c = append(c, bAddr)
	}
	// delete the key from partition table, now
	return k.partitions.deleteCommit(key, partID)
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
