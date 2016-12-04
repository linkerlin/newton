package kv

import (
	"github.com/purak/newton/log"
	"golang.org/x/net/context"

	ksrv "github.com/purak/newton/proto/kv"
)

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

	ms, err := k.partman.FindBackupOwners(partID)
	if err != nil {
		return err
	}
	s := []string{}
	for _, bAddr := range ms {
		log.Debugf("Calling TransactionForDelete for %s on %s", key, bAddr)
		if err := k.callTransactionForDeleteOn(bAddr, key, partID); err != nil {
		}
		s = append(s, bAddr)
	}
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
