package kv

import (
	"golang.org/x/net/context"

	"github.com/purak/newton/log"
	ksrv "github.com/purak/newton/proto/kv"
)

func (k *KV) Get(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return nil, err
	}
	if !local {
		return k.redirectGet(key, oAddr)
	}

	k.locker.Lock(key)
	defer func() {
		if err = k.locker.Unlock(key); err != nil {
			log.Errorf("Error while unlocking key %s: %s", key, err)
		}
	}()
	value, err := k.partitions.find(key, partID)
	if err != nil {
		return nil, err
	}
	isExpired, err := k.isValueExpired(value)
	if err != nil {
		return nil, err
	}
	if isExpired {
		return nil, ErrKeyNotFound
	}
	return value, nil
}

func (k *KV) redirectGet(key, oAddr string) ([]byte, error) {
	// Redirect the request to responsible node.
	conn, err := k.partman.GetMemberConn(oAddr)
	if err != nil {
		return nil, err
	}
	c := ksrv.NewKVClient(conn)
	gr := &ksrv.GetRequest{
		Key: key,
	}
	res, err := c.Get(context.Background(), gr)
	if err != nil {
		return nil, err
	}
	return res.Value, nil

}
