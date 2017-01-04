package kv

import (
	"encoding/binary"

	"golang.org/x/net/context"

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
	defer k.locker.Unlock(key)

	// We have the key. So you can update eviction data.
	if k.config.EvictionPolicy == evictionLRU {
		if err = k.partitions.check(key, partID); err != nil {
			return nil, err
		}

		pos, err := k.setLRUItem(key, partID)
		if err != nil {
			return nil, err
		}
		// Update bookkeeping data
		rrange := "-8"
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, pos)
		if err = k.partitions.modify(key, rrange, bs, partID); err != nil {
			return nil, err
		}
		value, err := k.partitions.find(key, partID)
		if err != nil {
			return nil, err
		}
		return value[:len(value)-8], nil
	}
	return k.partitions.find(key, partID)
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
