package kv

import (
	"encoding/binary"

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
	if k.config.Eviction {
		if err = k.partitions.check(key, partID); err != nil {
			return nil, err
		}

		// We have the key. So you can update eviction data.
		pos, err := k.setLRUItemOnSource(key, partID)
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
		// TODO: update bookkeeping data on partition members.
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
