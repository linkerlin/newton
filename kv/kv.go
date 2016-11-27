package kv

import (
	"context"
	"sync"

	"github.com/julienschmidt/httprouter"
	"github.com/purak/newton/log"
	"github.com/purak/newton/partition"
	ksrv "github.com/purak/newton/proto/kv"
	"github.com/spaolacci/murmur3"
)

// KV defines a distributed key-value store.
type KV struct {
	waitGroup sync.WaitGroup
	done      chan struct{}
	partman   *partition.Partition

	partitions *partitions
	backups    *partitions
	Grpc       *Grpc
}

func New(p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]*kv),
	}
	backups := &partitions{
		m: make(map[int32]*kv),
	}

	k := &KV{
		done:       make(chan struct{}),
		partman:    p,
		partitions: parts,
		backups:    backups,
	}
	g := &Grpc{
		kv: k,
	}
	k.Grpc = g

	router.POST("/kv/:key", k.setHandler)
	router.PUT("/kv/:key", k.setHandler)
	router.GET("/kv/:key", k.getHandler)
	router.DELETE("/kv/:key", k.deleteHandler)
	return k
}

func (k *KV) Stop() {
	select {
	case <-k.done:
		// Already closed
		return
	default:
	}
	close(k.done)
}

func (k *KV) rollbackBackups(addresses []string, key string) error {
	for _, address := range addresses {
		if err := k.callDeleteBackupOn(address, key); err != nil {
			// TODO: retry this.
			log.Errorf("Error while calling DeleteBackup on %s: %s", address, err)
			return err
		}
	}
	return nil
}

func (k *KV) setBackups(partID int32, key string, value []byte) error {
	ms, err := k.partman.FindBackupOwners(partID)
	if err != nil {
		return err
	}
	s := []string{}
	for _, bAddr := range ms {
		log.Debugf("Calling SetBackup for %s on %s", key, bAddr)
		if err := k.callSetBackupOn(bAddr, key, value); err != nil {
			if rErr := k.rollbackBackups(s, key); rErr != nil {
				return rErr
			}
			return err
		}
		s = append(s, bAddr)
	}
	return nil
}

func (k *KV) Set(key string, value []byte) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return err
	}
	if local {
		item := k.partitions.set(key, value, partID)
		defer item.mu.Unlock()
		if err := k.setBackups(partID, key, value); err != nil {
			// Stale item should be removed by garbage collector component of KV, if a client
			// does not try to set the key again shortly after the failure.
			item.stale = true
			return err
		}
		return nil
	}

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

func (k *KV) Get(key string) ([]byte, error) {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return nil, err
	}
	if local {
		return k.partitions.get(key, partID)
	}
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

func (k *KV) Delete(key string) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	oAddr, local, err := k.partman.FindPartitionOwner(partID)
	if err != nil {
		return err
	}
	if local {
		ms, err := k.partman.FindBackupOwners(partID)
		if err != nil {
			return err
		}
		for _, bAddr := range ms {
			log.Debugf("Calling DeleteBackup for %s on %s", key, bAddr)
			if err := k.callDeleteBackupOn(bAddr, key); err != nil {
				// TODO: What about stale items in kv?
				return err
			}
		}
		return k.partitions.delete(key, partID)
	}

	// Redirect the request to responsible node.
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

func getPartitionID(key string) int32 {
	data := []byte(key)
	hasher := murmur3.New64()
	hasher.Write(data)
	h := hasher.Sum64()
	// TODO: Get partition count from configuration
	partID := h % 23
	return int32(partID)
}
