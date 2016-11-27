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

	router.POST("/kv/set/:key", k.setHandler)
	router.PUT("/kv/set/:key", k.setHandler)
	router.GET("/kv/get/:key", k.getHandler)
	router.DELETE("/kv/delete/:key", k.deleteHandler)
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

func (k *KV) Set(key string, value []byte) error {
	// Find partition number for the given key
	partID := getPartitionID(key)
	rAddr, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return err
	}
	if local {
		item := k.partitions.set(key, value, partID)
		defer item.mu.Unlock()
		ms, err := k.partman.FindBackupMembers(partID)
		if err != nil {
			return err
		}
		for _, bAddr := range ms {
			log.Debugf("Setting backup for %s on %s", key, bAddr)
		}
		return nil
	}

	// Redirect the request to responsible node.
	conn, err := k.partman.GetMemberConn(rAddr)
	if err != nil {
		// TODO: Do we need to remove stale item explicitly from kv?
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
	rAddr, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return nil, err
	}
	if local {
		return k.partitions.get(key, partID)
	}
	// Redirect the request to responsible node.
	conn, err := k.partman.GetMemberConn(rAddr)
	if err != nil {
		// TODO: Do we need to remove stale item explicitly from kv?
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
	rAddr, local, err := k.partman.FindResponsibleMember(partID)
	if err != nil {
		return err
	}
	if local {
		return k.partitions.delete(key, partID)
	}

	// Redirect the request to responsible node.
	conn, err := k.partman.GetMemberConn(rAddr)
	if err != nil {
		// TODO: Do we need to remove stale item explicitly from kv?
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
