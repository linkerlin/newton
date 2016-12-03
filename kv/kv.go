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

	partitions   *partitions
	transactions *transactions
	backups      *partitions
	Grpc         *Grpc
}

func New(p *partition.Partition, router *httprouter.Router) *KV {
	parts := &partitions{
		m: make(map[int32]*kv),
	}
	backups := &partitions{
		m: make(map[int32]*kv),
	}
	transactions := &transactions{
		set:    make(map[int32]*kv),
		delete: make(map[int32]*tDelete),
	}

	k := &KV{
		done:         make(chan struct{}),
		partman:      p,
		partitions:   parts,
		backups:      backups,
		transactions: transactions,
	}

	g := &Grpc{
		kv: k,
	}
	k.Grpc = g

	router.POST("/kv/:key", k.setHandler)
	router.PUT("/kv/:key", k.setHandler)
	router.GET("/kv/:key", k.getHandler)
	router.DELETE("/kv/:key", k.deleteHandler)

	k.waitGroup.Add(1)
	k.garbageCollector()

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

func (k *KV) undoTransactionForSet(addresses []string, key string, partID int32) error {
	for _, address := range addresses {
		// TODO: Check the address on partition table.
		if err := k.callRollbackTransactionForSetOn(address, key, partID); err != nil {
			log.Errorf("Error while calling RollbackTransactionForSet on %s: %s", address, err)
			return err
		}
	}
	return nil
}

func (k *KV) startTransactionForSet(addresses []string, partID int32, key string, value []byte) error {
	s := []string{}
	for _, bAddr := range addresses {
		log.Debugf("Calling TransactionForSet for %s on %s", key, bAddr)
		if err := k.callTransactionForSetOn(bAddr, key, value, partID); err != nil {
			if rErr := k.undoTransactionForSet(s, key, partID); rErr != nil {
				return rErr
			}
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

	addresses, err := k.partman.FindBackupOwners(partID)
	if err != nil {
		return err
	}

	item, oldItem := k.partitions.set(key, value, partID, ttl)
	defer item.mu.Unlock()
	if err := k.startTransactionForSet(addresses, partID, key, value); err != nil {
		// Stale item should be removed by garbage collector component of KV, if a client
		// does not try to set the key again shortly after the failure.
		item.stale = true
		return err
	}
	// Commit changes now. If you encoutner a problem during commit phase,
	// remove committed data or set the old one again.
	s := []string{}
	for _, bAddr := range addresses {
		log.Debugf("Calling CommitTransactionForSet for %s on %s", key, bAddr)
		if err := k.callCommitTransactionForSetOn(bAddr, key, partID); err != nil {
			if oldItem != nil {
				if tErr := k.startTransactionForSet(s, partID, key, oldItem.value); tErr != nil {
					// Stale item should be removed by garbage collector component of KV, if a client
					// does not try to set the key again shortly after the failure.
					item.stale = true
					return tErr
				}
				return err
			}
			// TODO: delete committed item
			return err
		}
		s = append(s, bAddr)
	}

	return nil
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
	return k.partitions.get(key, partID)
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

func getPartitionID(key string) int32 {
	data := []byte(key)
	hasher := murmur3.New64()
	hasher.Write(data)
	h := hasher.Sum64()
	// TODO: Get partition count from configuration
	partID := h % 23
	return int32(partID)
}
