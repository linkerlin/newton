package kv

import (
	ksrv "github.com/purak/newton/proto/kv"
	"golang.org/x/net/context"
)

type Grpc struct {
	kv *KV
}

func (g *Grpc) Set(ctx context.Context, in *ksrv.SetRequest) (*ksrv.SetResponse, error) {
	err := g.kv.Set(in.Key, in.Value, in.Ttl)
	if err != nil {
		return nil, err
	}
	return &ksrv.SetResponse{}, nil
}

func (g *Grpc) Get(ctx context.Context, in *ksrv.GetRequest) (*ksrv.GetResponse, error) {
	value, err := g.kv.Get(in.Key)
	if err != nil {
		return nil, err
	}
	return &ksrv.GetResponse{
		Value: value,
	}, nil
}

func (g *Grpc) Delete(ctx context.Context, in *ksrv.DeleteRequest) (*ksrv.DeleteResponse, error) {
	err := g.kv.Delete(in.Key)
	if err != nil {
		return nil, err
	}
	return &ksrv.DeleteResponse{}, nil
}

func (g *Grpc) TransactionForSet(ctx context.Context, in *ksrv.TransactionForSetRequest) (*ksrv.TransactionForSetResponse, error) {
	err := g.kv.transactionForSet(in.Key, in.Value, in.Ttl, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionForSetResponse{}, nil
}

func (g *Grpc) CommitTransactionForSet(ctx context.Context, in *ksrv.TransactionQueryRequest) (*ksrv.TransactionQueryResponse, error) {
	err := g.kv.commitTransactionForSet(in.Key, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionQueryResponse{}, nil
}

func (g *Grpc) RollbackTransactionForSet(ctx context.Context, in *ksrv.TransactionQueryRequest) (*ksrv.TransactionQueryResponse, error) {
	err := g.kv.rollbackTransactionForSet(in.Key, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionQueryResponse{}, nil
}

func (g *Grpc) TransactionForDelete(ctx context.Context, in *ksrv.TransactionForDeleteRequest) (*ksrv.TransactionForDeleteResponse, error) {
	err := g.kv.transactionForDelete(in.Key, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionForDeleteResponse{}, nil
}

func (g *Grpc) CommitTransactionForDelete(ctx context.Context, in *ksrv.TransactionQueryRequest) (*ksrv.TransactionQueryResponse, error) {
	err := g.kv.commitTransactionForDelete(in.Key, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionQueryResponse{}, nil
}

func (g *Grpc) RollbackTransactionForDelete(ctx context.Context, in *ksrv.TransactionQueryRequest) (*ksrv.TransactionQueryResponse, error) {
	err := g.kv.rollbackTransactionForDelete(in.Key, in.PartitionID)
	if err != nil {
		return nil, err
	}
	return &ksrv.TransactionQueryResponse{}, nil
}
