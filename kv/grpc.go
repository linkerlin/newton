package kv

import (
	ksrv "github.com/purak/newton/proto/kv"
	"golang.org/x/net/context"
)

type Grpc struct {
	kv *KV
}

func (g *Grpc) Set(ctx context.Context, in *ksrv.SetRequest) (*ksrv.SetResponse, error) {
	err := g.kv.Set(in.Key, in.Value)
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
