package newton

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/purak/newton/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

type internalGRPC struct {
	startChan chan struct{}
	server    *grpc.Server
	listener  net.Listener
	address   string
	waitGroup sync.WaitGroup
	done      chan struct{}
}

func (g *internalGRPC) checkgRPCAliveness() {
	defer g.waitGroup.Done()

	callYourself := func() error { // Set up a connection to the server.
		conn, err := grpc.Dial(g.address, grpc.WithInsecure())
		if err != nil {
			return err
		}
		defer func() {
			if err := conn.Close(); err != nil {
				log.Errorf("Error while closing connection: %s", err)
			}
		}()
		c := grpc_health_v1.NewHealthClient(conn)
		r, err := c.Check(context.Background(), &grpc_health_v1.HealthCheckRequest{})
		if err != nil {
			return err
		}
		if r.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			return errors.New("not ready yet")
		}
		return nil
	}

	for {
		select {
		case <-time.After(50 * time.Millisecond):
			if err := callYourself(); err != nil {
				log.Errorf("Error while checking gRPC server: %s", err)
				continue
			}
			close(g.startChan)
			log.Infof("gRPC server runs on %s", g.address)
			return
		case <-g.done:
			return
		}
	}
}

func newInternalGRPC(address string) (*internalGRPC, error) {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	return &internalGRPC{
		startChan: make(chan struct{}),
		server:    grpc.NewServer(),
		listener:  ln,
		address:   address,
		done:      make(chan struct{}),
	}, nil
}

func (g *internalGRPC) start() error {
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(g.server, healthServer)
	g.waitGroup.Add(1)
	go g.checkgRPCAliveness()

	err := g.server.Serve(g.listener)
	if err != nil {
		if opErr, ok := err.(*net.OpError); !ok || (ok && opErr.Op != "accept") {
			log.Debugf("Something wrong with gRPC server: %s", err)
			// We call Close here because if HTTP/2 server fails, we cannot run this process anymore.
			// This block can be invoked during normal closing process but Close method will handle
			// redundant Close call.
			return err
		}
		// It's just like this: "accept tcp [::]:10000: use of closed network connection". Ignore it.
		return nil
	}
	return nil
}

func (g *internalGRPC) gracefulStop() {
	select {
	case <-g.done:
		// Already closed
		return
	default:
	}
	close(g.done)
	g.server.GracefulStop()
}
