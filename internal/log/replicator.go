package log

import (
	"context"
	"sync"

	api "github.com/kyleochata/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Replicator connects to other servers with the gRPC client.
type Replicator struct {
	//Options to configure the client
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex

	//map of server addresses to a channel, which the replicator uses to stop replicating from a server when the server fails or leaves the cluster
	servers map[string]chan struct{}
	closed  bool
	close   chan struct{}
}

// Join adds the given server address to the list of servers to replicate. Starts go routine for the actual replication logic
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		//already replicating so skip
		return nil
	}
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

// replicate creates a client and open up a stream to consume all logs on the server.
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to create new client", addr)
		return
	}
	defer cc.Close()
	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	//loop consumes logs from the discovered server in a stream and produces to the local server to save a copy. Replicate messages from the other server until that server fails or leaves the cluster and the replicator closes the channel for that server. Breaks the loop and ends the replicate() goroutine.
	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
		case record := <-records:
			_, err = r.LocalServer.Produce(ctx,
				&api.ProduceRequest{
					Record: record,
				},
			)
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// init lazy inititializes the server map. lazy initialization gives structs useful zero value to reduce api's size and complexity while maintaining functionality. Without the zero value, must export a replicator constructor function for the user to call or export the servers field on the replicator struct for the user to set.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close closes the replicator so it doesn't replicate new servers that join the cluster and it stops replicating existing servers by causing the replicate() goroutine to return
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// logError logs the errors (can expose these errors by exporting an error channel and send the errors into it for users to receive and handle)
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
