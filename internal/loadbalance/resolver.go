package loadbalance

import (
	"context"
	"fmt"
	"sync"

	api "github.com/kyleochata/proglog/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

// Resolver is the type implemented into gRPC's resolver.Builder and .Resolver interfaces.
type Resolver struct {
	mu sync.Mutex

	//clientConn is the user's connection and gRPC passes it to the resolver for the resolver to update with the servers it discovers.
	clientConn resolver.ClientConn

	//resolverConn is the resolver's own client connection to the server so it can call GetServers() and retrieve the servers in the cluster.
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

var _ resolver.Builder = (*Resolver)(nil)

// Build is used to implement the resolver.Builder interface. Build receives the data needed to build a resolver that can discover the servers and the client connection the resolver will update the servers it discovers. Sets up a client connection to the server so the resolver can call the GetServers() API
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(
		fmt.Sprintf(`{"loadBalancingConfig":[{"%s": {}}]}`, Name),
	)
	var err error
	r.resolverConn, err = grpc.NewClient(target.Endpoint(), dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

// Scheme is used to implement the resolver.Builder interface. Schem returns the resolver's scheme identifier. During grpc.Dial, gRPC parses out the scheme from the target address and tries to find a resolver that matches, defaulting to the DNS resolver. "proglog://your-service-address"
func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

// resolver.Resolver has ResolveNow() and Close() methods.
var _ resolver.Resolver = (*Resolver)(nil)

// ResolveNow implements resolver.Resolver. gRPC calls ResolveNow() to resolve the target, discover the servers and update the client connection with the servers
func (r *Resolver) ResolveNow(resolver.ResolveNowOptions) {
	// gRPC may call ResolveNow() concurrently so sync.Mutex to protect access across goroutines
	r.mu.Lock()
	defer r.mu.Unlock()

	client := api.NewLogClient(r.resolverConn)
	//get cluster and then set cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	// services can specify how clients should balance their calls to the service by updating the state with a service config. Update the state with a service config that specifies to use the "proglog" load balancer.
	// update state with a slice of resolver.Address to inform the load balancer what servers it can choose from
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			// address of server to connect to
			Addr: server.RpcAddr,
			// (optional) Attributes is a map containing any data for the load balancer
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}

	// once servers are discovered, update client connection with UpdateState() with the resolver.Address's. Addresses have the data in the api.Server's
	r.clientConn.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close closes the resolver and helps resolve implement the resolver.Resolver interface. Closes the connection to the server made in Build()
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}
