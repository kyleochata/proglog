package loadbalance_test

import (
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/kyleochata/proglog/api/v1"
	"github.com/kyleochata/proglog/internal/config"
	"github.com/kyleochata/proglog/internal/loadbalance"
	"github.com/kyleochata/proglog/internal/server"
)

func TestResolver(t *testing.T) {
	//sets up server for resolver to try and discover services.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)
	srv, err := server.NewGRPCServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))
	require.NoError(t, err)
	go srv.Serve(l)

	//create and build the test resolver and configure target enpint to point to the server set up above. Resolver calls GetServers() to resolve the servers and updatet he cc with the server's addr
	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}
	r := &loadbalance.Resolver{}
	// Construct the target URL with scheme, host, and path for the address
	targetURL := url.URL{
		// Scheme: r.Scheme(),
		Path: l.Addr().String(), // Address of your server
	}

	_, err = r.Build(
		resolver.Target{URL: targetURL},
		conn,
		opts,
	)
	require.NoError(t, err)

	// check that the resolver updated the client connection with the servers and data expected. Resolver is to find two servers and mark 9001 as the leader.
	wantState := resolver.State{
		Addresses: []resolver.Address{
			{
				Addr:       "localhost:9001",
				Attributes: attributes.New("is_leader", true),
			},
			{
				Addr:       "localhost:9002",
				Attributes: attributes.New("is_leader", false),
			},
		},
	}
	require.Equal(t, wantState, conn.state)
	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

// getServers implements GetServerers, to return a known server set for the resolver to find.
type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{
			Id:       "leader",
			RpcAddr:  "localhost:9001",
			IsLeader: true,
		}, {
			Id:      "follower",
			RpcAddr: "localhost:9002",
		},
	}, nil
}

// clientConn implements resolver.ClientConn and keeps a reference to the state the resolver updated it with so it can verify that the resolver updates the client connection with the correct data
type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
