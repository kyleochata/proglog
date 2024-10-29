package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/kyleochata/proglog/internal/auth"
	"github.com/kyleochata/proglog/internal/discovery"
	"github.com/kyleochata/proglog/internal/log"
	"github.com/kyleochata/proglog/internal/server"
)

// Agent runs on every service instance, setting up and connecting all the different components
type Agent struct {
	Config
	mux          cmux.CMux
	log          *log.DistributedLog
	server       *grpc.Server
	membership   *discovery.Membership
	replicator   *log.Replicator
	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	Bootstrap       bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New creates an Agent and runs a set of methods to set up and run the agent's components. After New, expect to have a running, functioning service.
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

// setupMux creates a listener on RPC address that'll accept both Raft and gRPC connections. Creates the mux with the listener. Mux will accept connections on that listener and match connections based on configured rules
func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

// setupLogger creates a new Zap Logger. It then calls ReplaceGlobals replaces the global and sugared logger.
func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// setupLog creates a Log service for the agent. (FOR non-multiplexed port)
//
//	func (a *Agent) setupLog() error {
//		var err error
//		a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
//		return err
//	}

// setupLog configures the rule to match Raft and create a distributed log. (for multiplexed port)
func (a *Agent) setupLog() error {
	//configure mux that matches Raft connections. Id Raft connections by reading one byte and checking that the byte matches the byte we set the ongoing Raft connection to write in the StreamLayer (internal/log/distributed)
	raftLn := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})

	//configure the distributed log's Raft to use the multiplexed listener and then configure and create the log.
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLn,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap
	var err error
	a.log, err = log.NewDistributedLog(a.Config.DataDir, logConfig)
	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

// setupServer uses the mux's listener to create the new GRPC server. Two connections are multiplexed(Raft, gRPC) and there is a matcher added for the raft connections. Know all other connections must be gRPC connections.
func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog:   a.log,
		Authorizer:  authorizer,
		GetServerer: a.log, //expose server endpoint that clients can call to get cluster's servers.
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	//cmux.Any() matches any connection (already know that all other connections outside of Raft and the matcher for Raft are gRPC)
	grpcLn := a.mux.Match(cmux.Any())

	//gRPC server to serve on the multiplexed listener.
	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
	// rpcAddr, err := a.RPCAddr()
	// if err != nil {
	// 	return err
	// }
	// ln, err := net.Listen("tcp", rpcAddr)
	// if err != nil {
	// 	return err
	// }
	// go func() {
	// 	if err := a.server.Serve(ln); err != nil {
	// 		_ = a.Shutdown()
	// 	}
	// }()
	// return err
}

// setupMembership dictates to the DistributedLog when servers join or leave the cluster.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName:       a.Config.NodeName,
		BindAddr:       a.Config.BindAddr,
		Tags:           map[string]string{"rpc_addr": rpcAddr},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
	// var opts []grpc.DialOption
	// if a.Config.PeerTLSConfig != nil {
	// 	opts = append(opts, grpc.WithTransportCredentials(
	// 		credentials.NewTLS(a.Config.PeerTLSConfig),
	// 	))
	// }
	// conn, err := grpc.NewClient(rpcAddr, opts...)
	// if err != nil {
	// 	return err
	// }
	// client := api.NewLogClient(conn)
	// a.replicator = &log.Replicator{
	// 	DialOptions: opts,
	// 	LocalServer: client,
	// }
	// a.membership, err = discovery.New(a.replicator, discovery.Config{
	// 	NodeName: a.Config.NodeName,
	// 	BindAddr: a.Config.BindAddr,
	// 	Tags: map[string]string{
	// 		"rpc_addr": rpcAddr,
	// 	},
	// 	StartJoinAddrs: a.Config.StartJoinAddrs,
	// })
	// return err
}

func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)
	shutdown := []func() error{
		a.membership.Leave,
		// a.replicator.Close, // no longer needed. Replication handled by Raft
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

// serve tells the mux to serve connections
func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
