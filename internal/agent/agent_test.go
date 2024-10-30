package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/kyleochata/proglog/api/v1"
	"github.com/kyleochata/proglog/internal/agent"
	"github.com/kyleochata/proglog/internal/config"
	"github.com/kyleochata/proglog/internal/loadbalance"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	//set up a 3 node cluster. Second and third join the first node's cluster
	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		//one port for gRPC log connections and other port for Serf service discovery connections
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]
		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)
		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}
		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			Bootstrap:       i == 0,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

	defer cancel()
	produceResponse, err := leaderClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("test-testerson"),
			},
		},
	)
	fmt.Println("Produce offset:", produceResponse.Offset)
	require.NoError(t, err)

	//wait until replication has finished
	//before we had the sleep above the followerClient because the lead client originally was connected to the lead server and didn't have to wait for replication on Consume calls.
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumeResponse, err := leaderClient.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("test-testerson"))

	followerClient := client(t, agents[1], peerTLSConfig)

	//Wait for lead to replicate when leadClient is connected to lead server
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	consumeResponse, err = followerClient.Consume(
		ctx,
		&api.ConsumeRequest{
			Offset: produceResponse.Offset,
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Record.Value, []byte("test-testerson"))

	//Fail when added. Produce only one record to the service and we're able to consume multiple records from teh original server because it's replicated data from another server that replicated its data from the original server.
	//Need to have a defined leader-follower relationship.
	consumeResponse, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Offset: produceResponse.Offset + 1,
		},
	)
	require.Nil(t, consumeResponse)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	//specify scheme in the URL so gRPC knows to use the resolver
	conn, err := grpc.NewClient(fmt.Sprintf("%s:///%s", loadbalance.Name, rpcAddr), opts...)

	require.NoError(t, err)
	client := api.NewLogClient(conn)
	return client
}
