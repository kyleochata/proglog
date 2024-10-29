package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	api "github.com/kyleochata/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

// NewDistributedLog returns a pointer to a new DistributedLog. Creates a log and then sets up Raft.
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	dl := &DistributedLog{config: config}
	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return dl, nil
}

// setupLog creates the log for this server and where the server will store the user's records
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft configures and creates the server's Raft instance
func (l *DistributedLog) setupRaft(dataDir string) error {
	//finite-state-machine(fsm) creation
	fsm := &fsm{log: l.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	logConfig.Segment.InitialOffset = 1 //required by Raft to start at 1

	//logStore stores commands given to Raft
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	//stableStore stores the cluster's configuration - servers in the cluster, addresses of those servers, etc. Bolt = embedded and persisted key-value database
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	retain := 1

	//snapshotStore stores compact snapshots of it's data. Recover and restore data efficiently. New servers will restore from snapshot and request the newest changes from the leader.
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(
		dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	//transport connects the server's peers. Wraps a stream layer
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	//LocalID = unique ID for server. Must be set, others are optional
	config.LocalID = l.config.Raft.LocalID
	//Overriding timeouts to speed up test

	//TODO: remove before deploy
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	//create the Raft instance and Bootstrap cluster
	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	//Bootstrap a server config with itself as the only voter, wait until it becomes the leader, and then tell the leader to add more servers to the cluster
	if l.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{ID: config.LocalID, Address: transport.LocalAddr()}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append appends the record to the log. Apply a command to Raft that tells the fsm to append the record to the log. Raft then replicates the command to a majority of the Raft servers.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply wraps Raft's API to apply requests and return their responses. Able to support multiple request types. Marshal the request type and request into bytes that is then used as the record's data Raft replicates.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (
	interface{},
	error,
) {
	var buf bytes.Buffer
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	// replicates the record and append the record to the leader's log
	future := l.raft.Apply(buf.Bytes(), timeout)
	//future.Error returns an error when err occurs with Raft's replication
	if future.Error() != nil {
		return nil, future.Error()
	}
	//future.Response returns fsm's Apply method returned
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read reads the record for the offset from the server's log. If ok with relaxed consistency, reads don't have to go through Raft for inc efficiency.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// Join adds the server to the Raft cluster. Every server added joins as a voter (AddNonVoter() API for non-vote status). Addition of non-voter servers are for replicating state to many servers to serve read only eventually consistent state.
// Each addition of a voter increases the probability that replications and elections will take longer because the leader has more servers it need to communicate with to reach a majority
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			// server has already joined
			return nil
		}
		// remove the existing server
		removeFuture := l.raft.RemoveServer(serverID, 0, 0)
		if err := removeFuture.Error(); err != nil {
			return err
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes the server from the cluster. Removing the leader will trigger an election. Raft will error and return ErrNotLeader when trying to change the cluster on non-leader nodes.
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader blocks until the cluster has elected a leader or times out.
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// Close shuts down the Raft instance and closes the local log.
func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}

// GetServers converts the data from Raft's raft.Server type into *api.Server for API to respond with.
func (l *DistributedLog) GetServers() ([]*api.Server, error) {
	future := l.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: l.raft.Leader() == server.Address,
		})
	}
	return servers, nil
}

// Raft defers the running of business logic to FSM
var _ raft.FSM = (*fsm)(nil)

// fsm must access the data it manages.
type fsm struct {
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Apply creates a RequestType and define append request type. Request Raft to apply it. Switch on the request type and call the corresponding method.
func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend unmarshal the request and then append the record to the local log and return the response for the Raft to send back to where we called raft.Apply() in Distributed-Log.Append()
func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot returns a FSMSnapshot that represetns a point-in-time snapshot of the FSM's state. (state == FSM's log). Call Reader() to return an io.Reader that will read all the log's data.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Persist is called to write its state to sink(file snapshotshore).
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release is called by Raft when it's finished with the snapshot
func (s *snapshot) Release() {}

// Restore resets the log and configures its inital offset to the first record's offset read from the snapshot so the log's offests match. Records are read from the snapshot and append them to the new log.
//
//	func (f *fsm) Restore(r io.ReadCloser) error {
//		b := make([]byte, lenWidth)
//		var buf bytes.Buffer
//		for i := 0; ; i++ {
//			_, err := io.ReadFull(r, b)
//			if err == io.EOF {
//				break
//			} else if err != nil {
//				return err
//			}
//			size := int64(enc.Uint64(b))
//			if _, err = io.CopyN(&buf, r, size); err != nil {
//				return err
//			}
//			record := &api.Record{}
//			if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
//				return err
//			}
//			if i == 0 {
//				f.log.Config.Segment.InitialOffset = record.Offset
//				if err := f.log.Reset(); err != nil {
//					return err
//				}
//			}
//			if _, err = f.log.Append(record); err != nil {
//				return err
//			}
//			buf.Reset()
//		}
//		return nil
//	}
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))
		data := make([]byte, size) // allocate a new buffer for each record
		if _, err = io.ReadFull(r, data); err != nil {
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(data, record); err != nil {
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		if _, err = f.log.Append(record); err != nil {
			return err
		}
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	off, err := l.HighestOffset()
	return off, err
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}
func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange removes records between the offsets. Remove records that're old or stored in a snapshot.
func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Dial makes outgoing connections to other servers in the Raft cluster. Once connected to a server, write the RaftRPC byte to identify the connection type to multiplex Raft on the same port as Log gRPC requests
func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}
	//identify to mux this is a raft rpc
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

// Accept is the mirror of Dial. Accept incoming connection and read the byte that id's the connection and then create the server-sid TLS connection
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

// Close closes the listener
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr returns the listener's address
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
