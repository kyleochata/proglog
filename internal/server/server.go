package server

import (
	"context"
	"fmt"
	"io"

	api "github.com/kyleochata/proglog/api/v1"
	"google.golang.org/grpc"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{Config: config}
	return srv, nil
}

// Produce handles request form the client to produce/append to the server's log
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume will take in a client request with the desired record's offset and Returns the record from the server's log.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// ProduceStream is a bidirectional streaming RPC so the client can stream data into the server's log
// and the server can tell the client whether each request succeeded.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			// If the error is EOF, it means the stream has been closed properly
			if err == io.EOF {
				return stream.SendAndClose(&api.ProduceResponse{})
			}
			return fmt.Errorf("failed to receive request: %w", err) // Add more context
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return fmt.Errorf("failed to produce: %w", err) // Add more context
		}
		if err = stream.SendMsg(res); err != nil {
			return fmt.Errorf("failed to send response: %w", err) // Add more context
		}
	}
}

// ConsumeStream implements a server-side streaming RPC so the client an tell the server where in the log to read records and the server will stream every record that follows (even records that aren't logged yed). When the stream reaches the end of the log, it'll wait for a new append before streaming again.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// CommitLog allows for the service to use any log implementation.
type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

// NewGRPCServer instantiates service, create gRPC server, and register service to the server (gives user a server that just needs a listener for it to accept incoming connections)
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
