package server

import (
	"context"
	"fmt"
	"io"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcAuth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	api "github.com/kyleochata/proglog/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

// var _ api.LogServer = (*grpcServer)(nil)

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
	// offset, err := s.CommitLog.Append(req.Record)
	// if err != nil {
	// 	return nil, err
	// }
	// return &api.ProduceResponse{Offset: offset}, nil
	if err := s.Config.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume will take in a client request with the desired record's offset and Returns the record from the server's log.
func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	// record, err := s.CommitLog.Read(req.Offset)
	if err := s.Config.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}
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

type Authorizer interface {
	Authorize(subject, object, action string) error
}

// NewGRPCServer instantiates service, create gRPC server, and register service to the server (gives user a server that just needs a listener for it to accept incoming connections)
// authenticate intercepter added to id the subject of each rpc and init authorization process
func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts, grpc.StreamInterceptor(
		grpcMiddleware.ChainStreamServer(
			grpcAuth.StreamServerInterceptor(authenticate),
		)), grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(grpcAuth.UnaryServerInterceptor(authenticate))),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// authenticate is an interceptor that reads the subject out of the client's cert and writes it to the RPC's context.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}
	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)
	return ctx, nil
}

// subject returns the client's certificates' subject to identify a client and check their access.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
