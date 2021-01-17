// Copyright 2017 David Ackroyd. All Rights Reserved.
// See LICENSE for licensing terms.

package recovery_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/testing/testpb"
)

type recoveryAssertService struct {
	testpb.TestServiceServer
}

func (s *recoveryAssertService) Ping(ctx context.Context, ping *testpb.PingRequest) (*testpb.PingResponse, error) {
	if ping.Value == "panic" {
		panic("very bad thing happened")
	}
	return s.TestServiceServer.Ping(ctx, ping)
}

func (s *recoveryAssertService) PingList(ping *testpb.PingListRequest, stream testpb.TestService_PingListServer) error {
	if ping.Value == "panic" {
		panic("very bad thing happened")
	}
	return s.TestServiceServer.PingList(ping, stream)
}

func TestRecoverySuite(t *testing.T) {
	s := &RecoverySuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &recoveryAssertService{TestServiceServer: &testpb.TestPingService{T: t}},
			ServerOpts: []grpc.ServerOption{
				middleware.WithStreamServerChain(
					recovery.StreamServerInterceptor()),
				middleware.WithUnaryServerChain(
					recovery.UnaryServerInterceptor()),
			},
		},
	}
	suite.Run(t, s)
}

type RecoverySuite struct {
	*testpb.InterceptorTestSuite
}

func (s *RecoverySuite) TestUnary_SuccessfulRequest() {
	_, err := s.Client.Ping(s.SimpleCtx(), testpb.GoodPing)
	require.NoError(s.T(), err, "no error must occur")
}

func (s *RecoverySuite) TestUnary_PanickingRequest() {
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "panic"})
	require.Error(s.T(), err, "there must be an error")
	assert.Equal(s.T(), codes.Internal, status.Code(err), "must error with internal")
	assert.Equal(s.T(), "very bad thing happened", status.Convert(err).Message(), "must error with message")
}

func (s *RecoverySuite) TestStream_SuccessfulReceive() {
	stream, err := s.Client.PingList(s.SimpleCtx(), testpb.GoodPingList)
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	pong, err := stream.Recv()
	require.NoError(s.T(), err, "no error must occur")
	require.NotNil(s.T(), pong, "pong must not be nil")
}

func (s *RecoverySuite) TestStream_PanickingReceive() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "panic"})
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	_, err = stream.Recv()
	require.Error(s.T(), err, "there must be an error")
	assert.Equal(s.T(), codes.Internal, status.Code(err), "must error with internal")
	assert.Equal(s.T(), "very bad thing happened", status.Convert(err).Message(), "must error with message")
}

func TestRecoveryOverrideSuite(t *testing.T) {
	opts := []recovery.Option{
		recovery.WithRecoveryHandler(func(p interface{}) (err error) {
			return status.Errorf(codes.Unknown, "panic triggered: %v", p)
		}),
	}
	s := &RecoveryOverrideSuite{
		InterceptorTestSuite: &testpb.InterceptorTestSuite{
			TestService: &recoveryAssertService{TestServiceServer: &testpb.TestPingService{T: t}},
			ServerOpts: []grpc.ServerOption{
				middleware.WithStreamServerChain(
					recovery.StreamServerInterceptor(opts...)),
				middleware.WithUnaryServerChain(
					recovery.UnaryServerInterceptor(opts...)),
			},
		},
	}
	suite.Run(t, s)
}

type RecoveryOverrideSuite struct {
	*testpb.InterceptorTestSuite
}

func (s *RecoveryOverrideSuite) TestUnary_SuccessfulRequest() {
	_, err := s.Client.Ping(s.SimpleCtx(), testpb.GoodPing)
	require.NoError(s.T(), err, "no error must occur")
}

func (s *RecoveryOverrideSuite) TestUnary_PanickingRequest() {
	_, err := s.Client.Ping(s.SimpleCtx(), &testpb.PingRequest{Value: "panic"})
	require.Error(s.T(), err, "there must be an error")
	assert.Equal(s.T(), codes.Unknown, status.Code(err), "must error with unknown")
	assert.Equal(s.T(), "panic triggered: very bad thing happened", status.Convert(err).Message(), "must error with message")
}

func (s *RecoveryOverrideSuite) TestStream_SuccessfulReceive() {
	stream, err := s.Client.PingList(s.SimpleCtx(), testpb.GoodPingList)
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	pong, err := stream.Recv()
	require.NoError(s.T(), err, "no error must occur")
	require.NotNil(s.T(), pong, "pong must not be nil")
}

func (s *RecoveryOverrideSuite) TestStream_PanickingReceive() {
	stream, err := s.Client.PingList(s.SimpleCtx(), &testpb.PingListRequest{Value: "panic"})
	require.NoError(s.T(), err, "should not fail on establishing the stream")
	_, err = stream.Recv()
	require.Error(s.T(), err, "there must be an error")
	assert.Equal(s.T(), codes.Unknown, status.Code(err), "must error with unknown")
	assert.Equal(s.T(), "panic triggered: very bad thing happened", status.Convert(err).Message(), "must error with message")
}
