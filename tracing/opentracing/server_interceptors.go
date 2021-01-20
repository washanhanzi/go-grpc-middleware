// Copyright 2017 Michal Witkowski. All Rights Reserved.
// See LICENSE for licensing terms.

package grpc_opentracing

import (
	"context"
	"errors"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/opentracing/opentracing-go/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	grpcTag = opentracing.Tag{Key: string(ext.Component), Value: "gRPC"}
)

// UnaryServerInterceptor returns a new unary server interceptor for OpenTracing.
func UnaryServerInterceptor(opts ...Option) grpc.UnaryServerInterceptor {
	o := evaluateOptions(opts)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if o.filterOutFunc != nil && !o.filterOutFunc(ctx, info.FullMethod) {
			return handler(ctx, req)
		}
		opName := info.FullMethod
		if o.opNameFunc != nil {
			opName = o.opNameFunc(info.FullMethod)
		}
		spCtx, err := extractSpan(ctx, o.tracer)
		//skip trace
		if err != nil {
			grpclog.Infof("grpc_opentracing: failed parsing trace information: %v", err)
			return handler(ctx, req)
		}

		serverSpan := o.tracer.StartSpan(
			opName,
			opentracing.ChildOf(spCtx),
			grpcTag,
		)

		injectOpentracingIdsToTags(o.traceHeaderName, serverSpan, grpc_ctxtags.Extract(ctx))
		newCtx := opentracing.ContextWithSpan(ctx, serverSpan)
		if o.unaryRequestHandlerFunc != nil {
			o.unaryRequestHandlerFunc(serverSpan, req)
		}
		resp, err := handler(newCtx, req)
		finishServerSpan(ctx, serverSpan, err)
		return resp, err
	}
}

// StreamServerInterceptor returns a new streaming server interceptor for OpenTracing.
func StreamServerInterceptor(opts ...Option) grpc.StreamServerInterceptor {
	o := evaluateOptions(opts)
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if o.filterOutFunc != nil && !o.filterOutFunc(stream.Context(), info.FullMethod) {
			return handler(srv, stream)
		}
		opName := info.FullMethod
		if o.opNameFunc != nil {
			opName = o.opNameFunc(info.FullMethod)
		}
		newCtx, serverSpan, err := newServerSpanFromInbound(stream.Context(), o.tracer, o.traceHeaderName, opName)
		//skip trace
		if err != nil {
			return handler(srv, stream)
		}
		wrappedStream := grpc_middleware.WrapServerStream(stream)
		wrappedStream.WrappedContext = newCtx
		err = handler(srv, wrappedStream)
		finishServerSpan(newCtx, serverSpan, err)
		return err
	}
}

func extractSpan(ctx context.Context, tracer opentracing.Tracer) (opentracing.SpanContext, error) {
	md := metautils.ExtractIncoming(ctx)
	spCtx, err := tracer.Extract(opentracing.HTTPHeaders, metadataTextMap(md))
	return spCtx, err
}

func newServerSpanFromInbound(ctx context.Context, tracer opentracing.Tracer, traceHeaderName, opName string) (context.Context, opentracing.Span, error) {
	md := metautils.ExtractIncoming(ctx)
	parentSpanContext, err := tracer.Extract(opentracing.HTTPHeaders, metadataTextMap(md))
	if err != nil || err != opentracing.ErrSpanContextNotFound {
		grpclog.Infof("grpc_opentracing: failed parsing trace information: %v", err)
		return nil, nil, errors.New("failed parse span")
	}

	serverSpan := tracer.StartSpan(
		opName,
		opentracing.ChildOf(parentSpanContext),
		grpcTag,
	)

	injectOpentracingIdsToTags(traceHeaderName, serverSpan, grpc_ctxtags.Extract(ctx))
	return opentracing.ContextWithSpan(ctx, serverSpan), serverSpan, nil
}

func finishServerSpan(ctx context.Context, serverSpan opentracing.Span, err error) {
	// Log context information
	tags := grpc_ctxtags.Extract(ctx)
	for k, v := range tags.Values() {
		// Don't tag errors, log them instead.
		if vErr, ok := v.(error); ok {
			serverSpan.LogKV(k, vErr.Error())
		} else {
			serverSpan.SetTag(k, v)
		}
	}
	if err != nil {
		ext.Error.Set(serverSpan, true)
		serverSpan.LogFields(log.String("event", "error"), log.String("message", err.Error()))
	}
	serverSpan.Finish()
}
