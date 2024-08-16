package main

import (
	"context"

	connect "github.com/bufbuild/connect-go"
	"github.com/tierklinik-dobersberg/apis/pkg/cli"
)

type authInterceptor struct {
	root *cli.Root
}

func NewAuthInterceptor(root *cli.Root) *authInterceptor {
	return &authInterceptor{
		root: root,
	}
}

func (i *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	// Same as previous UnaryInterceptorFunc.
	return connect.UnaryFunc(func(
		ctx context.Context,
		req connect.AnyRequest,
	) (connect.AnyResponse, error) {
		if req.Spec().IsClient {
			// Send a token with client requests.
			req.Header().Set("Authentication", "Bearer "+i.root.Tokens().AccessToken)
		}
		return next(ctx, req)
	})
}

func (i *authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return connect.StreamingClientFunc(func(
		ctx context.Context,
		spec connect.Spec,
	) connect.StreamingClientConn {
		conn := next(ctx, spec)

		conn.RequestHeader().Set("Authentication", "Bearer "+i.root.Tokens().AccessToken)

		return conn
	})
}

func (i *authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}
