// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rest

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/oauth2"
	"golang.org/x/sync/singleflight"
)

// AuthManager is an interface for providing custom authorization headers.
type AuthManager interface {
	// AuthHeader returns the key and value for the authorization header.
	AuthHeader() (string, string, error)
}

// ContextAuthManager is an optional interface an AuthManager may implement to
// honor a caller-supplied context (deadline and cancellation) while producing
// the authorization header. sessionTransport prefers AuthHeaderWithContext when
// the manager implements it, so a request's deadline also bounds the auth step
// rather than only the request/response cycle. A manager implementing only
// AuthManager still works, but its auth step cannot be interrupted mid-call —
// bounding that case is the implementer's responsibility.
type ContextAuthManager interface {
	AuthManager
	// AuthHeaderWithContext returns the authorization header, honoring ctx.
	AuthHeaderWithContext(ctx context.Context) (string, string, error)
}

// Oauth2AuthManager is an implementation of the AuthManager interface which
// uses an oauth2.TokenSource to provide bearer tokens. The token source
// handles caching, thread-safe refresh, and expiry management.
type Oauth2AuthManager struct {
	tokenSource oauth2.TokenSource
	// refresh shares one in-flight Token call, so a caller that stops waiting
	// leaves nothing queued behind it.
	refresh singleflight.Group
}

var _ ContextAuthManager = (*Oauth2AuthManager)(nil)

// NewOauth2AuthManager returns an AuthManager that sends tokens from ts, for
// identity providers the built-in client-credentials flow can't reach, such as
// Google Application Default Credentials for BigLake. Pass the result to
// WithAuthManager.
//
// Like oauth2.NewClient, it wraps ts in oauth2.ReuseTokenSource, so ts is
// called only when the cached token is missing or expired. A token without an
// expiry is reused indefinitely. The header uses the token's type, and an
// *oauth2.RetrieveError from ts is reported as ErrOAuthError.
//
// The built-in flows bound each refresh with their own HTTP client timeout.
// A refresh through ts is bounded only by the HTTP client ts uses; for
// sources built from a context, such as google.DefaultTokenSource, that is
// the client in the context's oauth2.HTTPClient value, or
// http.DefaultClient, which has no timeout. AuthHeaderWithContext still
// returns when its context is done.
//
// NewOauth2AuthManager panics if ts is nil.
func NewOauth2AuthManager(ts oauth2.TokenSource) *Oauth2AuthManager {
	if ts == nil {
		panic("rest: NewOauth2AuthManager called with a nil oauth2.TokenSource")
	}

	return &Oauth2AuthManager{tokenSource: oauth2.ReuseTokenSource(nil, ts)}
}

// AuthHeader returns the authorization header with the bearer token.
func (o *Oauth2AuthManager) AuthHeader() (string, string, error) {
	tok, err, _ := o.refresh.Do("token", o.fetchToken)

	return o.authHeader(tok, err)
}

// AuthHeaderWithContext returns the authorization header, honoring ctx. A
// cached, unexpired token is returned without a network call. When a refresh
// is required, the caller waits for it only until ctx is done; the refresh
// itself keeps running, bounded by the token-refresh client's Timeout, and its
// result serves later callers.
func (o *Oauth2AuthManager) AuthHeaderWithContext(ctx context.Context) (string, string, error) {
	if err := ctx.Err(); err != nil {
		return "", "", err
	}

	select {
	case <-ctx.Done():
		return "", "", ctx.Err()
	case result := <-o.refresh.DoChan("token", o.fetchToken):
		return o.authHeader(result.Val, result.Err)
	}
}

// fetchToken adapts the token source to singleflight's untyped callback.
func (o *Oauth2AuthManager) fetchToken() (any, error) {
	return o.tokenSource.Token()
}

func (o *Oauth2AuthManager) authHeader(tok any, err error) (string, string, error) {
	if err != nil {
		if re, ok := errors.AsType[*oauth2.RetrieveError](err); ok {
			return "", "", oauthError{
				code: re.ErrorCode,
				desc: re.ErrorDescription,
				uri:  re.ErrorURI,
			}
		}

		return "", "", fmt.Errorf("%w: %s", ErrOAuthError, err)
	}

	token := tok.(*oauth2.Token)

	return "Authorization", token.Type() + " " + token.AccessToken, nil
}

// oauthError wraps OAuth2 error details and implements the error chain
// so that errors.Is(err, ErrOAuthError) returns true.
type oauthError struct {
	code string
	desc string
	uri  string
}

func (e oauthError) Error() string {
	msg := e.code
	if e.desc != "" {
		msg += ": " + e.desc
	}
	if e.uri != "" {
		msg += " (" + e.uri + ")"
	}

	return msg
}

func (e oauthError) Unwrap() error { return ErrOAuthError }
