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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// AuthManager is an interface for providing custom authorization headers.
type AuthManager interface {
	// AuthHeader returns the key and value for the authorization header.
	AuthHeader() (string, string, error)
}

// AuthHeaderContext is an optional interface that an AuthManager may
// implement to support context-aware auth header retrieval. When the
// session transport calls AuthHeader, it will prefer this method if
// available, passing the request context so that token fetches can
// be cancelled.
type AuthHeaderContext interface {
	AuthHeaderCtx(ctx context.Context) (string, string, error)
}

// AuthRefresher is an optional interface that an AuthManager may implement
// to support refreshing credentials when authentication fails. When the
// session transport receives a 401, 403, or 419 response and the auth
// manager implements this interface, it will call RefreshAuth and retry
// the request once with the new credentials.
type AuthRefresher interface {
	// RefreshAuth forces a credential refresh, discarding any cached
	// tokens. After a successful refresh, AuthHeader will return the
	// new credentials.
	RefreshAuth(ctx context.Context) error
}

const (
	// maxExpiryThreshold is the maximum time before token expiry
	// to trigger a refresh.
	maxExpiryThreshold = 5 * time.Minute

	// minExpiryThreshold is the minimum time before token expiry
	// to trigger a refresh. This is the floor for very short-lived tokens.
	minExpiryThreshold = 5 * time.Second

	// statusAuthorizationExpired is a non-standard HTTP status code
	// used by some servers to indicate expired credentials.
	statusAuthorizationExpired = 419

	grantClientCredentials = "client_credentials"
	grantTokenExchange     = "urn:ietf:params:oauth:grant-type:token-exchange"
	grantRefreshToken      = "refresh_token"
	tokenTypeAccess        = "urn:ietf:params:oauth:token-type:access_token"
)

// expiryThreshold returns how far in advance of token expiry a
// refresh should be triggered. It uses a proportional buffer of
// 1/10th of the token lifetime, clamped to [5s, 5min].
func expiryThreshold(expiresIn time.Duration) time.Duration {
	threshold := expiresIn / 10
	if threshold < minExpiryThreshold {
		return minExpiryThreshold
	}
	if threshold > maxExpiryThreshold {
		return maxExpiryThreshold
	}

	return threshold
}

type oauthTokenResponse struct {
	AccessToken     string `json:"access_token"`
	IssuedTokenType string `json:"issued_token_type"`
	TokenType       string `json:"token_type"`
	ExpiresIn       int    `json:"expires_in"`
	Scope           string `json:"scope"`
	RefreshToken    string `json:"refresh_token"`
}

type oauthErrorResponse struct {
	Err     string `json:"error"`
	ErrDesc string `json:"error_description"`
	ErrURI  string `json:"error_uri"`
}

func (o oauthErrorResponse) Unwrap() error { return ErrOAuthError }
func (o oauthErrorResponse) Error() string {
	msg := o.Err
	if o.ErrDesc != "" {
		msg += ": " + o.ErrDesc
	}

	if o.ErrURI != "" {
		msg += " (" + o.ErrURI + ")"
	}

	return msg
}

// isAuthFailure returns true for HTTP status codes that indicate
// an authentication or authorization failure.
func isAuthFailure(code int) bool {
	return code == http.StatusUnauthorized ||
		code == http.StatusForbidden ||
		code == statusAuthorizationExpired
}

// Oauth2AuthManager implements AuthManager with OAuth2 token management.
// With a static Token it acts as a simple bearer token provider. When a
// Credential is set, it fetches tokens via client_credentials and
// refreshes them proactively before expiry using a fallback chain:
// token exchange → exchange with Basic auth → refresh_token →
// client_credentials.
type Oauth2AuthManager struct {
	Token      string
	Credential string

	AuthURI  *url.URL
	Scope    string
	Audience string
	Resource string
	Headers  http.Header
	Client   *http.Client

	mu           sync.RWMutex
	accessToken  string
	refreshToken string
	tokenType    string
	expiry       time.Time
}

// basicAuth returns a Basic auth header value for the credential.
func (o *Oauth2AuthManager) basicAuth() string {
	clientID, clientSecret, hasID := strings.Cut(o.Credential, ":")
	if !hasID {
		clientID, clientSecret = "", o.Credential
	}

	return "Basic " + base64.StdEncoding.EncodeToString(
		[]byte(clientID+":"+clientSecret))
}

// AuthHeader returns the authorization header with the bearer token.
// When Credential is set, tokens are fetched, cached, and refreshed
// automatically using the OAuth2 fallback chain.
func (o *Oauth2AuthManager) AuthHeader() (string, string, error) {
	return o.AuthHeaderCtx(context.Background())
}

// AuthHeaderCtx is the context-aware version of AuthHeader. The context
// is threaded through to any HTTP token requests so that callers can
// cancel or set deadlines on token fetches.
func (o *Oauth2AuthManager) AuthHeaderCtx(ctx context.Context) (string, string, error) {
	// If no credential, use whatever token we have (may be empty).
	if o.Credential == "" {
		return "Authorization", "Bearer " + o.Token, nil
	}

	// Fast path: read lock for cached token checks.
	o.mu.RLock()
	if o.Token != "" && o.accessToken == "" {
		o.mu.RUnlock()
		return "Authorization", "Bearer " + o.Token, nil
	}
	if o.accessToken != "" && time.Now().Before(o.expiry) {
		tok := o.accessToken
		o.mu.RUnlock()
		return "Authorization", "Bearer " + tok, nil
	}
	o.mu.RUnlock()

	// Slow path: write lock for token refresh.
	o.mu.Lock()
	defer o.mu.Unlock()

	// Re-check after acquiring write lock -- another goroutine may
	// have refreshed while we waited.
	if o.accessToken != "" && time.Now().Before(o.expiry) {
		return "Authorization", "Bearer " + o.accessToken, nil
	}

	// Refresh the token using the OAuth2 fallback chain.
	tok, err := o.refreshCurrentTokenLocked(ctx)
	if err != nil {
		return "", "", err
	}

	return "Authorization", "Bearer " + tok, nil
}

// RefreshAuth forces a credential refresh, discarding any cached
// tokens and falling back through the refresh chain.
func (o *Oauth2AuthManager) RefreshAuth(ctx context.Context) error {
	// Static token path -- nothing to refresh.
	if o.Credential == "" {
		return nil
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	// Clear cached tokens so refreshExpiredTokenLocked falls
	// through to client_credentials. The server already rejected
	// our credentials, so both access and refresh tokens are stale.
	o.accessToken = ""
	o.refreshToken = ""
	o.expiry = time.Time{}

	// Token is cleared, so treat as expired.
	_, err := o.refreshExpiredTokenLocked(ctx)
	return err
}

// refreshCurrentTokenLocked attempts to refresh a still-valid (or
// recently expired) token. If the server previously returned an
// issued_token_type (indicating RFC 8693 support), it tries token
// exchange first and falls back to refreshExpiredTokenLocked on
// failure. Caller must hold o.mu.
func (o *Oauth2AuthManager) refreshCurrentTokenLocked(ctx context.Context) (string, error) {
	if o.tokenType != "" && o.accessToken != "" {
		tok, err := o.exchangeTokenLocked(ctx, o.accessToken, o.tokenType, "")
		if err == nil {
			return tok, nil
		}
	}

	return o.refreshExpiredTokenLocked(ctx)
}

// refreshExpiredTokenLocked handles the case where the access token is
// expired or missing. The fallback chain follows the Iceberg REST spec:
//  1. Token exchange with Basic auth (expired subject token + credential)
//  2. Refresh token grant (RFC 6749 Section 6)
//  3. Client credentials (clean re-auth)
//
// Caller must hold o.mu.
func (o *Oauth2AuthManager) refreshExpiredTokenLocked(ctx context.Context) (string, error) {
	if o.tokenType != "" && o.accessToken != "" && o.Credential != "" {
		tok, err := o.exchangeTokenLocked(ctx, o.accessToken, o.tokenType, o.basicAuth())
		if err == nil {
			return tok, nil
		}
	}

	if o.refreshToken != "" {
		tok, err := o.refreshWithRefreshTokenLocked(ctx)
		if err == nil {
			return tok, nil
		}
	}

	return o.fetchClientCredentialsLocked(ctx)
}

// exchangeTokenLocked performs an RFC 8693 token exchange request.
// If authHeader is non-empty it is set as the Authorization header
// (used for Basic auth when the subject token is expired).
// Caller must hold o.mu.
func (o *Oauth2AuthManager) exchangeTokenLocked(ctx context.Context, subjectToken, subjectTokenType, authHeader string) (string, error) {
	if subjectTokenType == "" {
		subjectTokenType = tokenTypeAccess
	}

	scope := "catalog"
	if o.Scope != "" {
		scope = o.Scope
	}

	data := url.Values{
		"grant_type":         {grantTokenExchange},
		"subject_token":      {subjectToken},
		"subject_token_type": {subjectTokenType},
		"scope":              {scope},
	}

	if o.Audience != "" {
		data.Set("audience", o.Audience)
	}
	if o.Resource != "" {
		data.Set("resource", o.Resource)
	}

	return o.doTokenRequestLocked(ctx, data, authHeader)
}

// refreshWithRefreshTokenLocked performs a refresh_token grant per
// RFC 6749 Section 6. Caller must hold o.mu.
func (o *Oauth2AuthManager) refreshWithRefreshTokenLocked(ctx context.Context) (string, error) {
	scope := "catalog"
	if o.Scope != "" {
		scope = o.Scope
	}

	data := url.Values{
		"grant_type":    {grantRefreshToken},
		"refresh_token": {o.refreshToken},
		"scope":         {scope},
	}

	return o.doTokenRequestLocked(ctx, data, "")
}

// fetchClientCredentialsLocked performs a client_credentials grant.
// Caller must hold o.mu.
func (o *Oauth2AuthManager) fetchClientCredentialsLocked(ctx context.Context) (string, error) {
	clientID, clientSecret, hasID := strings.Cut(o.Credential, ":")
	if !hasID {
		clientID, clientSecret = "", o.Credential
	}

	scope := "catalog"
	if o.Scope != "" {
		scope = o.Scope
	}

	data := url.Values{
		"grant_type":    {grantClientCredentials},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {scope},
	}

	return o.doTokenRequestLocked(ctx, data, "")
}

// doTokenRequestLocked sends a token request and updates cached state.
// If authHeader is non-empty it is set as the Authorization header.
// Caller must hold o.mu.
func (o *Oauth2AuthManager) doTokenRequestLocked(ctx context.Context, data url.Values, authHeader string) (string, error) {
	if o.AuthURI == nil {
		return "", fmt.Errorf("%w: missing auth uri for fetching token", ErrRESTError)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.AuthURI.String(), strings.NewReader(data.Encode()))
	if err != nil {
		return "", err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for k, vals := range o.Headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	}

	rsp, err := o.Client.Do(req)
	if err != nil {
		return "", err
	}

	if rsp.StatusCode == http.StatusOK {
		defer rsp.Body.Close()
		dec := json.NewDecoder(rsp.Body)
		var tok oauthTokenResponse
		if err := dec.Decode(&tok); err != nil {
			return "", fmt.Errorf("failed to decode oauth token response: %w", err)
		}

		o.accessToken = tok.AccessToken
		if tok.IssuedTokenType != "" {
			o.tokenType = tok.IssuedTokenType
		}
		// Store refresh token if the server returned one.
		// If omitted, preserve the existing one per RFC 6749 Section 6.
		if tok.RefreshToken != "" {
			o.refreshToken = tok.RefreshToken
		}

		expiresIn := time.Duration(tok.ExpiresIn) * time.Second
		if expiresIn > 0 {
			o.expiry = time.Now().Add(expiresIn - expiryThreshold(expiresIn))
		} else {
			// Default to 1 hour if no expiry provided.
			o.expiry = time.Now().Add(time.Hour)
		}

		return tok.AccessToken, nil
	}

	switch rsp.StatusCode {
	case http.StatusUnauthorized, http.StatusBadRequest:
		defer func() {
			_, _ = io.Copy(io.Discard, rsp.Body)
			_ = rsp.Body.Close()
		}()
		dec := json.NewDecoder(rsp.Body)
		var oauthErr oauthErrorResponse
		if err := dec.Decode(&oauthErr); err != nil {
			return "", fmt.Errorf("failed to decode oauth error: %w", err)
		}

		return "", oauthErr
	default:
		return "", handleNon200(rsp, nil)
	}
}
