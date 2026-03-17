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
	"errors"
	"fmt"

	"golang.org/x/oauth2"
)

// AuthManager is an interface for providing custom authorization headers.
type AuthManager interface {
	// AuthHeader returns the key and value for the authorization header.
	AuthHeader() (string, string, error)
}

// Oauth2AuthManager is an implementation of the AuthManager interface which
// uses an oauth2.TokenSource to provide bearer tokens. The token source
// handles caching, thread-safe refresh, and expiry management.
type Oauth2AuthManager struct {
	tokenSource oauth2.TokenSource
}

// AuthHeader returns the authorization header with the bearer token.
func (o *Oauth2AuthManager) AuthHeader() (string, string, error) {
	tok, err := o.tokenSource.Token()
	if err != nil {
		var re *oauth2.RetrieveError
		if errors.As(err, &re) {
			return "", "", oauthError{
				code: re.ErrorCode,
				desc: re.ErrorDescription,
				uri:  re.ErrorURI,
			}
		}

		return "", "", fmt.Errorf("%w: %s", ErrOAuthError, err)
	}

	return "Authorization", tok.Type() + " " + tok.AccessToken, nil
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
