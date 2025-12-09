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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// AuthManager is an interface for providing custom authorization headers.
type AuthManager interface {
	// AuthHeader returns the key and value for the authorization header.
	AuthHeader() (string, string, error)
}

type oauthTokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	RefreshToken string `json:"refresh_token"`
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

// Oauth2AuthManager is an implementation of the AuthManager interface which
// simply returns the provided token as a bearer token. If a credential
// is provided instead of a static token, it will fetch and refresh the
// token as needed.
type Oauth2AuthManager struct {
	Token      string
	Credential string

	AuthURI *url.URL
	Scope   string
	Client  *http.Client
}

// AuthHeader returns the authorization header with the bearer token.
func (o *Oauth2AuthManager) AuthHeader() (string, string, error) {
	if o.Token == "" && o.Credential != "" {
		if o.Client == nil {
			return "", "", fmt.Errorf("%w: cannot fetch token without http client", ErrRESTError)
		}

		tok, err := o.fetchAccessToken()
		if err != nil {
			return "", "", err
		}
		o.Token = tok
	}

	return "Authorization", "Bearer " + o.Token, nil
}

func (o *Oauth2AuthManager) fetchAccessToken() (string, error) {
	clientID, clientSecret, hasID := strings.Cut(o.Credential, ":")
	if !hasID {
		clientID, clientSecret = "", o.Credential
	}

	scope := "catalog"
	if o.Scope != "" {
		scope = o.Scope
	}
	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {scope},
	}

	if o.AuthURI == nil {
		return "", fmt.Errorf("%w: missing auth uri for fetching token", ErrRESTError)
	}

	rsp, err := o.Client.PostForm(o.AuthURI.String(), data)
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
