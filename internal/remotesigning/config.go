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

// Package remotesigning shares REST-provided signing settings with FileIOs.
package remotesigning

import (
	"context"
	"errors"
	"net/http"
	"net/url"
)

const (
	EndpointProperty = "rest.remote-signing.endpoint"
	ConfigProperty   = "rest.remote-signing.config"
	TokenProperty    = "rest.remote-signing.token"
)

// Config is the REST catalog's remote-signing-config wire format.
type Config struct {
	Properties map[string]string `json:"properties,omitempty"`
	Headers    http.Header       `json:"headers,omitempty"`
}

// Session keeps table defaults outside the properties so refreshed or
// prefix-scoped credentials can still override the signing endpoint.
type Session struct {
	Client     *http.Client
	Endpoint   string
	CatalogURI string
}

type sessionKey struct{}

func WithSession(ctx context.Context, session Session) context.Context {
	return context.WithValue(ctx, sessionKey{}, session)
}

func SessionFromContext(ctx context.Context) Session {
	session, _ := ctx.Value(sessionKey{}).(Session)

	return session
}

// Endpoint resolves the configured signing endpoint. When a modern signing
// config is present, legacy signer endpoints are ignored. The caller supplies
// the default when it knows the table's catalog path.
func Endpoint(props map[string]string, fallback string) (string, error) {
	endpoint := props[EndpointProperty]
	base := props["uri"]
	_, hasConfig := props[ConfigProperty]
	if endpoint == "" && !hasConfig {
		endpoint = props["signer.endpoint"]
		legacyBase := props["signer.uri"]
		if legacyBase == "" {
			legacyBase = props["s3.signer.uri"]
		}
		if legacyBase != "" {
			base = legacyBase
			if endpoint == "" {
				endpoint = "v1/aws/s3/sign"
			}
		}
	}
	if endpoint == "" {
		endpoint = fallback
	}
	if endpoint != "" {
		var err error
		endpoint, err = resolveEndpoint(endpoint, base)
		if err != nil {
			return "", err
		}
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.New("invalid remote signing endpoint")
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return "", errors.New("remote signing requires an absolute HTTP(S) endpoint")
	}

	return parsed.String(), nil
}

func resolveEndpoint(endpoint, base string) (string, error) {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", errors.New("invalid remote signing endpoint")
	}
	if parsed.IsAbs() {
		return parsed.String(), nil
	}
	if parsed.Host != "" {
		return "", errors.New("remote signing endpoint must be a path or an absolute HTTP(S) URI")
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", errors.New("invalid remote signing base URI")
	}
	// Iceberg relative endpoints are appended to the catalog or signer URI.
	joined := baseURL.JoinPath(parsed.EscapedPath())
	joined.RawQuery, joined.ForceQuery = parsed.RawQuery, parsed.ForceQuery
	joined.Fragment, joined.RawFragment = parsed.Fragment, parsed.RawFragment

	return joined.String(), nil
}

// CloneHeaders retains multiple values and explicit empty headers while
// normalizing names from catalog JSON or caller-provided request maps.
func CloneHeaders(headers http.Header) http.Header {
	result := make(http.Header, len(headers))
	for name, values := range headers {
		name = http.CanonicalHeaderKey(name)
		result[name] = append(result[name], values...)
	}

	return result
}
