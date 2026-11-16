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
	"maps"
	"net/http"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal/remotesigning"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Keep load-time signer settings with the table's other FileIO properties so
// credential refreshes and server-planned scans use the same signing session.
func (t *loadTableResponse) fileIOProperties(base iceberg.Properties) (iceberg.Properties, iceberg.Properties) {
	config := maps.Clone(base)
	if config == nil {
		config = make(iceberg.Properties)
	}
	maps.Copy(config, t.Metadata.Properties())
	saved := maps.Clone(t.Metadata.Properties())
	if saved == nil {
		saved = make(iceberg.Properties)
	}
	maps.Copy(config, t.Config)
	maps.Copy(saved, t.Config)
	if len(t.RemoteSigningConfig) != 0 && string(t.RemoteSigningConfig) != "null" {
		config[remotesigning.ConfigProperty] = string(t.RemoteSigningConfig)
		saved[remotesigning.ConfigProperty] = string(t.RemoteSigningConfig)
	}
	// Preserve table-token provenance even if a later credential refresh is
	// what first enables remote signing.
	if token, ok := t.Config[keyOauthToken]; ok {
		config[remotesigning.TokenProperty] = token
		saved[remotesigning.TokenProperty] = token
	}

	return config, saved
}

func (r *Catalog) remoteSigningSession(identifier []string) (remotesigning.Session, error) {
	ident, err := r.tableIdentifierPath(identifier)
	if err != nil {
		return remotesigning.Session{}, err
	}
	path, err := ident.objectReqPath(endpointRemoteSign)
	if err != nil {
		return remotesigning.Session{}, err
	}
	base := r.catalogURI
	if base == nil {
		base = r.baseURI
	}

	return remotesigning.Session{
		Client:     r.remoteSigningHTTPClient(),
		Endpoint:   r.baseURI.JoinPath(path...).String(),
		CatalogURI: base.String(),
	}, nil
}

// This client shares the catalog's connection pool and refreshing auth manager,
// but explicit signer authentication takes precedence. The catalog's own
// session keeps its stricter managed-auth behavior.
func (r *Catalog) remoteSigningHTTPClient() *http.Client {
	client := *r.cl
	if session, ok := client.Transport.(*sessionTransport); ok {
		client.Transport = &remoteSigningTransport{session: session}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	return &client
}

type remoteSigningTransport struct {
	session *sessionTransport
}

func (s *remoteSigningTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	req := request.Clone(request.Context())
	req.Header = remotesigning.CloneHeaders(request.Header)
	session := *s.session
	authority := *req.URL
	if req.Host != "" {
		authority.Host = req.Host
	}
	toCatalog := session.catalogOrigin != nil && sameOrigin(session.catalogOrigin, req.URL) && sameOrigin(session.catalogOrigin, &authority)
	if !toCatalog {
		session.catalogOrigin = nil
	}
	_, explicitAuth := req.Header[authorizationHeader]

	if !toCatalog {
		session.builtinHeaders = nil
	}
	session.authOrigin = nil
	session.authManager = nil
	if explicitAuth {
		session.signer = nil
	} else if s.session.authManager != nil && toCatalog {
		var key, value string
		var err error
		if manager, ok := s.session.authManager.(ContextAuthManager); ok {
			key, value, err = manager.AuthHeaderWithContext(req.Context())
		} else {
			key, value, err = s.session.authManager.AuthHeader()
		}
		if err != nil {
			return nil, err
		}
		// RemoteSigningConfig headers are provided by the server and must be
		// passed unchanged, including custom authentication header names.
		if _, supplied := req.Header[http.CanonicalHeaderKey(key)]; !supplied {
			req.Header.Set(key, value)
		}
	}

	return session.RoundTrip(req)
}

type remoteSigningPlanIO struct {
	table.PlanIO
	session remotesigning.Session
}

func (p *remoteSigningPlanIO) Load(ctx context.Context) (iceio.IO, error) {
	return p.PlanIO.Load(remotesigning.WithSession(ctx, p.session))
}
