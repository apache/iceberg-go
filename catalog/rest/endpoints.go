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
	"fmt"
	"net/http"
	"strings"
)

// ErrEndpointNotSupported indicates the server did not advertise a REST endpoint
// required by the requested operation. It wraps [ErrRESTError].
var ErrEndpointNotSupported = fmt.Errorf("%w: endpoint not supported by server", ErrRESTError)

// pathPrefix is the part of every endpoint template already encoded in the base
// URI (API version plus optional catalog prefix); reqPath strips it.
const pathPrefix = "/v1/{prefix}"

// endpoint identifies a REST catalog operation by HTTP method and path template
// (e.g. "GET /v1/{prefix}/namespaces"). Servers advertise the endpoints they
// support in the config response so the client can negotiate capabilities.
//
// Each endpoint both gates capability and builds the request path (see
// [endpoint.reqPath]), making it the single source of truth for its operation.
type endpoint struct {
	method string
	path   string
}

func (e endpoint) String() string { return e.method + " " + e.path }

// reqPath renders the request path relative to the base URI, substituting params
// in order for the "{...}" placeholders that follow [pathPrefix].
func (e endpoint) reqPath(params ...string) []string {
	segs := strings.Split(strings.TrimPrefix(e.path, pathPrefix+"/"), "/")

	out := make([]string, len(segs))
	p := 0
	for i, s := range segs {
		if strings.HasPrefix(s, "{") && strings.HasSuffix(s, "}") {
			out[i], p = params[p], p+1
		} else {
			out[i] = s
		}
	}

	return out
}

// endpointFromString parses an endpoint from its "METHOD PATH" wire form,
// erroring unless the string is exactly two whitespace-separated tokens.
func endpointFromString(s string) (endpoint, error) {
	fields := strings.Fields(s)
	if len(fields) != 2 {
		return endpoint{}, fmt.Errorf("%w: invalid endpoint %q (expected \"METHOD PATH\")", ErrRESTError, s)
	}

	return endpoint{method: fields[0], path: fields[1]}, nil
}

// Well-known REST catalog endpoints, keyed by advertised method and path template.
var (
	endpointListNamespaces    = endpoint{http.MethodGet, "/v1/{prefix}/namespaces"}
	endpointLoadNamespace     = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}"}
	endpointNamespaceExists   = endpoint{http.MethodHead, "/v1/{prefix}/namespaces/{namespace}"}
	endpointCreateNamespace   = endpoint{http.MethodPost, "/v1/{prefix}/namespaces"}
	endpointUpdateNamespace   = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/properties"}
	endpointDeleteNamespace   = endpoint{http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}"}
	endpointCommitTransaction = endpoint{http.MethodPost, "/v1/{prefix}/transactions/commit"}

	endpointListTables       = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}/tables"}
	endpointLoadTable        = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"}
	endpointTableExists      = endpoint{http.MethodHead, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"}
	endpointCreateTable      = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables"}
	endpointUpdateTable      = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"}
	endpointDeleteTable      = endpoint{http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}/tables/{table}"}
	endpointRenameTable      = endpoint{http.MethodPost, "/v1/{prefix}/tables/rename"}
	endpointRegisterTable    = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/register"}
	endpointReportMetrics    = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics"}
	endpointTableCredentials = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials"}

	endpointListViews    = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}/views"}
	endpointLoadView     = endpoint{http.MethodGet, "/v1/{prefix}/namespaces/{namespace}/views/{view}"}
	endpointViewExists   = endpoint{http.MethodHead, "/v1/{prefix}/namespaces/{namespace}/views/{view}"}
	endpointCreateView   = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/views"}
	endpointUpdateView   = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/views/{view}"}
	endpointDeleteView   = endpoint{http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}/views/{view}"}
	endpointRenameView   = endpoint{http.MethodPost, "/v1/{prefix}/views/rename"}
	endpointRegisterView = endpoint{http.MethodPost, "/v1/{prefix}/namespaces/{namespace}/register-view"}
)

// fallbackEndpoints is assumed when a server advertises no "endpoints" list. It
// covers every operation the client can invoke, for compatibility with servers
// that predate endpoint negotiation.
var fallbackEndpoints = []endpoint{
	// namespace and table operations
	endpointListNamespaces, endpointLoadNamespace, endpointCreateNamespace,
	endpointUpdateNamespace, endpointDeleteNamespace,
	endpointListTables, endpointLoadTable, endpointCreateTable,
	endpointUpdateTable, endpointDeleteTable, endpointRenameTable,
	endpointRegisterTable, endpointReportMetrics, endpointCommitTransaction,

	// view operations
	endpointListViews, endpointLoadView, endpointCreateView,
	endpointUpdateView, endpointDeleteView, endpointRenameView,

	// Existence checks and view registration are included for parity with past
	// behavior. When an advertised set omits a HEAD endpoint, Check*Exists still
	// degrades to a GET.
	endpointNamespaceExists, endpointTableExists, endpointViewExists,
	endpointRegisterView,
}

// endpointSet is the set of endpoints a catalog server supports.
type endpointSet map[endpoint]struct{}

// newEndpointSet builds a set from a list of endpoints.
func newEndpointSet(eps []endpoint) endpointSet {
	s := endpointSet{}
	for _, e := range eps {
		s[e] = struct{}{}
	}

	return s
}

func (s endpointSet) contains(e endpoint) bool {
	_, ok := s[e]

	return ok
}

// check returns [ErrEndpointNotSupported] if the endpoint is not in the set. A
// nil set (capabilities never negotiated) permits every endpoint.
func (s endpointSet) check(e endpoint) error {
	if s == nil || s.contains(e) {
		return nil
	}

	return fmt.Errorf("%w: %s", ErrEndpointNotSupported, e)
}

// resolveEndpoints builds the effective endpoint set from the advertised list.
func resolveEndpoints(advertised []string) endpointSet {
	s := endpointSet{}
	for _, raw := range advertised {
		if e, err := endpointFromString(raw); err == nil {
			s[e] = struct{}{}
		}
	}
	// A server that advertises nothing (or only unparseable values) gets the
	// backward-compatibility fallback set.
	if len(s) == 0 {
		return newEndpointSet(fallbackEndpoints)
	}

	return s
}
