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

package resttest

import (
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// maxRequestBytes bounds request bodies the server decodes.
const maxRequestBytes = 16 << 20

// Option configures a handler built by NewHandler or NewServer.
type Option func(*config)

type config struct {
	warehouse string
	prefix    string
	defaults  iceberg.Properties
	overrides iceberg.Properties
}

// WithWarehouse sets the warehouse location, such as "file://"+t.TempDir().
// A table created without an explicit location, in a namespace without a
// "location" property, is placed under <warehouse>/<namespace>.db/<table>.
// Without a warehouse such a create fails with 400.
func WithWarehouse(uri string) Option {
	return func(c *config) { c.warehouse = uri }
}

// WithPrefix serves the catalog under /v1/{prefix}/ and returns the prefix in
// the config response overrides, so a client picks it up without being told.
func WithPrefix(prefix string) Option {
	return func(c *config) { c.prefix = strings.Trim(prefix, "/") }
}

// WithProperties sets the defaults and overrides the config endpoint returns.
// The server always sets the "prefix" override itself when WithPrefix is used.
func WithProperties(defaults, overrides iceberg.Properties) Option {
	return func(c *config) {
		c.defaults = maps.Clone(defaults)
		c.overrides = maps.Clone(overrides)
	}
}

// NewHandler returns an in-memory REST catalog. It is safe for concurrent use.
func NewHandler(opts ...Option) http.Handler {
	s := &server{
		namespaces: map[string]iceberg.Properties{},
		tables:     map[string]*tableEntry{},
	}
	for _, opt := range opts {
		opt(&s.cfg)
	}
	if s.cfg.prefix != "" {
		s.prefixSegments = strings.Split(s.cfg.prefix, "/")
	}

	return s
}

// NewServer starts NewHandler on an httptest server and closes it when the
// test ends. Unless WithWarehouse is given, the warehouse is a temporary
// directory owned by t.
func NewServer(t testing.TB, opts ...Option) *httptest.Server {
	t.Helper()

	opts = append([]Option{WithWarehouse("file://" + t.TempDir())}, opts...)
	srv := httptest.NewServer(NewHandler(opts...))
	t.Cleanup(srv.Close)

	return srv
}

type tableEntry struct {
	namespace   []string
	name        string
	metadataLoc string
	metadata    table.Metadata
}

type server struct {
	cfg            config
	prefixSegments []string

	// mu guards namespaces and tables. Handlers hold it for the whole
	// request, including metadata file writes, which keeps commits atomic.
	mu sync.Mutex
	// namespaces maps a namespace key (levels joined by nsSep) to its
	// properties.
	namespaces map[string]iceberg.Properties
	// tables maps tableKey(namespace, name) to the current table pointer.
	tables map[string]*tableEntry
}

// nsSep joins namespace levels. It is the decoded form of the REST client's
// default "%1F" separator.
const nsSep = "\x1f"

func nsKey(ns []string) string { return strings.Join(ns, nsSep) }

func tableKey(ns []string, name string) string { return nsKey(ns) + "\x00" + name }

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	segments, err := pathSegments(r.URL.EscapedPath())
	if err != nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, err.Error())

		return
	}
	if len(segments) == 0 || segments[0] != "v1" {
		writeError(w, r, http.StatusNotFound, typeNotFound, "unknown path "+r.URL.Path)

		return
	}
	segments = segments[1:]

	if len(segments) == 1 && segments[0] == "config" {
		if r.Method != http.MethodGet {
			writeError(w, r, http.StatusMethodNotAllowed, typeBadRequest, "config only supports GET")

			return
		}
		s.handleConfig(w)

		return
	}

	if len(segments) < len(s.prefixSegments) {
		writeError(w, r, http.StatusNotFound, typeNotFound, "unknown path "+r.URL.Path)

		return
	}
	for i, p := range s.prefixSegments {
		if segments[i] != p {
			writeError(w, r, http.StatusNotFound, typeNotFound, "unknown prefix in "+r.URL.Path)

			return
		}
	}
	segments = segments[len(s.prefixSegments):]

	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.route(w, r, segments) {
		writeError(w, r, http.StatusNotImplemented, typeNotImplemented,
			"resttest does not implement "+r.Method+" "+r.URL.Path)
	}
}

// route dispatches a request whose path has had /v1 and the prefix removed.
// It reports false when no supported endpoint matches.
func (s *server) route(w http.ResponseWriter, r *http.Request, seg []string) bool {
	switch {
	case len(seg) == 1 && seg[0] == "namespaces":
		switch r.Method {
		case http.MethodGet:
			s.listNamespaces(w, r)
		case http.MethodPost:
			s.createNamespace(w, r)
		default:
			return false
		}
	case len(seg) == 2 && seg[0] == "namespaces":
		ns := splitNamespace(seg[1])
		switch r.Method {
		case http.MethodGet:
			s.loadNamespace(w, r, ns)
		case http.MethodHead:
			s.namespaceExists(w, r, ns)
		case http.MethodDelete:
			s.dropNamespace(w, r, ns)
		default:
			return false
		}
	case len(seg) == 3 && seg[0] == "namespaces" && seg[2] == "properties" && r.Method == http.MethodPost:
		s.updateNamespaceProperties(w, r, splitNamespace(seg[1]))
	case len(seg) == 3 && seg[0] == "namespaces" && seg[2] == "register" && r.Method == http.MethodPost:
		s.registerTable(w, r, splitNamespace(seg[1]))
	case len(seg) == 3 && seg[0] == "namespaces" && seg[2] == "tables":
		ns := splitNamespace(seg[1])
		switch r.Method {
		case http.MethodGet:
			s.listTables(w, r, ns)
		case http.MethodPost:
			s.createTable(w, r, ns)
		default:
			return false
		}
	case len(seg) == 4 && seg[0] == "namespaces" && seg[2] == "tables":
		ns, name := splitNamespace(seg[1]), seg[3]
		switch r.Method {
		case http.MethodGet:
			s.loadTable(w, r, ns, name)
		case http.MethodHead:
			s.tableExists(w, r, ns, name)
		case http.MethodPost:
			s.commitTable(w, r, ns, name)
		case http.MethodDelete:
			s.dropTable(w, r, ns, name)
		default:
			return false
		}
	case len(seg) == 2 && seg[0] == "tables" && seg[1] == "rename" && r.Method == http.MethodPost:
		s.renameTable(w, r)
	default:
		return false
	}

	return true
}

// endpoints lists every route the server implements, in the
// "METHOD /v1/{prefix}/path" form the config response advertises. The HEAD
// routes are included so the client's existence checks use them.
var endpoints = []string{
	"GET /v1/{prefix}/namespaces",
	"POST /v1/{prefix}/namespaces",
	"GET /v1/{prefix}/namespaces/{namespace}",
	"HEAD /v1/{prefix}/namespaces/{namespace}",
	"DELETE /v1/{prefix}/namespaces/{namespace}",
	"POST /v1/{prefix}/namespaces/{namespace}/properties",
	"GET /v1/{prefix}/namespaces/{namespace}/tables",
	"POST /v1/{prefix}/namespaces/{namespace}/tables",
	"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
	"POST /v1/{prefix}/namespaces/{namespace}/register",
	"POST /v1/{prefix}/tables/rename",
}

func (s *server) handleConfig(w http.ResponseWriter) {
	overrides := maps.Clone(s.cfg.overrides)
	if s.cfg.prefix != "" {
		if overrides == nil {
			overrides = iceberg.Properties{}
		}
		overrides["prefix"] = s.cfg.prefix
	}
	defaults := s.cfg.defaults
	if defaults == nil {
		defaults = iceberg.Properties{}
	}
	if overrides == nil {
		overrides = iceberg.Properties{}
	}

	writeJSON(w, http.StatusOK, configResponse{
		Defaults:  defaults,
		Overrides: overrides,
		Endpoints: endpoints,
	})
}

// pathSegments splits an escaped URL path and decodes each segment, so an
// encoded "/" or separator inside a name stays within its segment.
func pathSegments(escapedPath string) ([]string, error) {
	trimmed := strings.Trim(escapedPath, "/")
	if trimmed == "" {
		return nil, nil
	}

	raw := strings.Split(trimmed, "/")
	segments := make([]string, len(raw))
	for i, seg := range raw {
		if seg == "" {
			return nil, errors.New("path must not contain empty segments")
		}
		decoded, err := url.PathUnescape(seg)
		if err != nil {
			return nil, err
		}
		segments[i] = decoded
	}

	return segments, nil
}

func splitNamespace(decoded string) []string { return strings.Split(decoded, nsSep) }

// decodeJSON decodes a request body into v, writing a 400 on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "malformed request body: "+err.Error())

		return false
	}

	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// Error types the server reports, matching the names Iceberg REST servers use.
const (
	typeBadRequest        = "BadRequestException"
	typeNotFound          = "NotFoundException"
	typeNoSuchNamespace   = "NoSuchNamespaceException"
	typeNoSuchTable       = "NoSuchTableException"
	typeAlreadyExists     = "AlreadyExistsException"
	typeNamespaceNotEmpty = "NamespaceNotEmptyException"
	typeCommitFailed      = "CommitFailedException"
	typeNotImplemented    = "NotImplementedException"
	typeServerError       = "InternalServerError"
)

// writeError writes the spec error envelope. HEAD responses carry no body.
func writeError(w http.ResponseWriter, r *http.Request, status int, errType, msg string) {
	if r.Method == http.MethodHead {
		w.WriteHeader(status)

		return
	}

	writeJSON(w, status, errorEnvelope{Error: errorModel{Message: msg, Type: errType, Code: status}})
}
