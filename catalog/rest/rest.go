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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"iter"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

var _ catalog.Catalog = (*Catalog)(nil)

const (
	pageSizeKey contextKey = "page_size"

	defaultPageSize = 20

	keyOauthToken        = "token"
	keyWarehouseLocation = "warehouse"
	keyMetadataLocation  = "metadata_location"
	keyOauthCredential   = "credential"
	keyScope             = "scope"

	authorizationHeader = "Authorization"
	bearerPrefix        = "Bearer"
	namespaceSeparator  = "\x1F"
	keyPrefix           = "prefix"

	icebergRestSpecVersion = "0.14.1"

	keyRestSigV4        = "rest.sigv4-enabled"
	keyRestSigV4Region  = "rest.signing-region"
	keyRestSigV4Service = "rest.signing-name"
	keyAuthUrl          = "rest.authorization-url"
	keyTlsSkipVerify    = "rest.tls.skip-verify"
)

var (
	ErrRESTError            = errors.New("REST error")
	ErrBadRequest           = fmt.Errorf("%w: bad request", ErrRESTError)
	ErrForbidden            = fmt.Errorf("%w: forbidden", ErrRESTError)
	ErrUnauthorized         = fmt.Errorf("%w: unauthorized", ErrRESTError)
	ErrAuthorizationExpired = fmt.Errorf("%w: authorization expired", ErrRESTError)
	ErrServiceUnavailable   = fmt.Errorf("%w: service unavailable", ErrRESTError)
	ErrServerError          = fmt.Errorf("%w: server error", ErrRESTError)
	ErrCommitFailed         = fmt.Errorf("%w: commit failed, refresh and try again", ErrRESTError)
	ErrCommitStateUnknown   = fmt.Errorf("%w: commit failed due to unknown reason", ErrRESTError)
	ErrOAuthError           = fmt.Errorf("%w: oauth error", ErrRESTError)
)

func init() {
	reg := catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (catalog.Catalog, error) {
		return newCatalogFromProps(ctx, name, p.Get("uri", ""), p)
	})

	catalog.Register(string(catalog.REST), reg)
	catalog.Register("http", reg)
	catalog.Register("https", reg)
}

type errorResponse struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`

	wrapping error
}

type contextKey string

func (e errorResponse) Unwrap() error { return e.wrapping }
func (e errorResponse) Error() string {
	return e.Type + ": " + e.Message
}

type identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type commitTableResponse struct {
	MetadataLoc string          `json:"metadata-location"`
	RawMetadata json.RawMessage `json:"metadata"`
	Metadata    table.Metadata  `json:"-"`
}

func (t *commitTableResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias commitTableResponse
	if err = json.Unmarshal(b, (*Alias)(t)); err != nil {
		return err
	}

	t.Metadata, err = table.ParseMetadataBytes(t.RawMetadata)

	return err
}

type loadTableResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
	Metadata    table.Metadata     `json:"-"`
}

func (t *loadTableResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias loadTableResponse
	if err = json.Unmarshal(b, (*Alias)(t)); err != nil {
		return err
	}

	t.Metadata, err = table.ParseMetadataBytes(t.RawMetadata)

	return err
}

type createTableRequest struct {
	Name          string                 `json:"name"`
	Schema        *iceberg.Schema        `json:"schema"`
	Location      string                 `json:"location,omitempty"`
	PartitionSpec *iceberg.PartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *table.SortOrder       `json:"write-order,omitempty"`
	StageCreate   bool                   `json:"stage-create"`
	Props         iceberg.Properties     `json:"properties,omitempty"`
}

type configResponse struct {
	Defaults  iceberg.Properties `json:"defaults"`
	Overrides iceberg.Properties `json:"overrides"`
}

type sessionTransport struct {
	http.RoundTripper

	defaultHeaders http.Header
	signer         v4.HTTPSigner
	cfg            aws.Config
	service        string
	newHash        func() hash.Hash
}

// from https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/signer/v4#Signer.SignHTTP
const emptyStringHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func (s *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range s.defaultHeaders {
		// Skip Content-Type if it's already set in the request
		// to avoid duplicate headers (e.g., when using PostForm)
		if http.CanonicalHeaderKey(k) == "Content-Type" && r.Header.Get("Content-Type") != "" {
			continue
		}
		for _, hdr := range v {
			r.Header.Add(k, hdr)
		}
	}

	if s.signer != nil {
		var payloadHash string
		if r.Body == nil {
			payloadHash = emptyStringHash
		} else {
			rdr, err := r.GetBody()
			if err != nil {
				return nil, err
			}

			h := s.newHash()
			if _, err = io.Copy(h, rdr); err != nil {
				return nil, err
			}

			payloadHash = hex.EncodeToString(h.Sum(nil))
		}

		creds, err := s.cfg.Credentials.Retrieve(r.Context())
		if err != nil {
			return nil, err
		}

		// Set the x-amz-content-sha256 header before signing.
		// This header is required for AWS SigV4 signature verification.
		r.Header.Set("x-amz-content-sha256", payloadHash)

		// modifies the request in place
		err = s.signer.SignHTTP(r.Context(), creds, r, payloadHash,
			s.service, s.cfg.Region, time.Now())
		if err != nil {
			return nil, err
		}
	}

	return s.RoundTripper.RoundTrip(r)
}

func do[T any](ctx context.Context, method string, baseURI *url.URL, path []string, cl *http.Client, override map[int]error, allowNoContent bool) (ret T, err error) {
	var (
		req *http.Request
		rsp *http.Response
	)

	uri := baseURI.JoinPath(path...).String()
	if req, err = http.NewRequestWithContext(ctx, method, uri, nil); err != nil {
		return ret, err
	}

	if rsp, err = cl.Do(req); err != nil {
		return ret, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, rsp.Body)
		_ = rsp.Body.Close()
	}()

	if allowNoContent && rsp.StatusCode == http.StatusNoContent {
		return ret, err
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	if method == http.MethodHead || method == http.MethodDelete {
		return ret, err
	}

	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return ret, err
}

func doGet[T any](ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error) (ret T, err error) {
	return do[T](ctx, http.MethodGet, baseURI, path, cl, override, false)
}

func doDelete[T any](ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error) (ret T, err error) {
	return do[T](ctx, http.MethodDelete, baseURI, path, cl, override, true)
}

func doHead(ctx context.Context, baseURI *url.URL, path []string, cl *http.Client, override map[int]error) error {
	_, err := do[struct{}](ctx, http.MethodHead, baseURI, path, cl, override, true)

	return err
}

func doPost[Payload, Result any](ctx context.Context, baseURI *url.URL, path []string, payload Payload, cl *http.Client, override map[int]error) (ret Result, err error) {
	return doPostAllowNoContent[Payload, Result](ctx, baseURI, path, payload, cl, override, false)
}

func doPostAllowNoContent[Payload, Result any](ctx context.Context, baseURI *url.URL, path []string, payload Payload, cl *http.Client, override map[int]error, allowNoContent bool) (ret Result, err error) {
	var (
		req  *http.Request
		rsp  *http.Response
		data []byte
	)

	uri := baseURI.JoinPath(path...).String()
	data, err = json.Marshal(payload)
	if err != nil {
		return ret, err
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(data))
	if err != nil {
		return ret, err
	}

	rsp, err = cl.Do(req)
	if err != nil {
		return ret, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, rsp.Body)
		_ = rsp.Body.Close()
	}()

	if allowNoContent && rsp.StatusCode == http.StatusNoContent {
		return ret, err
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	if rsp.ContentLength == 0 {
		return ret, err
	}

	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return ret, err
}

func handleNon200(rsp *http.Response, override map[int]error) error {
	var e errorResponse

	// Only try to decode if there's a body (HEAD requests don't have one)
	if rsp.ContentLength != 0 {
		decErr := json.NewDecoder(rsp.Body).Decode(&struct {
			Error *errorResponse `json:"error"`
		}{Error: &e})
		if decErr != nil && decErr != io.EOF {
			return fmt.Errorf("%w: failed to decode error response: %s", ErrRESTError, decErr.Error())
		}
	}

	if override != nil {
		if err, ok := override[rsp.StatusCode]; ok {
			e.wrapping = err

			return e
		}
	}

	switch rsp.StatusCode {
	case http.StatusBadRequest:
		e.wrapping = ErrBadRequest
	case http.StatusUnauthorized:
		e.wrapping = ErrUnauthorized
	case http.StatusForbidden:
		e.wrapping = ErrForbidden
	case http.StatusUnprocessableEntity:
		e.wrapping = ErrRESTError
	case 419:
		e.wrapping = ErrAuthorizationExpired
	case http.StatusNotImplemented:
		e.wrapping = iceberg.ErrNotImplemented
	case http.StatusServiceUnavailable:
		e.wrapping = ErrServiceUnavailable
	default:
		if 500 <= rsp.StatusCode && rsp.StatusCode < 600 {
			e.wrapping = ErrServerError
		} else {
			e.wrapping = ErrRESTError
		}
	}

	return e
}

func fromProps(props iceberg.Properties, o *options) {
	for k, v := range props {
		switch k {
		case keyWarehouseLocation:
			o.warehouseLocation = v
		case keyMetadataLocation:
			o.metadataLocation = v
		case keyRestSigV4:
			o.enableSigv4 = strings.ToLower(v) == "true"
		case keyRestSigV4Region:
			o.sigv4Region = v
		case keyRestSigV4Service:
			o.sigv4Service = v
		case keyAuthUrl:
			u, err := url.Parse(v)
			if err != nil {
				continue
			}
			o.authUri = u
		case keyOauthCredential:
			o.credential = v
		case keyScope:
			o.scope = v
		case keyPrefix:
			o.prefix = v
		case keyTlsSkipVerify:
			verify := strings.ToLower(v) == "true"
			if o.tlsConfig == nil {
				o.tlsConfig = &tls.Config{
					InsecureSkipVerify: verify,
				}
			} else {
				o.tlsConfig.InsecureSkipVerify = verify
			}
		case "uri", "type":
		default:
			if v != "" {
				if o.additionalProps == nil {
					o.additionalProps = iceberg.Properties{}
				}
				o.additionalProps[k] = v
			}
		}
	}
}

func toProps(o *options) iceberg.Properties {
	props := iceberg.Properties{}
	maps.Copy(props, o.additionalProps)

	setIf := func(key, v string) {
		if v != "" {
			props[key] = v
		}
	}

	setIf(keyOauthCredential, o.credential)
	setIf(keyWarehouseLocation, o.warehouseLocation)
	setIf(keyMetadataLocation, o.metadataLocation)
	if o.enableSigv4 {
		props[keyRestSigV4] = "true"
		setIf(keyRestSigV4Region, o.sigv4Region)
		setIf(keyRestSigV4Service, o.sigv4Service)
	}

	setIf(keyPrefix, o.prefix)
	if o.authUri != nil {
		setIf(keyAuthUrl, o.authUri.String())
	}

	return props
}

type Catalog struct {
	baseURI *url.URL
	cl      *http.Client

	name  string
	props iceberg.Properties
}

func newCatalogFromProps(ctx context.Context, name string, uri string, p iceberg.Properties) (*Catalog, error) {
	var ops options
	fromProps(p, &ops)

	r := &Catalog{name: name}
	if err := r.init(ctx, &ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

func NewCatalog(ctx context.Context, name, uri string, opts ...Option) (*Catalog, error) {
	ops := &options{}
	for _, o := range opts {
		o(ops)
	}

	if ops.tlsConfig != nil && ops.transport != nil {
		return nil, errors.New("invalid catalog config with non-nil tlsConfig and transport: tlsConfig will be ignored, it should be added to the provided transport instead")
	}

	r := &Catalog{name: name}
	if err := r.init(ctx, ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

// setupOAuthManager creates an Oauth2AuthManager based on the provided options.
// The allows users to set their token, credential, or just get the defaults if no auth manager is set.
func setupOAuthManager(r *Catalog, cl *http.Client, opts *options) *Oauth2AuthManager {
	authURI := opts.authUri
	if authURI == nil {
		authURI = r.baseURI.JoinPath("oauth/tokens")
	}

	return &Oauth2AuthManager{
		Token:      opts.oauthToken,
		Credential: opts.credential,
		AuthURI:    authURI,
		Scope:      opts.scope,
		Client:     cl,
	}
}

func (r *Catalog) init(ctx context.Context, ops *options, uri string) error {
	baseuri, err := url.Parse(uri)
	if err != nil {
		return err
	}

	r.baseURI = baseuri.JoinPath("v1")
	if r.cl, ops, err = r.fetchConfig(ctx, ops); err != nil {
		return err
	}

	if ops.prefix != "" {
		r.baseURI = r.baseURI.JoinPath(ops.prefix)
	}
	r.props = toProps(ops)

	return nil
}

func (r *Catalog) createSession(ctx context.Context, opts *options) (*http.Client, error) {
	session := &sessionTransport{
		defaultHeaders: http.Header{},
	}
	if opts.transport != nil {
		session.RoundTripper = opts.transport
	} else {
		session.RoundTripper = &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: opts.tlsConfig}
	}
	cl := &http.Client{Transport: session}

	// If the user does not set an AuthManager, we can construct an OAuth2AuthManager based off their options.
	if opts.authManager == nil {
		opts.authManager = setupOAuthManager(r, cl, opts)
	}

	session.defaultHeaders.Set("X-Client-Version", icebergRestSpecVersion)
	session.defaultHeaders.Set("Content-Type", "application/json")
	session.defaultHeaders.Set("User-Agent", "GoIceberg/"+iceberg.Version())
	session.defaultHeaders.Set("X-Iceberg-Access-Delegation", "vended-credentials")

	for k, v := range opts.headers {
		session.defaultHeaders.Set(k, v)
	}

	if opts.authManager != nil {
		k, v, err := opts.authManager.AuthHeader()
		if err != nil {
			return nil, err
		}
		session.defaultHeaders.Set(k, v)
	}

	if opts.enableSigv4 {
		cfg := opts.awsConfig
		if !opts.awsConfigSet {
			// If no config provided, load defaults from environment.
			var err error
			cfg, err = config.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, err
			}
		}
		if opts.sigv4Region != "" {
			cfg.Region = opts.sigv4Region
		}

		session.cfg, session.service = cfg, opts.sigv4Service
		session.signer, session.newHash = v4.NewSigner(), sha256.New
	}

	return cl, nil
}

func (r *Catalog) fetchConfig(ctx context.Context, opts *options) (*http.Client, *options, error) {
	params := url.Values{}
	if opts.warehouseLocation != "" {
		params.Set(keyWarehouseLocation, opts.warehouseLocation)
	}

	route := r.baseURI.JoinPath("config")
	route.RawQuery = params.Encode()

	sess, err := r.createSession(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	rsp, err := doGet[configResponse](ctx, route, []string{}, sess, nil)
	if err != nil {
		return nil, nil, err
	}

	cfg := rsp.Defaults
	if cfg == nil {
		cfg = iceberg.Properties{}
	}
	maps.Copy(cfg, toProps(opts))
	maps.Copy(cfg, rsp.Overrides)

	o := *opts
	fromProps(cfg, &o)

	if uri, ok := cfg["uri"]; ok {
		r.baseURI, err = url.Parse(uri)
		if err != nil {
			return nil, nil, err
		}
		r.baseURI = r.baseURI.JoinPath("v1")
	}

	return sess, &o, nil
}

func (r *Catalog) Name() string              { return r.name }
func (r *Catalog) CatalogType() catalog.Type { return catalog.REST }

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}

	return nil
}

func (r *Catalog) tableFromResponse(ctx context.Context, identifier []string, metadata table.Metadata, loc string, config iceberg.Properties) (*table.Table, error) {
	return table.New(
		identifier,
		metadata,
		loc,
		iceio.LoadFSFunc(config, loc),
		r,
	), nil
}

func (r *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		pageSize := r.getPageSize(ctx)
		var pageToken string

		for {
			tables, nextPageToken, err := r.listTablesPage(ctx, namespace, pageToken, pageSize)
			if err != nil {
				yield(table.Identifier{}, err)

				return
			}
			for _, tbl := range tables {
				if !yield(tbl, nil) {
					return
				}
			}
			if nextPageToken == "" {
				return
			}
			pageToken = nextPageToken
		}
	}
}

func (r *Catalog) listTablesPage(ctx context.Context, namespace table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, "", err
	}
	ns := strings.Join(namespace, namespaceSeparator)
	uri := r.baseURI.JoinPath("namespaces", ns, "tables")

	v := url.Values{}
	if pageSize >= 0 {
		v.Set("pageSize", strconv.Itoa(pageSize))
	}
	if pageToken != "" {
		v.Set("pageToken", pageToken)
	}

	uri.RawQuery = v.Encode()
	type resp struct {
		Identifiers   []identifier `json:"identifiers"`
		NextPageToken string       `json:"next-page-token,omitempty"`
	}
	rsp, err := doGet[resp](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, "", err
	}
	out := make([]table.Identifier, len(rsp.Identifiers))
	for i, id := range rsp.Identifiers {
		out[i] = append(id.Namespace, id.Name)
	}

	return out, rsp.NextPageToken, nil
}

func splitIdentForPath(ident table.Identifier) (string, string, error) {
	if len(ident) < 1 {
		return "", "", fmt.Errorf("%w: missing namespace or invalid identifier %v",
			catalog.ErrNoSuchTable, strings.Join(ident, "."))
	}

	return strings.Join(catalog.NamespaceFromIdent(ident), namespaceSeparator), catalog.TableNameFromIdent(ident), nil
}

func (r *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	cfg := catalog.NewCreateTableCfg()
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.SortOrder.Fields() == nil && cfg.SortOrder.OrderID() == 0 {
		cfg.SortOrder = table.UnsortedSortOrder
	}

	payload := createTableRequest{
		Name:          tbl,
		Schema:        schema,
		Location:      cfg.Location,
		PartitionSpec: cfg.PartitionSpec,
		WriteOrder:    &cfg.SortOrder,
		StageCreate:   false,
		Props:         cfg.Properties,
	}

	ret, err := doPost[createTableRequest, loadTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables"}, payload,
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrTableAlreadyExists})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	maps.Copy(config, ret.Config)

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) CommitTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	ns, tblName, err := splitIdentForPath(ident)
	if err != nil {
		return nil, "", err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      tblName,
	}

	type payload struct {
		Identifier   identifier          `json:"identifier"`
		Requirements []table.Requirement `json:"requirements"`
		Updates      []table.Update      `json:"updates"`
	}

	ret, err := doPost[payload, commitTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables", tblName},
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable, http.StatusConflict: ErrCommitFailed})
	if err != nil {
		return nil, "", err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())

	return ret.Metadata, ret.MetadataLoc, nil
}

func (r *Catalog) RegisterTable(ctx context.Context, identifier table.Identifier, metadataLoc string) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	type payload struct {
		Name        string `json:"name"`
		MetadataLoc string `json:"metadata-location"`
	}

	ret, err := doPost[payload, loadTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "register"},
		payload{Name: tbl, MetadataLoc: metadataLoc}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrTableAlreadyExists,
		})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	maps.Copy(config, ret.Config)

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	ret, err := doGet[loadTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	for k, v := range ret.Config {
		config[k] = v
	}

	return r.tableFromResponse(ctx, identifier, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) UpdateTable(ctx context.Context, ident table.Identifier, requirements []table.Requirement, updates []table.Update) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(ident)
	if err != nil {
		return nil, err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      tbl,
	}
	type payload struct {
		Identifier   identifier          `json:"identifier"`
		Requirements []table.Requirement `json:"requirements"`
		Updates      []table.Update      `json:"updates"`
	}
	ret, err := doPost[payload, commitTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl},
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable, http.StatusConflict: ErrCommitFailed})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())

	return r.tableFromResponse(ctx, ident, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return err
	}

	_, err = doDelete[struct{}](ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})

	return err
}

func (r *Catalog) PurgeTable(ctx context.Context, identifier table.Identifier) error {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return err
	}

	uri := r.baseURI.JoinPath("namespaces", ns, "tables", tbl)
	v := url.Values{}
	v.Set("purgeRequested", "true")
	uri.RawQuery = v.Encode()

	_, err = doDelete[struct{}](ctx, uri, []string{}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})

	return err
}

func (r *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	type payload struct {
		Source      identifier `json:"source"`
		Destination identifier `json:"destination"`
	}
	src := identifier{
		Namespace: catalog.NamespaceFromIdent(from),
		Name:      catalog.TableNameFromIdent(from),
	}
	dst := identifier{
		Namespace: catalog.NamespaceFromIdent(to),
		Name:      catalog.TableNameFromIdent(to),
	}

	_, err := doPostAllowNoContent[payload, any](ctx, r.baseURI, []string{"tables", "rename"}, payload{Source: src, Destination: dst}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable}, true)
	if err != nil {
		return nil, err
	}

	return r.LoadTable(ctx, to)
}

func (r *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	_, err := doPost[map[string]any, struct{}](ctx, r.baseURI, []string{"namespaces"},
		map[string]any{"namespace": namespace, "properties": props}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrNamespaceAlreadyExists,
		})

	return err
}

func (r *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	_, err := doDelete[struct{}](ctx, r.baseURI, []string{"namespaces", strings.Join(namespace, namespaceSeparator)},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})

	return err
}

func (r *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	var allNamespaces []table.Identifier
	pageSize := r.getPageSize(ctx)
	var pageToken string

	for {
		namespaces, nextPageToken, err := r.listNamespacesPage(ctx, parent, pageToken, pageSize)
		if err != nil {
			return nil, err
		}
		allNamespaces = append(allNamespaces, namespaces...)
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
	}

	return allNamespaces, nil
}

func (r *Catalog) listNamespacesPage(ctx context.Context, parent table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	uri := r.baseURI.JoinPath("namespaces")

	v := url.Values{}
	if len(parent) != 0 {
		v.Set("parent", strings.Join(parent, namespaceSeparator))
	}
	if pageSize >= 0 {
		v.Set("pageSize", strconv.Itoa(pageSize))
	}
	if pageToken != "" {
		v.Set("pageToken", pageToken)
	}
	if len(v) > 0 {
		uri.RawQuery = v.Encode()
	}

	type rsptype struct {
		Namespaces    []table.Identifier `json:"namespaces"`
		NextPageToken string             `json:"next-page-token,omitempty"`
	}

	rsp, err := doGet[rsptype](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, "", err
	}

	return rsp.Namespaces, rsp.NextPageToken, nil
}

func (r *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	type nsresponse struct {
		Namespace table.Identifier   `json:"namespace"`
		Props     iceberg.Properties `json:"properties"`
	}

	rsp, err := doGet[nsresponse](ctx, r.baseURI, []string{"namespaces", strings.Join(namespace, namespaceSeparator)},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	return rsp.Props, nil
}

func (r *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties,
) (catalog.PropertiesUpdateSummary, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return catalog.PropertiesUpdateSummary{}, err
	}

	type payload struct {
		Remove  []string           `json:"removals"`
		Updates iceberg.Properties `json:"updates"`
	}

	ns := strings.Join(namespace, namespaceSeparator)

	return doPost[payload, catalog.PropertiesUpdateSummary](ctx, r.baseURI, []string{"namespaces", ns, "properties"},
		payload{Remove: removals, Updates: updates}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
}

func (r *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return false, err
	}

	err := doHead(ctx, r.baseURI, []string{"namespaces", strings.Join(namespace, namespaceSeparator)},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchNamespace) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (r *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return false, err
	}
	err = doHead(ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchTable) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

func (r *Catalog) ListViews(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		pageSize := r.getPageSize(ctx)
		var pageToken string

		for {
			views, nextPageToken, err := r.listViewsPage(ctx, namespace, pageToken, pageSize)
			if err != nil {
				yield(table.Identifier{}, err)

				return
			}
			for _, view := range views {
				if !yield(view, nil) {
					return
				}
			}
			if nextPageToken == "" {
				return
			}
			pageToken = nextPageToken
		}
	}
}

func (r *Catalog) listViewsPage(ctx context.Context, namespace table.Identifier, pageToken string, pageSize int) ([]table.Identifier, string, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, "", err
	}
	ns := strings.Join(namespace, namespaceSeparator)
	uri := r.baseURI.JoinPath("namespaces", ns, "views")

	v := url.Values{}
	if pageSize >= 0 {
		v.Set("pageSize", strconv.Itoa(pageSize))
	}
	if pageToken != "" {
		v.Set("pageToken", pageToken)
	}

	uri.RawQuery = v.Encode()
	type resp struct {
		Identifiers   []identifier `json:"identifiers"`
		NextPageToken string       `json:"next-page-token,omitempty"`
	}

	rsp, err := doGet[resp](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, "", err
	}

	out := make([]table.Identifier, len(rsp.Identifiers))
	for i, id := range rsp.Identifiers {
		out[i] = append(id.Namespace, id.Name)
	}

	return out, rsp.NextPageToken, nil
}

func (r *Catalog) getPageSize(ctx context.Context) int {
	if pageSize, ok := ctx.Value(pageSizeKey).(int); ok {
		return pageSize
	}

	return defaultPageSize
}

func (r *Catalog) SetPageSize(ctx context.Context, sz int) context.Context {
	return context.WithValue(ctx, pageSizeKey, sz)
}

func (r *Catalog) DropView(ctx context.Context, identifier table.Identifier) error {
	ns, view, err := splitIdentForPath(identifier)
	if err != nil {
		return err
	}

	_, err = doDelete[struct{}](ctx, r.baseURI, []string{"namespaces", ns, "views", view}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchView})

	return err
}

func (r *Catalog) CheckViewExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	ns, view, err := splitIdentForPath(identifier)
	if err != nil {
		return false, err
	}

	err = doHead(ctx, r.baseURI, []string{"namespaces", ns, "views", view},
		r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchView})
	if err != nil {
		if errors.Is(err, catalog.ErrNoSuchView) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

type createViewRequest struct {
	Name        string             `json:"name"`
	Schema      *iceberg.Schema    `json:"schema"`
	Location    string             `json:"location,omitempty"`
	Props       iceberg.Properties `json:"properties"`
	ViewVersion *view.Version      `json:"view-version"`
}

type viewResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
	Metadata    view.Metadata      `json:"-"`
}

func (v *viewResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias viewResponse
	if err = json.Unmarshal(b, (*Alias)(v)); err != nil {
		return err
	}

	v.Metadata, err = view.ParseMetadataBytes(v.RawMetadata)

	return
}

// CreateView creates a new view in the catalog.
func (r *Catalog) CreateView(ctx context.Context, identifier table.Identifier, version *view.Version, schema *iceberg.Schema, opts ...catalog.CreateViewOpt) (*view.View, error) {
	ns, viewName, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	if opts == nil {
		opts = []catalog.CreateViewOpt{}
	}
	cfg := catalog.NewCreateViewCfg()
	for _, opt := range opts {
		opt(&cfg)
	}

	freshSchema, err := iceberg.AssignFreshSchemaIDs(schema, nil)
	if err != nil {
		return nil, err
	}

	// Shallow copy for overriding certain attrs
	newVersion := *version
	version = &newVersion

	// Enforce starting ID of 1
	version.VersionID = 1

	// Set default catalog unless set by caller
	if len(version.DefaultCatalog) == 0 {
		version.DefaultCatalog = r.name
	}

	payload := createViewRequest{
		Name:        viewName,
		Location:    cfg.Location,
		Schema:      freshSchema,
		Props:       cfg.Properties,
		ViewVersion: version,
	}

	ret, err := doPost[createViewRequest, viewResponse](ctx, r.baseURI, []string{"namespaces", ns, "views"}, payload,
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace,
			http.StatusConflict: catalog.ErrViewAlreadyExists,
		})
	if err != nil {
		return nil, err
	}

	return view.New(identifier, ret.Metadata, ret.MetadataLoc), nil
}

// UpdateView updates a view in the catalog.
func (r *Catalog) UpdateView(ctx context.Context, ident table.Identifier, requirements []view.Requirement, updates []view.Update) (*view.View, error) {
	ns, viewName, err := splitIdentForPath(ident)
	if err != nil {
		return nil, err
	}

	restIdentifier := identifier{
		Namespace: catalog.NamespaceFromIdent(ident),
		Name:      viewName,
	}
	type payload struct {
		Identifier   identifier         `json:"identifier"`
		Requirements []view.Requirement `json:"requirements"`
		Updates      []view.Update      `json:"updates"`
	}
	ret, err := doPost[payload, viewResponse](ctx, r.baseURI, []string{"namespaces", ns, "views", viewName},
		payload{Identifier: restIdentifier, Requirements: requirements, Updates: updates}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchView, http.StatusConflict: ErrCommitFailed})
	if err != nil {
		return nil, err
	}

	return view.New(ident, ret.Metadata, ret.MetadataLoc), nil
}

// loadViewResponse contains the response from loading a view
type loadViewResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
}

// LoadView loads a view from the catalog.
func (r *Catalog) LoadView(ctx context.Context, identifier table.Identifier) (*view.View, error) {
	ns, v, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	rsp, err := doGet[loadViewResponse](ctx, r.baseURI, []string{"namespaces", ns, "views", v},
		r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchView,
		})
	if err != nil {
		return nil, err
	}

	metadata, err := view.ParseMetadataBytes(rsp.RawMetadata)
	if err != nil {
		return nil, fmt.Errorf("failed to parse view metadata: %w", err)
	}

	return view.New(identifier, metadata, rsp.MetadataLoc), nil
}
