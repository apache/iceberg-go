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
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

var (
	_ catalog.Catalog = (*Catalog)(nil)
)

const (
	keyOauthToken        = "token"
	keyWarehouseLocation = "warehouse"
	keyMetadataLocation  = "metadata_location"
	keyOauthCredential   = "credential"

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
	reg := catalog.RegistrarFunc(func(name string, p iceberg.Properties) (catalog.Catalog, error) {
		return newCatalogFromProps(name, p.Get("uri", ""), p)
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
	return
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
	return
}

type createTableRequest struct {
	Name          string                 `json:"name"`
	Schema        *iceberg.Schema        `json:"schema"`
	Location      string                 `json:"location,omitempty"`
	PartitionSpec *iceberg.PartitionSpec `json:"partition-spec,omitempty"`
	WriteOrder    *table.SortOrder       `json:"write-order,omitempty"`
	StageCreate   bool                   `json:"stage-create,omitempty"`
	Props         iceberg.Properties     `json:"properties,omitempty"`
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

type configResponse struct {
	Defaults  iceberg.Properties `json:"defaults"`
	Overrides iceberg.Properties `json:"overrides"`
}

type sessionTransport struct {
	http.Transport

	defaultHeaders http.Header
	signer         v4.HTTPSigner
	cfg            aws.Config
	service        string
	h              hash.Hash
}

// from https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/signer/v4#Signer.SignHTTP
const emptyStringHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func (s *sessionTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	for k, v := range s.defaultHeaders {
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

			if _, err = io.Copy(s.h, rdr); err != nil {
				return nil, err
			}

			payloadHash = string(s.h.Sum(nil))
			s.h.Reset()
		}

		creds, err := s.cfg.Credentials.Retrieve(r.Context())
		if err != nil {
			return nil, err
		}

		// modifies the request in place
		err = s.signer.SignHTTP(r.Context(), creds, r, payloadHash,
			s.service, s.cfg.Region, time.Now())
		if err != nil {
			return nil, err
		}
	}

	return s.Transport.RoundTrip(r)
}

func do[T any](ctx context.Context, method string, baseURI *url.URL, path []string, cl *http.Client, override map[int]error, allowNoContent bool) (ret T, err error) {
	var (
		req *http.Request
		rsp *http.Response
	)

	uri := baseURI.JoinPath(path...).String()
	if req, err = http.NewRequestWithContext(ctx, method, uri, nil); err != nil {
		return
	}

	if rsp, err = cl.Do(req); err != nil {
		return
	}

	if allowNoContent && rsp.StatusCode == http.StatusNoContent {
		return
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	if method == http.MethodHead {
		return
	}

	defer rsp.Body.Close()
	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return
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
	var (
		req  *http.Request
		rsp  *http.Response
		data []byte
	)

	uri := baseURI.JoinPath(path...).String()
	data, err = json.Marshal(payload)
	if err != nil {
		return
	}

	req, err = http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(data))
	if err != nil {
		return
	}

	rsp, err = cl.Do(req)
	if err != nil {
		return
	}

	if rsp.StatusCode != http.StatusOK {
		return ret, handleNon200(rsp, override)
	}

	if rsp.ContentLength == 0 {
		return
	}

	defer rsp.Body.Close()
	if err = json.NewDecoder(rsp.Body).Decode(&ret); err != nil {
		return ret, fmt.Errorf("%w: error decoding json payload: `%s`", ErrRESTError, err.Error())
	}

	return
}

func handleNon200(rsp *http.Response, override map[int]error) error {
	var e errorResponse

	dec := json.NewDecoder(rsp.Body)
	dec.Decode(&struct {
		Error *errorResponse `json:"error"`
	}{Error: &e})

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

func fromProps(props iceberg.Properties) *options {
	o := &options{}
	for k, v := range props {
		switch k {
		case keyOauthToken:
			o.oauthToken = v
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
		}
	}
	return o
}

func toProps(o *options) iceberg.Properties {
	props := iceberg.Properties{}

	setIf := func(key, v string) {
		if v != "" {
			props[key] = v
		}
	}

	setIf(keyOauthCredential, o.credential)
	setIf(keyOauthToken, o.oauthToken)
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

func newCatalogFromProps(name string, uri string, p iceberg.Properties) (*Catalog, error) {
	ops := fromProps(p)

	r := &Catalog{name: name}
	if err := r.init(ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

func NewCatalog(name, uri string, opts ...Option) (*Catalog, error) {
	ops := &options{}
	for _, o := range opts {
		o(ops)
	}

	r := &Catalog{name: name}
	if err := r.init(ops, uri); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Catalog) init(ops *options, uri string) error {
	baseuri, err := url.Parse(uri)
	if err != nil {
		return err
	}

	r.baseURI = baseuri.JoinPath("v1")
	if r.cl, ops, err = r.fetchConfig(ops); err != nil {
		return err
	}

	if ops.prefix != "" {
		r.baseURI = r.baseURI.JoinPath(ops.prefix)
	}
	r.props = toProps(ops)
	return nil
}

func (r *Catalog) fetchAccessToken(cl *http.Client, creds string, opts *options) (string, error) {
	clientID, clientSecret, hasID := strings.Cut(creds, ":")
	if !hasID {
		clientID, clientSecret = "", clientID
	}

	scope := "catalog"
	if opts.scope != "" {
		scope = opts.scope
	}
	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {scope},
	}

	uri := opts.authUri
	if uri == nil {
		uri = r.baseURI.JoinPath("oauth/tokens")
	}

	rsp, err := cl.PostForm(uri.String(), data)
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
		defer rsp.Request.GetBody()
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

func (r *Catalog) createSession(opts *options) (*http.Client, error) {
	session := &sessionTransport{
		Transport:      http.Transport{TLSClientConfig: opts.tlsConfig},
		defaultHeaders: http.Header{},
	}
	cl := &http.Client{Transport: session}

	token := opts.oauthToken
	if token == "" && opts.credential != "" {
		var err error
		if token, err = r.fetchAccessToken(cl, opts.credential, opts); err != nil {
			return nil, fmt.Errorf("auth error: %w", err)
		}
	}

	if token != "" {
		session.defaultHeaders.Set(authorizationHeader, bearerPrefix+" "+token)
	}

	session.defaultHeaders.Set("X-Client-Version", icebergRestSpecVersion)
	session.defaultHeaders.Set("Content-Type", "application/json")
	session.defaultHeaders.Set("User-Agent", "GoIceberg/"+iceberg.Version())
	session.defaultHeaders.Set("X-Iceberg-Access-Delegation", "vended-credentials")

	if opts.enableSigv4 {
		cfg, err := config.LoadDefaultConfig(context.Background())
		if err != nil {
			return nil, err
		}

		if opts.sigv4Region != "" {
			cfg.Region = opts.sigv4Region
		}

		session.cfg, session.service = cfg, opts.sigv4Service
		session.signer, session.h = v4.NewSigner(), sha256.New()
	}

	return cl, nil
}

func (r *Catalog) fetchConfig(opts *options) (*http.Client, *options, error) {
	params := url.Values{}
	if opts.warehouseLocation != "" {
		params.Set(keyWarehouseLocation, opts.warehouseLocation)
	}

	route := r.baseURI.JoinPath("config")
	route.RawQuery = params.Encode()

	sess, err := r.createSession(opts)
	if err != nil {
		return nil, nil, err
	}

	rsp, err := doGet[configResponse](context.Background(), route, []string{}, sess, nil)
	if err != nil {
		return nil, nil, err
	}

	cfg := rsp.Defaults
	if cfg == nil {
		cfg = iceberg.Properties{}
	}
	maps.Copy(cfg, toProps(opts))
	maps.Copy(cfg, rsp.Overrides)

	o := fromProps(cfg)
	o.awsConfig = opts.awsConfig
	o.tlsConfig = opts.tlsConfig

	if uri, ok := cfg["uri"]; ok {
		r.baseURI, err = url.Parse(uri)
		if err != nil {
			return nil, nil, err
		}
		r.baseURI = r.baseURI.JoinPath("v1")
	}

	return sess, o, nil
}

func (r *Catalog) Name() string              { return r.name }
func (r *Catalog) CatalogType() catalog.Type { return catalog.REST }

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", catalog.ErrNoSuchNamespace)
	}
	return nil
}

func (r *Catalog) tableFromResponse(identifier []string, metadata table.Metadata, loc string, config iceberg.Properties) (*table.Table, error) {
	id := identifier
	if r.name != "" {
		id = append([]string{r.name}, identifier...)
	}

	iofs, err := iceio.LoadFS(config, loc)
	if err != nil {
		return nil, err
	}

	return table.New(id, metadata, loc, iofs), nil
}

func (r *Catalog) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	ns := strings.Join(namespace, namespaceSeparator)
	path := []string{"namespaces", ns, "tables"}

	type resp struct {
		Identifiers []identifier `json:"identifiers"`
	}
	rsp, err := doGet[resp](ctx, r.baseURI, path, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	out := make([]table.Identifier, len(rsp.Identifiers))
	for i, id := range rsp.Identifiers {
		out[i] = append(id.Namespace, id.Name)
	}
	return out, nil
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

	var cfg internal.CreateTableCfg
	for _, o := range opts {
		o(&cfg)
	}

	freshSchema, err := iceberg.AssignFreshSchemaIDs(schema, nil)
	if err != nil {
		return nil, err
	}

	freshPartitionSpec, err := iceberg.AssignFreshPartitionSpecIDs(cfg.PartitionSpec, schema, freshSchema)
	if err != nil {
		return nil, err
	}

	freshSortOrder, err := table.AssignFreshSortOrderIDs(cfg.SortOrder, schema, freshSchema)
	if err != nil {
		return nil, err
	}

	payload := createTableRequest{
		Name:          tbl,
		Schema:        freshSchema,
		Location:      cfg.Location,
		PartitionSpec: &freshPartitionSpec,
		WriteOrder:    &freshSortOrder,
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

	return r.tableFromResponse(identifier, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) CommitTable(ctx context.Context, tbl *table.Table, requirements []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	ident := tbl.Identifier()

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

	ret, err := doPost[payload, loadTableResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl},
		payload{Name: tbl, MetadataLoc: metadataLoc}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrTableAlreadyExists})
	if err != nil {
		return nil, err
	}

	config := maps.Clone(r.props)
	maps.Copy(config, ret.Metadata.Properties())
	maps.Copy(config, ret.Config)
	return r.tableFromResponse(identifier, ret.Metadata, ret.MetadataLoc, config)
}

func (r *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
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
	maps.Copy(config, props)
	maps.Copy(config, ret.Metadata.Properties())
	for k, v := range ret.Config {
		config[k] = v
	}

	return r.tableFromResponse(identifier, ret.Metadata, ret.MetadataLoc, config)
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

	return r.tableFromResponse(ident, ret.Metadata, ret.MetadataLoc, config)
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
		From identifier `json:"from"`
		To   identifier `json:"to"`
	}
	f := identifier{
		Namespace: catalog.NamespaceFromIdent(from),
		Name:      catalog.TableNameFromIdent(from),
	}
	t := identifier{
		Namespace: catalog.NamespaceFromIdent(to),
		Name:      catalog.TableNameFromIdent(to),
	}

	_, err := doPost[payload, any](ctx, r.baseURI, []string{"tables", "rename"}, payload{From: f, To: t}, r.cl,
		map[int]error{http.StatusNotFound: catalog.ErrNoSuchTable})
	if err != nil {
		return nil, err
	}

	return r.LoadTable(ctx, to, nil)
}

func (r *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	_, err := doPost[map[string]any, struct{}](ctx, r.baseURI, []string{"namespaces"},
		map[string]any{"namespace": namespace, "properties": props}, r.cl, map[int]error{
			http.StatusNotFound: catalog.ErrNoSuchNamespace, http.StatusConflict: catalog.ErrNamespaceAlreadyExists})
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
	uri := r.baseURI.JoinPath("namespaces")
	if len(parent) != 0 {
		v := url.Values{}
		v.Set("parent", strings.Join(parent, namespaceSeparator))
		uri.RawQuery = v.Encode()
	}

	type rsptype struct {
		Namespaces []table.Identifier `json:"namespaces"`
	}

	rsp, err := doGet[rsptype](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: catalog.ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	return rsp.Namespaces, nil
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
	removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {

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
