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

package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/go-kit/log"
	"github.com/thanos-io/objstore/client"
)

var (
	_ Catalog = (*RestCatalog)(nil)
)

const (
	authorizationHeader = "Authorization"
	bearerPrefix        = "Bearer"
	namespaceSeparator  = "\x1F"
	keyPrefix           = "prefix"

	icebergRestSpecVersion = "0.14.1"

	keyRestSigV4        = "rest.sigv4-enabled"
	keyRestSigV4Region  = "rest.signing-region"
	keyRestSigV4Service = "rest.signing-name"
	keyAuthUrl          = "rest.authorization-url"
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

	req.Header.Add("Content-Type", "application/json")

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

func ToRestIdentifier(ident ...string) table.Identifier {
	if len(ident) == 1 {
		if ident[0] == "" {
			return nil
		}
		return table.Identifier(strings.Split(ident[0], "."))
	}

	return table.Identifier(ident)
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

type RestCatalog struct {
	baseURI *url.URL
	cl      *http.Client

	name  string
	props iceberg.Properties
}

func NewRestCatalog(name, uri string, opts ...Option[RestCatalog]) (*RestCatalog, error) {
	ops := &options{}
	for _, o := range opts {
		o(ops)
	}

	baseuri, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	r := &RestCatalog{
		name:    name,
		baseURI: baseuri.JoinPath("v1"),
	}

	if ops, err = r.fetchConfig(ops); err != nil {
		return nil, err
	}

	cl, err := r.createSession(ops)
	if err != nil {
		return nil, err
	}

	r.cl = cl
	if ops.prefix != "" {
		r.baseURI = r.baseURI.JoinPath(ops.prefix)
	}
	r.props = toProps(ops)
	return r, nil
}

func (r *RestCatalog) fetchAccessToken(cl *http.Client, creds string, opts *options) (string, error) {
	clientID, clientSecret, hasID := strings.Cut(creds, ":")
	if !hasID {
		clientID, clientSecret = "", clientID
	}

	data := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {"catalog"},
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

func (r *RestCatalog) createSession(opts *options) (*http.Client, error) {
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

func (r *RestCatalog) fetchConfig(opts *options) (*options, error) {
	params := url.Values{}
	if opts.warehouseLocation != "" {
		params.Set(keyWarehouseLocation, opts.warehouseLocation)
	}

	route := r.baseURI.JoinPath("config")
	route.RawQuery = params.Encode()

	sess, err := r.createSession(opts)
	if err != nil {
		return nil, err
	}

	rsp, err := doGet[configResponse](context.Background(), route, []string{}, sess, nil)
	if err != nil {
		return nil, err
	}

	cfg := rsp.Defaults
	maps.Copy(cfg, toProps(opts))
	maps.Copy(cfg, rsp.Overrides)

	o := fromProps(cfg)
	o.awsConfig = opts.awsConfig
	o.tlsConfig = opts.tlsConfig

	if uri, ok := cfg["uri"]; ok {
		r.baseURI, err = url.Parse(uri)
		if err != nil {
			return nil, err
		}
		r.baseURI = r.baseURI.JoinPath("v1")
	}

	return o, nil
}

func (r *RestCatalog) CatalogType() CatalogType { return REST }

func checkValidNamespace(ident table.Identifier) error {
	if len(ident) < 1 {
		return fmt.Errorf("%w: empty namespace identifier", ErrNoSuchNamespace)
	}
	return nil
}

func (r *RestCatalog) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	ns := strings.Join(namespace, namespaceSeparator)
	path := []string{"namespaces", ns, "tables"}

	type resp struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}

	rsp, err := doGet[resp](ctx, r.baseURI, path, r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
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
			ErrNoSuchTable, strings.Join(ident, "."))
	}

	return strings.Join(NamespaceFromIdent(ident), namespaceSeparator), TableNameFromIdent(ident), nil
}

type tblResponse struct {
	MetadataLoc string             `json:"metadata-location"`
	RawMetadata json.RawMessage    `json:"metadata"`
	Config      iceberg.Properties `json:"config"`
	Metadata    table.Metadata     `json:"-"`
}

func (t *tblResponse) UnmarshalJSON(b []byte) (err error) {
	type Alias tblResponse
	if err = json.Unmarshal(b, (*Alias)(t)); err != nil {
		return err
	}

	t.Metadata, err = table.ParseMetadataBytes(t.RawMetadata)
	return
}

func (r *RestCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	if props == nil {
		props = iceberg.Properties{}
	}

	ret, err := doGet[tblResponse](ctx, r.baseURI, []string{"namespaces", ns, "tables", tbl},
		r.cl, map[int]error{http.StatusNotFound: ErrNoSuchTable})
	if err != nil {
		return nil, err
	}

	id := identifier
	if r.name != "" {
		id = append([]string{r.name}, identifier...)
	}

	tblProps := maps.Clone(r.props)
	maps.Copy(tblProps, props)
	maps.Copy(tblProps, ret.Metadata.Properties())
	for k, v := range ret.Config {
		tblProps[k] = v
	}

	bucket, err := client.NewBucket(log.NewNopLogger(), []byte(tblProps["bucket_conf"]), "rest")
	if err != nil {
		return nil, err
	}
	return table.New(id, ret.Metadata, ret.MetadataLoc, bucket), nil
}

func (r *RestCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	return fmt.Errorf("%w: [Rest Catalog] drop table", iceberg.ErrNotImplemented)
}

func (r *RestCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return nil, fmt.Errorf("%w: [Rest Catalog] rename table", iceberg.ErrNotImplemented)
}

func (r *RestCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	_, err := doPost[map[string]any, struct{}](ctx, r.baseURI, []string{"namespaces"},
		map[string]any{"namespace": namespace, "properties": props}, r.cl, map[int]error{
			http.StatusNotFound: ErrNoSuchNamespace, http.StatusConflict: ErrNamespaceAlreadyExists})
	return err
}

func (r *RestCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if err := checkValidNamespace(namespace); err != nil {
		return err
	}

	_, err := doDelete[struct{}](ctx, r.baseURI, []string{"namespaces", strings.Join(namespace, namespaceSeparator)},
		r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})

	return err
}

func (r *RestCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	uri := r.baseURI.JoinPath("namespaces")
	if len(parent) != 0 {
		v := url.Values{}
		v.Set("parent", strings.Join(parent, namespaceSeparator))
		uri.RawQuery = v.Encode()
	}

	type rsptype struct {
		Namespaces []table.Identifier `json:"namespaces"`
	}

	rsp, err := doGet[rsptype](ctx, uri, []string{}, r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	return rsp.Namespaces, nil
}

func (r *RestCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	if err := checkValidNamespace(namespace); err != nil {
		return nil, err
	}

	type nsresponse struct {
		Namespace table.Identifier   `json:"namespace"`
		Props     iceberg.Properties `json:"properties"`
	}

	rsp, err := doGet[nsresponse](ctx, r.baseURI, []string{"namespaces", strings.Join(namespace, namespaceSeparator)},
		r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
	if err != nil {
		return nil, err
	}

	return rsp.Props, nil
}

func (r *RestCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
	removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error) {

	if err := checkValidNamespace(namespace); err != nil {
		return PropertiesUpdateSummary{}, err
	}

	type payload struct {
		Remove  []string           `json:"removals"`
		Updates iceberg.Properties `json:"updates"`
	}

	ns := strings.Join(namespace, namespaceSeparator)
	return doPost[payload, PropertiesUpdateSummary](ctx, r.baseURI, []string{"namespaces", ns, "properties"},
		payload{Remove: removals, Updates: updates}, r.cl, map[int]error{http.StatusNotFound: ErrNoSuchNamespace})
}
