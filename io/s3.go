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

package io

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// Constants for S3 configuration options
const (
	S3Region                 = "s3.region"
	S3SessionToken           = "s3.session-token"
	S3SecretAccessKey        = "s3.secret-access-key"
	S3AccessKeyID            = "s3.access-key-id"
	S3EndpointURL            = "s3.endpoint"
	S3ProxyURI               = "s3.proxy-uri"
	S3ConnectTimeout         = "s3.connect-timeout"
	S3SignerUri              = "s3.signer.uri"
	S3SignerAuthToken        = "token"
	S3RemoteSigningEnabled   = "s3.remote-signing-enabled"
	S3ForceVirtualAddressing = "s3.force-virtual-addressing"
)

var unsupportedS3Props = []string{
	S3ConnectTimeout,
}

// ParseAWSConfig parses S3 properties and returns a configuration.
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	// If any unsupported properties are set, return an error.
	for k := range props {
		if k == S3ConnectTimeout {
			continue // no need to error for timeout prop
		} else if slices.Contains(unsupportedS3Props, k) {
			return nil, fmt.Errorf("unsupported S3 property %q", k)
		}
	}

	opts := []func(*config.LoadOptions) error{}

	if tok, ok := props["token"]; ok {
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	region := ""
	if r, ok := props[S3Region]; ok {
		region = r
		opts = append(opts, config.WithRegion(region))
	} else if r, ok := props["client.region"]; ok {
		region = r
		opts = append(opts, config.WithRegion(region))
	}

	// Check if remote signing is configured and enabled
	signerURI, hasSignerURI := props[S3SignerUri]
	remoteSigningEnabled := true // Default to true for backward compatibility
	if enabledStr, ok := props[S3RemoteSigningEnabled]; ok {
		if enabled, err := strconv.ParseBool(enabledStr); err == nil {
			remoteSigningEnabled = enabled
		}
	}

	if hasSignerURI && signerURI != "" && remoteSigningEnabled {
		// For remote signing, we still need valid (but potentially dummy) credentials
		// The actual signing will be handled by the transport layer
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"remote-signer", "remote-signer", "")))

		// Create a custom HTTP client with remote signing transport
		baseTransport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
		}

		// Apply proxy if configured
		if proxy, ok := props[S3ProxyURI]; ok {
			proxyURL, err := url.Parse(proxy)
			if err != nil {
				return nil, fmt.Errorf("invalid s3 proxy url '%s'", proxy)
			}
			baseTransport.Proxy = http.ProxyURL(proxyURL)
		}

		// Get auth token if configured
		authToken := props[S3SignerAuthToken]

		remoteSigningTransport := newRemoteSigningTransport(baseTransport, signerURI, region, authToken)
		httpClient := &http.Client{
			Transport: remoteSigningTransport,
		}

		opts = append(opts, config.WithHTTPClient(httpClient))
	} else {
		// Use regular credentials if no remote signer
		accessKey, secretAccessKey := props[S3AccessKeyID], props[S3SecretAccessKey]
		token := props[S3SessionToken]
		if accessKey != "" || secretAccessKey != "" || token != "" {
			opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				props[S3AccessKeyID], props[S3SecretAccessKey], props[S3SessionToken])))
		}

		if proxy, ok := props[S3ProxyURI]; ok {
			proxyURL, err := url.Parse(proxy)
			if err != nil {
				return nil, fmt.Errorf("invalid s3 proxy url '%s'", proxy)
			}

			opts = append(opts, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(
				func(t *http.Transport) {
					t.Proxy = http.ProxyURL(proxyURL)
				},
			)))
		}
	}

	awscfg := new(aws.Config)
	var err error
	*awscfg, err = config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return awscfg, nil
}

func createS3Bucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	var (
		awscfg *aws.Config
		err    error
	)
	if v := utils.GetAwsConfig(ctx); v != nil {
		awscfg = v
	} else {
		awscfg, err = ParseAWSConfig(ctx, props)
		if err != nil {
			return nil, err
		}
	}

	endpoint, ok := props[S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	usePathStyle := true
	if forceVirtual, ok := props[S3ForceVirtualAddressing]; ok {
		if cfgForceVirtual, err := strconv.ParseBool(forceVirtual); err == nil {
			usePathStyle = !cfgForceVirtual
		}
	}

	client := s3.NewFromConfig(*awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = usePathStyle
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	// Create a *blob.Bucket.
	bucket, err := s3blob.OpenBucketV2(ctx, client, parsed.Host, nil)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}

// RemoteSigningRequest represents the request sent to the remote signer
type RemoteSigningRequest struct {
	Method  string            `json:"method"`
	URI     string            `json:"uri"`
	Headers map[string]string `json:"headers,omitempty"`
	Region  string            `json:"region"`
}

// RemoteSigningResponse represents the response from the remote signer
type RemoteSigningResponse struct {
	Headers map[string]string `json:"headers"`
}

// remoteSigningTransport wraps an HTTP transport to handle remote signing
type remoteSigningTransport struct {
	base      http.RoundTripper
	signerURI string
	region    string
	authToken string
	client    *http.Client
}

// newRemoteSigningTransport creates a new remote signing transport
func newRemoteSigningTransport(base http.RoundTripper, signerURI, region, authToken string) *remoteSigningTransport {
	return &remoteSigningTransport{
		base:      base,
		signerURI: signerURI,
		region:    region,
		authToken: authToken,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// RoundTrip implements http.RoundTripper
func (r *remoteSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only handle S3 requests
	if !r.isS3Request(req) {
		return r.base.RoundTrip(req)
	}

	// Get signed headers from remote signer
	signedHeaders, err := r.getRemoteSignature(req.Context(), req.Method, req.URL.String(), r.extractHeaders(req))
	if err != nil {
		fmt.Printf("\033[31m%s\033[0m\n", err.Error()) // fails silently
		return nil, fmt.Errorf("failed to get remote signature: %w", err)
	}

	// Clone the request and apply signed headers
	newReq := req.Clone(req.Context())
	for key, value := range signedHeaders {
		newReq.Header.Set(key, value)
	}

	return r.base.RoundTrip(newReq)
}

// isS3Request checks if the request is destined for S3
func (r *remoteSigningTransport) isS3Request(req *http.Request) bool {
	// Check if the host contains typical S3 patterns
	host := req.URL.Host

	// Don't sign requests to the remote signer itself to avoid circular dependency
	if r.signerURI != "" {
		signerHost := ""
		if signerURL, err := url.Parse(r.signerURI); err == nil {
			signerHost = signerURL.Host
		}
		if host == signerHost {
			return false
		}
	}

	result := host != "" && (
	// Standard S3 endpoints
	host == "s3.amazonaws.com" ||
		// Regional S3 endpoints
		(len(host) > 12 && host[len(host)-12:] == ".amazonaws.com" && (host[:3] == "s3." || host[len(host)-17:len(host)-12] == ".s3")) ||
		// Virtual hosted-style bucket access
		(len(host) > 17 && host[len(host)-17:] == ".s3.amazonaws.com") ||
		// Path-style access to S3
		(len(host) > 3 && host[:3] == "s3.") ||
		// Cloudflare R2 endpoints
		(len(host) > 20 && host[len(host)-20:] == ".r2.cloudflarestorage.com") ||
		// MinIO or other custom S3-compatible endpoints (be more conservative)
		(len(host) > 0 && (host == "localhost:9000" || host == "127.0.0.1:9000" ||
			// Only sign if it looks like an S3 request pattern (has bucket-like structure)
			// and is NOT a catalog service (which typically has /catalog/ in the path)
			(req.URL.Path != "" && !strings.Contains(req.URL.Path, "/catalog/") &&
				!strings.Contains(host, "catalog") &&
				// Exclude common non-S3 service patterns
				!strings.Contains(host, "glue.") &&
				!strings.Contains(host, "api.") &&
				!strings.Contains(host, "catalog.")))))

	return result
}

// extractHeaders extracts relevant headers from the request
func (r *remoteSigningTransport) extractHeaders(req *http.Request) map[string]string {
	headers := make(map[string]string)
	for key, values := range req.Header {
		if len(values) > 0 {
			headers[key] = values[0]
		}
	}
	return headers
}

// getRemoteSignature sends a request to the remote signer and returns signed headers
func (r *remoteSigningTransport) getRemoteSignature(ctx context.Context, method, uri string, headers map[string]string) (map[string]string, error) {
	reqBody := RemoteSigningRequest{
		Method:  method,
		URI:     uri,
		Headers: headers,
		Region:  r.region,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal signing request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", r.signerURI, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create signer request to %s: %w", r.signerURI, err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Add authentication token if configured
	if r.authToken != "" {
		req.Header.Set("Authorization", "Bearer "+r.authToken)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to contact remote signer at %s: %w", r.signerURI, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		// Read the response body for better error diagnostics
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("remote signer at %s returned status %d (failed to read response body: %v)", r.signerURI, resp.StatusCode, readErr)
		}

		// Provide detailed error information based on status code
		switch resp.StatusCode {
		case 401:
			return nil, fmt.Errorf("remote signer authentication failed (401) at %s: %s", r.signerURI, string(body))
		case 403:
			return nil, fmt.Errorf("remote signer authorization denied (403) at %s: %s. Check that the signer service has proper AWS credentials and permissions for the target resource. Request was: %s", r.signerURI, string(body), string(payload))
		case 404:
			return nil, fmt.Errorf("remote signer endpoint not found (404) at %s: %s. Check the signer URI configuration", r.signerURI, string(body))
		case 500:
			return nil, fmt.Errorf("remote signer internal error (500) at %s: %s", r.signerURI, string(body))
		default:
			return nil, fmt.Errorf("remote signer at %s returned status %d: %s", r.signerURI, resp.StatusCode, string(body))
		}
	}

	var signingResponse RemoteSigningResponse
	if err := json.NewDecoder(resp.Body).Decode(&signingResponse); err != nil {
		return nil, fmt.Errorf("failed to decode signer response from %s: %w", r.signerURI, err)
	}

	return signingResponse.Headers, nil
}
