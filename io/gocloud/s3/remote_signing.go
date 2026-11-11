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

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	stdio "io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/internal/remotesigning"
	"github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithymiddleware "github.com/aws/smithy-go/middleware"
)

func remoteSigningEnabled(props map[string]string) bool {
	enabled, _ := strconv.ParseBool(props[io.S3RemoteSigningEnabled])

	return enabled
}

type remoteSignRequest struct {
	Region     string            `json:"region"`
	URI        string            `json:"uri"`
	Method     string            `json:"method"`
	Headers    http.Header       `json:"headers"`
	Properties map[string]string `json:"properties"`
	Body       *string           `json:"body,omitempty"`
	Provider   string            `json:"provider"`
}

type remoteSignResponse struct {
	URI     string      `json:"uri"`
	Headers http.Header `json:"headers"`
}

// remoteSigningClient is installed only on an S3 client. Catalog and OAuth
// requests never pass through it, including when their hosts resemble S3 hosts.
type remoteSigningClient struct {
	storage  aws.HTTPClient
	signer   *http.Client
	endpoint string
	region   string
	config   remotesigning.Config
	token    string
	tokenSet bool
}

func configureRemoteSigning(ctx context.Context, props map[string]string, cfg *aws.Config) error {
	session := remotesigning.SessionFromContext(ctx)
	props = maps.Clone(props)
	if props == nil {
		props = make(map[string]string)
	}
	if props["uri"] == "" {
		props["uri"] = session.CatalogURI
	}
	endpoint, err := remotesigning.Endpoint(props, session.Endpoint)
	if err != nil {
		return err
	}
	var settings remotesigning.Config
	if raw := props[remotesigning.ConfigProperty]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &settings); err != nil {
			return fmt.Errorf("invalid remote signing config: %w", err)
		}
	}
	signer := session.Client
	token, tokenSet := props[remotesigning.TokenProperty]
	if signer == nil {
		// Retain the S3 client's TLS, proxy and dial settings, but never follow a
		// redirect that could replay signing credentials or the request body.
		signer = &http.Client{Timeout: 30 * time.Second}
		switch client := cfg.HTTPClient.(type) {
		case *http.Client:
			*signer = *client
		case *awshttp.BuildableClient:
			signer.Transport = client.GetTransport()
			if timeout := client.GetTimeout(); timeout != 0 {
				signer.Timeout = timeout
			}
		}
		token, tokenSet = props["token"]
	} else {
		copyClient := *signer
		signer = &copyClient
	}
	signer.CheckRedirect = noSigningRedirects
	if signer.Timeout == 0 {
		signer.Timeout = 30 * time.Second
	}
	storage := cfg.HTTPClient
	// Disable transport-level redirects too: the signature covers the exact
	// returned URI and must not be replayed on a redirect target.
	switch client := storage.(type) {
	case *http.Client:
		copyClient := *client
		copyClient.CheckRedirect = noSigningRedirects
		storage = &copyClient
	case *awshttp.BuildableClient:
		copyClient := *client.Freeze().(*http.Client)
		copyClient.CheckRedirect = noSigningRedirects
		storage = &copyClient
	}

	cfg.Credentials = aws.AnonymousCredentials{}
	cfg.HTTPClient = &remoteSigningClient{
		storage: storage, signer: signer, endpoint: endpoint,
		region: cfg.Region, config: settings, token: token, tokenSet: tokenSet,
	}

	return nil
}

func (c *remoteSigningClient) Do(req *http.Request) (*http.Response, error) {
	request := req.Clone(req.Context())
	request.Header = remotesigning.CloneHeaders(req.Header)
	sent := false
	defer func() {
		if !sent && request.Body != nil {
			_ = request.Body.Close()
		}
	}()
	if strings.Contains(strings.ToLower(strings.Join(request.Header.Values("Content-Encoding"), ",")), "aws-chunked") || strings.Contains(strings.Join(request.Header.Values("X-Amz-Content-Sha256"), ","), "STREAMING-") {
		return nil, permanentSigningError(errors.New("remote S3 signing does not support aws-chunked payloads"))
	}
	request.Header.Del("Authorization")
	request.Header.Del("X-Amz-Security-Token")
	request.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	payload := remoteSignRequest{
		Region: c.region, URI: request.URL.String(), Method: request.Method,
		Headers: request.Header.Clone(), Properties: maps.Clone(c.config.Properties), Provider: "s3",
	}
	if request.Host != "" {
		payload.Headers.Set("Host", request.Host)
	}
	// Only the small DeleteObjects XML is sent to the signer. Data file bodies
	// stay streaming and go directly to object storage, as in Iceberg Java.
	if request.Method == http.MethodPost && request.URL.Query().Has("delete") && request.Body != nil {
		body, err := stdio.ReadAll(request.Body)
		if err != nil {
			return nil, fmt.Errorf("read DeleteObjects body for remote signing: %w", err)
		}
		_ = request.Body.Close()
		text := string(body)
		payload.Body = &text
		request.Body = stdio.NopCloser(bytes.NewReader(body))
	}
	if payload.Properties == nil {
		payload.Properties = map[string]string{}
	}
	signed, err := c.sign(request.Context(), payload)
	if err != nil {
		return nil, err
	}
	uri, err := url.Parse(signed.URI)
	if err != nil || (uri.Scheme != "http" && uri.Scheme != "https") || uri.Host == "" || uri.User != nil || uri.Fragment != "" {
		return nil, permanentSigningError(errors.New("remote S3 signer returned an invalid URI"))
	}
	if signed.Headers == nil {
		return nil, permanentSigningError(errors.New("remote S3 signer response is missing headers"))
	}
	request.URL, request.Host = uri, uri.Host
	for key, values := range signed.Headers {
		if strings.EqualFold(key, "Cache-Control") {
			continue
		}
		// Returned authentication and signed headers win over the unsigned request.
		request.Header[http.CanonicalHeaderKey(key)] = append([]string(nil), values...)
	}
	if host := request.Header.Get("Host"); host != "" {
		request.Host = host
	}
	request.Header.Del("Host")
	sent = true
	resp, err := c.storage.Do(request)

	return resp, redactSigningURLError(err)
}

func (c *remoteSigningClient) sign(ctx context.Context, payload remoteSignRequest) (remoteSignResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return remoteSignResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return remoteSignResponse{}, err
	}
	req.Header = remotesigning.CloneHeaders(c.config.Headers)
	if _, supplied := req.Header["Content-Type"]; !supplied {
		req.Header.Set("Content-Type", "application/json")
	}
	if _, supplied := req.Header["Authorization"]; !supplied && c.tokenSet {
		if c.token == "" {
			req.Header["Authorization"] = nil
		} else {
			req.Header.Set("Authorization", "Bearer "+c.token)
		}
	}
	if host := req.Header.Get("Host"); host != "" {
		req.Host = host
		req.Header.Del("Host")
	}
	resp, err := c.signer.Do(req)
	if err != nil {
		return remoteSignResponse{}, fmt.Errorf("remote S3 signing failed: %w", redactSigningURLError(err))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return remoteSignResponse{}, &remoteSigningError{
			err:       fmt.Errorf("remote S3 signing failed: HTTP %d", resp.StatusCode),
			retryable: resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
		}
	}
	var signed remoteSignResponse
	if err := json.NewDecoder(resp.Body).Decode(&signed); err != nil {
		return signed, permanentSigningError(fmt.Errorf("decode remote S3 signature: %w", err))
	}

	return signed, nil
}

// Suppress computed upload checksums, whose HTTPS implementation wraps the body
// in aws-chunked framing. Explicit checksum headers remain intact. Keep the SDK
// checksum setup for operations such as DeleteObjects, where S3 requires it.
func disableRemoteSigningUploadChecksums(stack *smithymiddleware.Stack) error {
	if err := v4.SwapComputePayloadSHA256ForUnsignedPayloadMiddleware(stack); err != nil {
		return err
	}
	wrapper := &remoteSigningChecksumSetup{}
	original, err := stack.Initialize.Swap(awsChecksumSetupInputContextID, wrapper)
	if err == nil {
		wrapper.original = original
	}

	return nil
}

type remoteSigningChecksumSetup struct {
	original smithymiddleware.InitializeMiddleware
}

func (*remoteSigningChecksumSetup) ID() string { return awsChecksumSetupInputContextID }

func (m *remoteSigningChecksumSetup) HandleInitialize(ctx context.Context, in smithymiddleware.InitializeInput, next smithymiddleware.InitializeHandler) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
	switch input := in.Parameters.(type) {
	case *s3.PutObjectInput:
		if input.ChecksumAlgorithm != "" && !hasRemoteSigningChecksum(input) {
			return smithymiddleware.InitializeOutput{}, smithymiddleware.Metadata{}, errors.New("remote S3 signing requires an explicit checksum value when ChecksumAlgorithm is set")
		}

		return next.HandleInitialize(ctx, in)
	case *s3.UploadPartInput:
		if input.ChecksumAlgorithm != "" && !hasRemoteSigningChecksum(input) {
			return smithymiddleware.InitializeOutput{}, smithymiddleware.Metadata{}, errors.New("remote S3 signing requires an explicit checksum value when ChecksumAlgorithm is set")
		}

		return next.HandleInitialize(ctx, in)
	}

	return m.original.HandleInitialize(ctx, in, next)
}

func hasRemoteSigningChecksum(input any) bool {
	switch v := input.(type) {
	case *s3.PutObjectInput:
		return aws.ToString(map[string]*string{
			"CRC32": v.ChecksumCRC32, "CRC32C": v.ChecksumCRC32C, "CRC64NVME": v.ChecksumCRC64NVME,
			"SHA1": v.ChecksumSHA1, "SHA256": v.ChecksumSHA256, "MD5": v.ChecksumMD5, "SHA512": v.ChecksumSHA512,
			"XXHASH128": v.ChecksumXXHASH128, "XXHASH3": v.ChecksumXXHASH3, "XXHASH64": v.ChecksumXXHASH64,
		}[string(v.ChecksumAlgorithm)]) != ""
	case *s3.UploadPartInput:
		return aws.ToString(map[string]*string{
			"CRC32": v.ChecksumCRC32, "CRC32C": v.ChecksumCRC32C, "CRC64NVME": v.ChecksumCRC64NVME,
			"SHA1": v.ChecksumSHA1, "SHA256": v.ChecksumSHA256, "MD5": v.ChecksumMD5, "SHA512": v.ChecksumSHA512,
			"XXHASH128": v.ChecksumXXHASH128, "XXHASH3": v.ChecksumXXHASH3, "XXHASH64": v.ChecksumXXHASH64,
		}[string(v.ChecksumAlgorithm)]) != ""
	}

	return false
}

func noSigningRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

type remoteSigningError struct {
	err       error
	retryable bool
}

func (e *remoteSigningError) Error() string        { return e.err.Error() }
func (e *remoteSigningError) Unwrap() error        { return e.err }
func (e *remoteSigningError) RetryableError() bool { return e.retryable }

func permanentSigningError(err error) error { return &remoteSigningError{err: err} }

// Signed query parameters and signer URLs may contain credentials. Keep the
// original error available to errors.Is/As without including that URL in logs.
type signingTransportError struct {
	cause   error
	message string
}

func (e *signingTransportError) Error() string { return e.message }
func (e *signingTransportError) Unwrap() error { return e.cause }

func redactSigningURLError(err error) error {
	if urlErr, ok := errors.AsType[*url.Error](err); ok {
		return &signingTransportError{cause: err, message: urlErr.Op + " request failed"}
	}

	return err
}
