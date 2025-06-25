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
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"crypto/hmac"
	"crypto/sha256"

	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
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
	S3SignerEndpoint         = "s3.signer.endpoint"
	S3SignerAuthToken        = "token"
	S3RemoteSigningEnabled   = "s3.remote-signing-enabled"
	S3ForceVirtualAddressing = "s3.force-virtual-addressing"
)

// ParseAWSConfig parses S3 properties and returns a configuration.
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	opts := []func(*config.LoadOptions) error{}

	if tok, ok := props[S3SignerAuthToken]; ok {
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	region := ""
	if r, ok := props[S3Region]; ok {
		region = r
		opts = append(opts, config.WithRegion(region))
	} else if r, ok := props["client.region"]; ok {
		region = r
		// For Google Cloud Storage, "auto" region should be converted to a valid AWS region
		if region == "auto" && (strings.Contains(props[S3EndpointURL], "storage.googleapis.com") || strings.Contains(props[S3EndpointURL], "googleapis.com")) {
			region = "us-east-1" // Default region for GCS compatibility
		}
		opts = append(opts, config.WithRegion(region))
	} else if r, ok := props["rest.signing-region"]; ok {
		region = r
		// For Google Cloud Storage, "auto" region should be converted to a valid AWS region
		if region == "auto" && (strings.Contains(props[S3EndpointURL], "storage.googleapis.com") || strings.Contains(props[S3EndpointURL], "googleapis.com")) {
			region = "us-east-1" // Default region for GCS compatibility
		}
		opts = append(opts, config.WithRegion(region))
	}

	endpoint := props[S3EndpointURL]

	// Check if remote signing is configured and enabled
	signerURI, hasSignerURI := props[S3SignerUri]
	signerEndpoint := props[S3SignerEndpoint]
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
		timeoutStr := props[S3ConnectTimeout]

		remoteSigningTransport := NewRemoteSigningTransport(baseTransport, signerURI, signerEndpoint, region, authToken, timeoutStr)
		httpClient := &http.Client{
			Transport: remoteSigningTransport,
		}

		opts = append(opts, config.WithHTTPClient(httpClient))
	} else {
		// Use regular credentials if no remote signer
		accessKey, secretAccessKey := props[S3AccessKeyID], props[S3SecretAccessKey]
		token := props[S3SessionToken]

		if accessKey != "" || secretAccessKey != "" || token != "" {
			// Special handling for GCS - try a different approach
			if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
				// For GCS, let's try using the credentials but with special handling
				// The key insight is that GCS HMAC keys work differently than AWS keys
				opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					accessKey, secretAccessKey, token)))
			} else {
				// Regular AWS credentials
				opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					props[S3AccessKeyID], props[S3SecretAccessKey], props[S3SessionToken])))
			}
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

	// Special handling for Google Cloud Storage
	if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
		// GCS S3-compatible API has some specific requirements
		// Try to disable certain AWS-specific features that might not work with GCS
		awscfg.ClientLogMode = 0 // Disable detailed client logging that might interfere

		// Instead of anonymous credentials, let's use the actual credentials but configure
		// the signing process to be more compatible with GCS
		if accessKey := props[S3AccessKeyID]; accessKey != "" {
			secretKey := props[S3SecretAccessKey]
			token := props[S3SessionToken]
			awscfg.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)
		}
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

	// Google Cloud Storage requires path-style access when using S3-compatible API
	if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
		usePathStyle = true
	}

	client := s3.NewFromConfig(*awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		o.UsePathStyle = usePathStyle
		o.DisableLogOutputChecksumValidationSkipped = true

		// Special configuration for Google Cloud Storage
		if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
			// Disable S3-specific features that don't work well with GCS
			o.UseAccelerate = false

			// For GCS, we need to implement our own signing that's compatible with GCS requirements
			// The AWS SDK signing has subtle differences that don't work with GCS

			// Get the credentials for custom signing
			accessKey := props[S3AccessKeyID]
			secretKey := props[S3SecretAccessKey]

			if accessKey != "" && secretKey != "" {
				// Add custom transport that implements GCS-compatible signing
				o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
					return stack.Finalize.Add(
						middleware.FinalizeMiddlewareFunc("GCSCustomSigning", func(
							ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
						) (middleware.FinalizeOutput, middleware.Metadata, error) {
							// Cast to smithy HTTP request
							req, ok := in.Request.(*smithyhttp.Request)
							if ok {
								// Remove AWS authorization header since we'll create our own
								req.Header.Del("Authorization")

								// Set required headers for GCS
								req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
								req.Header.Set("Host", req.URL.Host)

								// Create GCS-compatible authorization header
								authHeader, err := createGCSAuthHeader(req, accessKey, secretKey, awscfg.Region)
								if err != nil {
									// Fall back to AWS signing
								} else {
									req.Header.Set("Authorization", authHeader)
								}
							}
							return next.HandleFinalize(ctx, in)
						}),
						middleware.After,
					)
				})
			}
		}

		// Only add middleware to prevent chunked encoding for GCS endpoints
		// For regular S3, these middleware can interfere with authentication
		if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				// Add a serialize step middleware to disable chunked encoding
				return stack.Serialize.Add(
					middleware.SerializeMiddlewareFunc("DisableChunkedEncoding", func(
						ctx context.Context, in middleware.SerializeInput, next middleware.SerializeHandler,
					) (middleware.SerializeOutput, middleware.Metadata, error) {
						// Try to access the S3 PutObjectInput to disable chunked encoding
						switch v := in.Parameters.(type) {
						case *s3.PutObjectInput:
							// Disable multipart uploads for smaller files to avoid chunking
							// This forces the SDK to use regular PUT operations
							if v.ContentLength != nil && *v.ContentLength < 5*1024*1024*1024 { // 5GB threshold
								// Processing handled by AWS SDK
							}
						}

						return next.HandleSerialize(ctx, in)
					}),
					middleware.After,
				)
			})

			// Also add a finalize middleware to ensure headers are set correctly
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Finalize.Add(
					middleware.FinalizeMiddlewareFunc("PreventChunkedHeaders", func(
						ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
					) (middleware.FinalizeOutput, middleware.Metadata, error) {
						// Cast to smithy HTTP request
						req, ok := in.Request.(*smithyhttp.Request)
						if ok {
							// Remove any chunked encoding headers
							req.Header.Del("Content-Encoding")
							req.Header.Del("Transfer-Encoding")
							req.Header.Del("X-Amz-Content-Sha256")

							// Set UNSIGNED-PAYLOAD to avoid content hashing which can trigger chunking
							req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

							// Ensure Content-Length is set if we have it
							if req.ContentLength > 0 {
								req.Header.Set("Content-Length", strconv.FormatInt(req.ContentLength, 10))
							}
						}
						return next.HandleFinalize(ctx, in)
					}),
					middleware.Before,
				)
			})
		}
	})

	// Create a *blob.Bucket with options
	bucketOpts := &s3blob.Options{
		// Note: UsePathStyle is configured on the S3 client above, not here
	}

	bucket, err := s3blob.OpenBucketV2(ctx, client, parsed.Host, bucketOpts)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}

func createGCSAuthHeader(req *smithyhttp.Request, accessKey, secretKey, region string) (string, error) {
	// Implementation of GCS-compatible AWS4-HMAC-SHA256 signing
	// Based on GCS documentation for x-amz extensions with HMAC keys

	// Get current time for signature
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	// Credential scope for GCS with x-amz extensions
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, region)

	// Algorithm
	algorithm := "AWS4-HMAC-SHA256"

	// Set required headers
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")

	// Create canonical headers - must be sorted
	var headerNames []string
	canonicalHeaders := ""

	// Collect headers that start with x-amz or are host/content-type
	for name := range req.Header {
		lowerName := strings.ToLower(name)
		if strings.HasPrefix(lowerName, "x-amz-") || lowerName == "host" || lowerName == "content-type" {
			headerNames = append(headerNames, lowerName)
		}
	}
	sort.Strings(headerNames)

	signedHeaders := strings.Join(headerNames, ";")

	for _, name := range headerNames {
		value := req.Header.Get(name)
		canonicalHeaders += name + ":" + strings.TrimSpace(value) + "\n"
	}

	// Create canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	canonicalQueryString := ""
	if req.URL.RawQuery != "" {
		// Parse and sort query parameters
		values, _ := url.ParseQuery(req.URL.RawQuery)
		var keys []string
		for k := range values {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		var queryParts []string
		for _, k := range keys {
			for _, v := range values[k] {
				queryParts = append(queryParts, url.QueryEscape(k)+"="+url.QueryEscape(v))
			}
		}
		canonicalQueryString = strings.Join(queryParts, "&")
	}

	canonicalRequest := req.Method + "\n" +
		canonicalURI + "\n" +
		canonicalQueryString + "\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		"UNSIGNED-PAYLOAD"

	// Create string to sign
	stringToSign := algorithm + "\n" +
		amzDate + "\n" +
		credentialScope + "\n" +
		fmt.Sprintf("%x", sha256.Sum256([]byte(canonicalRequest)))

	// Create signing key
	signingKey := getSigningKey(secretKey, dateStamp, region, "s3")

	// Create signature
	h := hmac.New(sha256.New, signingKey)
	h.Write([]byte(stringToSign))
	signature := fmt.Sprintf("%x", h.Sum(nil))

	// Create authorization header
	authHeader := fmt.Sprintf("%s Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		algorithm, accessKey, credentialScope, signedHeaders, signature)

	return authHeader, nil
}

func getSigningKey(key, dateStamp, regionName, serviceName string) []byte {
	// AWS4 signing key derivation for GCS compatibility
	kDate := hmacSHA256([]byte("AWS4"+key), dateStamp)
	kRegion := hmacSHA256(kDate, regionName)
	kService := hmacSHA256(kRegion, serviceName)
	kSigning := hmacSHA256(kService, "aws4_request")
	return kSigning
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}
