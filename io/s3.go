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
	"log"
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

	// bytes, _ := json.Marshal(props)
	// log.Printf("ParseAWSConfig: props => %s\n", string(bytes))

	if tok, ok := props[S3SignerAuthToken]; ok {
		log.Printf("ParseAWSConfig: Found signer auth token\n")
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	region := ""
	originalRegion := ""
	if r, ok := props[S3Region]; ok {
		originalRegion = r
		region = r
		log.Printf("ParseAWSConfig: Found s3.region = '%s'\n", region)
		opts = append(opts, config.WithRegion(region))
	} else if r, ok := props["client.region"]; ok {
		originalRegion = r
		region = r
		log.Printf("ParseAWSConfig: Found client.region = '%s'\n", region)
		// For Google Cloud Storage, "auto" region should be converted to a valid AWS region
		if region == "auto" && (strings.Contains(props[S3EndpointURL], "storage.googleapis.com") || strings.Contains(props[S3EndpointURL], "googleapis.com")) {
			log.Printf("ParseAWSConfig: Converting 'auto' region to 'us-east-1' for GCS compatibility\n")
			region = "us-east-1" // Default region for GCS compatibility
		}
		log.Printf("ParseAWSConfig: Using region = '%s' (original: '%s')\n", region, originalRegion)
		opts = append(opts, config.WithRegion(region))
	} else if r, ok := props["rest.signing-region"]; ok {
		originalRegion = r
		region = r
		log.Printf("ParseAWSConfig: Found rest.signing-region = '%s'\n", region)
		// For Google Cloud Storage, "auto" region should be converted to a valid AWS region
		if region == "auto" && (strings.Contains(props[S3EndpointURL], "storage.googleapis.com") || strings.Contains(props[S3EndpointURL], "googleapis.com")) {
			log.Printf("ParseAWSConfig: Converting 'auto' region to 'us-east-1' for GCS compatibility\n")
			region = "us-east-1" // Default region for GCS compatibility
		}
		log.Printf("ParseAWSConfig: Using region = '%s' (original: '%s')\n", region, originalRegion)
		opts = append(opts, config.WithRegion(region))
	} else {
		log.Printf("ParseAWSConfig: No region specified, using default\n")
	}

	endpoint := props[S3EndpointURL]
	log.Printf("ParseAWSConfig: Endpoint = '%s'\n", endpoint)

	// Check if remote signing is configured and enabled
	signerURI, hasSignerURI := props[S3SignerUri]
	signerEndpoint := props[S3SignerEndpoint]
	remoteSigningEnabled := true // Default to true for backward compatibility
	if enabledStr, ok := props[S3RemoteSigningEnabled]; ok {
		if enabled, err := strconv.ParseBool(enabledStr); err == nil {
			remoteSigningEnabled = enabled
		}
	}

	log.Printf("ParseAWSConfig: Remote signing - hasSignerURI: %v, signerURI: '%s', signerEndpoint: '%s', enabled: %v\n",
		hasSignerURI, signerURI, signerEndpoint, remoteSigningEnabled)

	if hasSignerURI && signerURI != "" && remoteSigningEnabled {
		log.Printf("ParseAWSConfig: Using remote signing configuration\n")
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
			log.Printf("ParseAWSConfig: Using proxy: %s\n", proxy)
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
		log.Printf("ParseAWSConfig: Using regular credentials (not remote signing)\n")
		// Use regular credentials if no remote signer
		accessKey, secretAccessKey := props[S3AccessKeyID], props[S3SecretAccessKey]
		token := props[S3SessionToken]
		log.Printf("ParseAWSConfig: Credentials - accessKey: '%s' (len=%d), secretKey: (len=%d), token: (len=%d)\n",
			accessKey, len(accessKey), len(secretAccessKey), len(token))

		if accessKey != "" || secretAccessKey != "" || token != "" {
			log.Printf("ParseAWSConfig: Setting static credentials provider\n")

			// Special handling for GCS - try a different approach
			if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
				log.Printf("ParseAWSConfig: Using GCS-compatible credential configuration\n")

				// For GCS, let's try using the credentials but with special handling
				// The key insight is that GCS HMAC keys work differently than AWS keys
				opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					accessKey, secretAccessKey, token)))
			} else {
				// Regular AWS credentials
				opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
					props[S3AccessKeyID], props[S3SecretAccessKey], props[S3SessionToken])))
			}
		} else {
			log.Printf("ParseAWSConfig: No credentials provided, using default provider chain\n")
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
			log.Printf("ParseAWSConfig: Using proxy: %s\n", proxy)
		}
	}

	log.Printf("ParseAWSConfig: Loading AWS config with %d options\n", len(opts))
	awscfg := new(aws.Config)
	var err error
	*awscfg, err = config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.Printf("ParseAWSConfig: ERROR loading config: %v\n", err)
		return nil, err
	}

	// Special handling for Google Cloud Storage
	if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
		log.Printf("ParseAWSConfig: Applying GCS-specific AWS config modifications\n")

		// GCS S3-compatible API has some specific requirements
		// Try to disable certain AWS-specific features that might not work with GCS
		awscfg.ClientLogMode = 0 // Disable detailed client logging that might interfere

		// Instead of anonymous credentials, let's use the actual credentials but configure
		// the signing process to be more compatible with GCS
		if accessKey := props[S3AccessKeyID]; accessKey != "" {
			secretKey := props[S3SecretAccessKey]
			token := props[S3SessionToken]
			log.Printf("ParseAWSConfig: Using HMAC credentials for GCS compatibility\n")
			awscfg.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, token)
		}

		log.Printf("ParseAWSConfig: Applied GCS compatibility settings\n")
	}

	log.Printf("ParseAWSConfig: Successfully loaded AWS config with region: %s\n", awscfg.Region)
	return awscfg, nil
}

func createS3Bucket(ctx context.Context, parsed *url.URL, props map[string]string) (*blob.Bucket, error) {
	log.Printf("createS3Bucket: Starting bucket creation for URL: %s\n", parsed.String())
	log.Printf("createS3Bucket: Host: %s, Path: %s\n", parsed.Host, parsed.Path)

	var (
		awscfg *aws.Config
		err    error
	)
	if v := utils.GetAwsConfig(ctx); v != nil {
		log.Printf("createS3Bucket: Using existing AWS config from context\n")
		awscfg = v
	} else {
		log.Printf("createS3Bucket: Parsing new AWS config from props\n")
		awscfg, err = ParseAWSConfig(ctx, props)
		if err != nil {
			log.Printf("createS3Bucket: ERROR parsing AWS config: %v\n", err)
			return nil, err
		}
	}

	endpoint, ok := props[S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
		log.Printf("createS3Bucket: No endpoint in props, using env var: %s\n", endpoint)
	} else {
		log.Printf("createS3Bucket: Using endpoint from props: %s\n", endpoint)
	}

	usePathStyle := true
	if forceVirtual, ok := props[S3ForceVirtualAddressing]; ok {
		if cfgForceVirtual, err := strconv.ParseBool(forceVirtual); err == nil {
			usePathStyle = !cfgForceVirtual
			log.Printf("createS3Bucket: Force virtual addressing set to %v, usePathStyle = %v\n", cfgForceVirtual, usePathStyle)
		}
	} else {
		log.Printf("createS3Bucket: No force virtual addressing specified, defaulting to path style\n")
	}

	// Google Cloud Storage requires path-style access when using S3-compatible API
	if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
		log.Printf("createS3Bucket: Detected Google Cloud Storage endpoint, forcing path-style access\n")
		usePathStyle = true
	}

	// Check if remote signing is enabled
	_, hasSignerURI := props[S3SignerUri]
	remoteSigningEnabled := true // Default to true for backward compatibility
	if enabledStr, ok := props[S3RemoteSigningEnabled]; ok {
		if enabled, err := strconv.ParseBool(enabledStr); err == nil {
			remoteSigningEnabled = enabled
		}
	}

	log.Printf("createS3Bucket: Remote signing - hasSignerURI: %v, enabled: %v\n", hasSignerURI, remoteSigningEnabled)
	log.Printf("createS3Bucket: Final configuration - endpoint: %s, usePathStyle: %v, region: %s\n", endpoint, usePathStyle, awscfg.Region)

	client := s3.NewFromConfig(*awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
			log.Printf("createS3Bucket: Set S3 client base endpoint to: %s\n", endpoint)
		}
		o.UsePathStyle = usePathStyle
		log.Printf("createS3Bucket: Set S3 client UsePathStyle to: %v\n", usePathStyle)
		o.DisableLogOutputChecksumValidationSkipped = true

		// Special configuration for Google Cloud Storage
		if endpoint != "" && (strings.Contains(endpoint, "storage.googleapis.com") || strings.Contains(endpoint, "googleapis.com")) {
			log.Printf("createS3Bucket: Applying GCS-specific configurations\n")

			// For GCS, we need to implement our own signing that's compatible with GCS requirements
			// The AWS SDK signing has subtle differences that don't work with GCS
			log.Printf("createS3Bucket: Implementing custom GCS-compatible signing\n")

			// Get the credentials for custom signing
			accessKey := props[S3AccessKeyID]
			secretKey := props[S3SecretAccessKey]

			if accessKey != "" && secretKey != "" {
				log.Printf("createS3Bucket: Setting up custom GCS signing transport\n")

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
									log.Printf("createS3Bucket: Error creating GCS auth header: %v\n", err)
									// Fall back to AWS signing
								} else {
									req.Header.Set("Authorization", authHeader)
									log.Printf("createS3Bucket: Set custom GCS authorization header\n")
								}

								log.Printf("createS3Bucket: Applied custom GCS signing\n")
							}
							return next.HandleFinalize(ctx, in)
						}),
						middleware.After,
					)
				})
			}

			// Add request/response logging for debugging
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Deserialize.Add(
					middleware.DeserializeMiddlewareFunc("GCSDebugLogging", func(
						ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
					) (middleware.DeserializeOutput, middleware.Metadata, error) {
						// Log the request
						if req, ok := in.Request.(*smithyhttp.Request); ok {
							log.Printf("GCS Request: %s %s\n", req.Method, req.URL.String())
							log.Printf("GCS Request Headers: %v\n", req.Header)
						}

						out, metadata, err := next.HandleDeserialize(ctx, in)

						// Log the response
						if err != nil {
							log.Printf("GCS Response Error: %v\n", err)
						} else if resp, ok := out.RawResponse.(*smithyhttp.Response); ok {
							log.Printf("GCS Response Status: %d %s\n", resp.StatusCode, resp.Status)
							log.Printf("GCS Response Headers: %v\n", resp.Header)
						}

						return out, metadata, err
					}),
					middleware.Before,
				)
			})
		}

		// If remote signing is enabled, configure the client to avoid chunked encoding
		if hasSignerURI && remoteSigningEnabled {
			log.Printf("createS3Bucket: Adding middleware to prevent chunked encoding for remote signing\n")
			// Add middleware to prevent chunked encoding
			o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
				return stack.Build.Add(
					middleware.BuildMiddlewareFunc("PreventChunkedEncoding", func(
						ctx context.Context, in middleware.BuildInput, next middleware.BuildHandler,
					) (middleware.BuildOutput, middleware.Metadata, error) {
						// Cast to smithy HTTP request
						req, ok := in.Request.(*smithyhttp.Request)
						if ok {
							// Force Content-Length header to prevent chunked encoding
							if req.ContentLength == 0 && req.Body != nil {
								// Try to read the body to determine length
								// Note: This is a workaround and may not work for all cases
							}

							// Remove any existing Content-Encoding header
							req.Header.Del("Content-Encoding")
							req.Header.Del("Transfer-Encoding")
						}
						return next.HandleBuild(ctx, in)
					}),
					middleware.After,
				)
			})
		}
	})

	log.Printf("createS3Bucket: Created S3 client successfully\n")

	// Create a *blob.Bucket with options
	bucketOpts := &s3blob.Options{
		// Note: UsePathStyle is configured on the S3 client above, not here
	}

	log.Printf("createS3Bucket: Opening bucket with host: %s\n", parsed.Host)
	bucket, err := s3blob.OpenBucketV2(ctx, client, parsed.Host, bucketOpts)
	if err != nil {
		log.Printf("createS3Bucket: ERROR opening bucket: %v\n", err)
		return nil, err
	}

	log.Printf("createS3Bucket: Successfully created bucket\n")
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

	log.Printf("createGCSAuthHeader: Generated signature for GCS: %s\n", authHeader[:50]+"...")
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
