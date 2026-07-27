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

package gocloud

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
	smithymiddleware "github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"gocloud.dev/blob"
	"gocloud.dev/blob/s3blob"
)

// ParseAWSConfig parses S3 properties and returns a configuration.
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	// Remote S3 request signing is not implemented yet.
	if v, ok := props[io.S3RemoteSigningEnabled]; ok {
		if enabled, err := strconv.ParseBool(v); err == nil && enabled {
			return nil, errors.New("remote S3 request signing is not supported")
		}
	}

	opts := []func(*config.LoadOptions) error{}
	var httpClient *awshttp.BuildableClient

	if tok, ok := props["token"]; ok {
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	if region, ok := props[io.S3Region]; ok {
		opts = append(opts, config.WithRegion(region))
	} else if region, ok := props[io.S3ClientRegion]; ok {
		opts = append(opts, config.WithRegion(region))
	}

	accessKey, secretAccessKey := props[io.S3AccessKeyID], props[io.S3SecretAccessKey]
	token := props[io.S3SessionToken]
	if accessKey != "" || secretAccessKey != "" || token != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			props[io.S3AccessKeyID], props[io.S3SecretAccessKey], props[io.S3SessionToken])))
	}

	if proxy, ok := props[io.S3ProxyURI]; ok {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 proxy url %q: %w", proxy, err)
		}

		httpClient = newS3BuildableClient().WithTransportOptions(
			func(t *http.Transport) {
				t.Proxy = http.ProxyURL(proxyURL)
			},
		)
	}

	if timeout, ok := props[io.S3ConnectTimeout]; ok {
		duration, err := parseS3ConnectTimeout(timeout)
		if err != nil {
			return nil, err
		}

		if httpClient == nil {
			httpClient = newS3BuildableClient()
		}
		httpClient = httpClient.WithDialerOptions(func(d *net.Dialer) {
			d.Timeout = duration
		})
	}

	if httpClient != nil {
		opts = append(opts, config.WithHTTPClient(httpClient))
	}

	awscfg := new(aws.Config)
	var err error
	*awscfg, err = config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	return awscfg, nil
}

func parseS3ConnectTimeout(timeout string) (time.Duration, error) {
	var duration time.Duration
	if seconds, err := strconv.ParseFloat(timeout, 64); err == nil {
		duration = time.Duration(seconds * float64(time.Second))
	} else {
		parsedDuration, err := time.ParseDuration(timeout)
		if err != nil {
			return 0, fmt.Errorf("invalid s3.connect-timeout %q: must be seconds as a number or a Go duration string", timeout)
		}
		duration = parsedDuration
	}

	if duration <= 0 {
		return 0, errors.New("s3.connect-timeout must be a positive duration")
	}

	return duration, nil
}

// S3 transport tuning shared by all S3 BuildableClient paths.
const (
	s3MaxIdleConns        = 256
	s3MaxIdleConnsPerHost = 256
	s3MaxConnsPerHost     = 2048
	s3IdleConnTimeout     = 90 * time.Second
)

// newS3BuildableClient returns an AWS buildable HTTP client with the S3
// transport tuning applied. Subsequent WithTransportOptions/WithDialerOptions
// calls preserve this tuning, since the builder clones the transport forward.
func newS3BuildableClient() *awshttp.BuildableClient {
	return awshttp.NewBuildableClient().WithTransportOptions(applyS3TransportTuning)
}

func applyS3TransportTuning(t *http.Transport) {
	t.MaxIdleConns = s3MaxIdleConns
	t.MaxIdleConnsPerHost = s3MaxIdleConnsPerHost
	t.MaxConnsPerHost = s3MaxConnsPerHost
	t.IdleConnTimeout = s3IdleConnTimeout
}

// resolveUsePathStyle determines whether the S3 client should use
// path-style addressing. It defaults to virtual-hosted style for
// standard AWS S3 and path-style for custom endpoints (e.g. MinIO).
// The s3.force-virtual-addressing property can override either default.
func resolveUsePathStyle(endpoint string, props map[string]string) bool {
	usePathStyle := endpoint != ""
	if forceVirtual, ok := props[io.S3ForceVirtualAddressing]; ok {
		if cfgForceVirtual, err := strconv.ParseBool(forceVirtual); err == nil {
			usePathStyle = !cfgForceVirtual
		}
	}

	return usePathStyle
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

	// Default HTTP client when not configured: use the SDK buildable client so
	// proxy, TLS, dial, and HTTP/2 behavior match the usual AWS defaults, with
	// the S3 transport tuning applied (see applyS3TransportTuning).
	if awscfg.HTTPClient == nil {
		awscfg.HTTPClient = newS3BuildableClient()
	}

	endpoint, ok := props[io.S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	compatMode := s3CompatModeEnabled(props)

	client := s3.NewFromConfig(*awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		if compatMode {
			o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			o.APIOptions = append(o.APIOptions, stripS3InputChecksumAlgorithm)
			o.APIOptions = append(o.APIOptions, stripGCSIncompatibleSignedHeaders)
		}
		o.UsePathStyle = resolveUsePathStyle(endpoint, props)
		o.DisableLogOutputChecksumValidationSkipped = true
	})

	// Create a *blob.Bucket.
	bucket, err := s3blob.OpenBucketV2(ctx, client, parsed.Host, nil)
	if err != nil {
		return nil, err
	}

	return bucket, nil
}

func stripS3InputChecksumAlgorithm(stack *smithymiddleware.Stack) error {
	m := smithymiddleware.InitializeMiddlewareFunc(
		"iceberg-go/strip-s3-input-checksum-algorithm",
		func(ctx context.Context, in smithymiddleware.InitializeInput, next smithymiddleware.InitializeHandler) (smithymiddleware.InitializeOutput, smithymiddleware.Metadata, error) {
			switch v := in.Parameters.(type) {
			case *s3.PutObjectInput:
				v.ChecksumAlgorithm = ""
			case *s3.UploadPartInput:
				v.ChecksumAlgorithm = ""
			case *s3.CreateMultipartUploadInput:
				v.ChecksumAlgorithm = ""
			}

			return next.HandleInitialize(ctx, in)
		},
	)

	if err := stack.Initialize.Insert(m, "AWSChecksum:SetupInputContext", smithymiddleware.Before); err != nil {
		return stack.Initialize.Add(m, smithymiddleware.Before)
	}

	return nil
}

// SDK-internal headers that GCS's S3 interop endpoint doesn't expect in the SigV4 signed set
var gcsIncompatibleSignedHeaders = []string{
	"Amz-Sdk-Invocation-Id",
	"Amz-Sdk-Request",
	"Accept-Encoding",
}

// limits header stripping to write ops;
// reads must keep Accept-Encoding: identity so responses aren't silently gzip-decompressed.
var gcsStripSignedHeaderOps = map[string]struct{}{
	"PutObject":             {},
	"UploadPart":            {},
	"CreateMultipartUpload": {},
}

func stripGCSIncompatibleSignedHeaders(stack *smithymiddleware.Stack) error {
	m := smithymiddleware.FinalizeMiddlewareFunc(
		"iceberg-go/strip-gcs-incompatible-signed-headers",
		func(ctx context.Context, in smithymiddleware.FinalizeInput, next smithymiddleware.FinalizeHandler) (smithymiddleware.FinalizeOutput, smithymiddleware.Metadata, error) {
			if _, isWrite := gcsStripSignedHeaderOps[awsmiddleware.GetOperationName(ctx)]; isWrite {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					for _, h := range gcsIncompatibleSignedHeaders {
						req.Header.Del(h)
					}
				}
			}

			return next.HandleFinalize(ctx, in)
		},
	)

	if err := stack.Finalize.Insert(m, "Signing", smithymiddleware.Before); err != nil {
		return stack.Finalize.Add(m, smithymiddleware.Before)
	}

	return nil
}

func s3CompatModeEnabled(props map[string]string) bool {
	if v, ok := props[io.S3CompatMode]; ok {
		if enabled, err := strconv.ParseBool(v); err == nil {
			return enabled
		}
	}

	return false
}
