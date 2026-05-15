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
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/auth/bearer"
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

	var httpClient *awshttp.BuildableClient

	if proxy, ok := props[io.S3ProxyURI]; ok {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 proxy url '%s'", proxy)
		}

		httpClient = awshttp.NewBuildableClient().WithTransportOptions(
			func(t *http.Transport) {
				t.Proxy = http.ProxyURL(proxyURL)
			},
		)
	}

	if connectTimeout, ok := props[io.S3ConnectTimeout]; ok {
		timeout, err := time.ParseDuration(connectTimeout)
		if err != nil {
			return nil, fmt.Errorf("invalid s3 connect timeout %q: %w", connectTimeout, err)
		}

		if httpClient == nil {
			httpClient = awshttp.NewBuildableClient()
		}
		httpClient = httpClient.WithDialerOptions(func(d *net.Dialer) {
			d.Timeout = timeout
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
	// proxy, TLS, dial, and HTTP/2 behavior match the usual AWS defaults, but
	// raise per-host idle limits (Go's DefaultTransport uses 2 per host).
	if awscfg.HTTPClient == nil {
		awscfg.HTTPClient = awshttp.NewBuildableClient().WithTransportOptions(
			func(t *http.Transport) {
				t.MaxIdleConns = 256
				t.MaxIdleConnsPerHost = 256
				t.MaxConnsPerHost = 256
				t.IdleConnTimeout = 90 * time.Second
			},
		)
	}

	endpoint, ok := props[io.S3EndpointURL]
	if !ok {
		endpoint = os.Getenv("AWS_S3_ENDPOINT")
	}

	client := s3.NewFromConfig(*awscfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
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
