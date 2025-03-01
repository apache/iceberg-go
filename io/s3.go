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
	"slices"
	"strconv"

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
	S3ForceVirtualAddressing = "s3.force-virtual-addressing"
)

var unsupportedS3Props = []string{
	S3ConnectTimeout,
	S3SignerUri,
}

// ParseAWSConfig parses S3 properties and returns a configuration.
func ParseAWSConfig(ctx context.Context, props map[string]string) (*aws.Config, error) {
	// If any unsupported properties are set, return an error.
	for k := range props {
		if slices.Contains(unsupportedS3Props, k) {
			return nil, fmt.Errorf("unsupported S3 property %q", k)
		}
	}

	opts := []func(*config.LoadOptions) error{}

	if tok, ok := props["token"]; ok {
		opts = append(opts, config.WithBearerAuthTokenProvider(
			&bearer.StaticTokenProvider{Token: bearer.Token{Value: tok}}))
	}

	if region, ok := props[S3Region]; ok {
		opts = append(opts, config.WithRegion(region))
	} else if region, ok := props["client.region"]; ok {
		opts = append(opts, config.WithRegion(region))
	}

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
