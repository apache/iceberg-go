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
	"crypto/tls"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Option func(*options)

func WithCredential(cred string) Option {
	return func(o *options) {
		o.credential = cred
	}
}

func WithOAuthToken(token string) Option {
	return func(o *options) {
		o.oauthToken = token
	}
}

func WithTLSConfig(config *tls.Config) Option {
	return func(o *options) {
		o.tlsConfig = config
	}
}

func WithWarehouseLocation(loc string) Option {
	return func(o *options) {
		o.warehouseLocation = loc
	}
}

func WithMetadataLocation(loc string) Option {
	return func(o *options) {
		o.metadataLocation = loc
	}
}

func WithSigV4() Option {
	return func(o *options) {
		o.enableSigv4 = true
		o.sigv4Service = "execute-api"
	}
}

func WithSigV4RegionSvc(region, service string) Option {
	return func(o *options) {
		o.enableSigv4 = true
		o.sigv4Region = region

		if service == "" {
			o.sigv4Service = "execute-api"
		} else {
			o.sigv4Service = service
		}
	}
}

func WithAuthURI(uri *url.URL) Option {
	return func(o *options) {
		o.authUri = uri
	}
}

func WithPrefix(prefix string) Option {
	return func(o *options) {
		o.prefix = prefix
	}
}

func WithAwsConfig(cfg aws.Config) Option {
	return func(o *options) {
		o.awsConfig = cfg
	}
}

func WithScope(scope string) Option {
	return func(o *options) {
		o.scope = scope
	}
}

func WithAdditionalProps(props iceberg.Properties) Option {
	return func(o *options) {
		o.additionalProps = props
	}
}

type options struct {
	awsConfig         aws.Config
	tlsConfig         *tls.Config
	credential        string
	oauthToken        string
	warehouseLocation string
	metadataLocation  string
	enableSigv4       bool
	sigv4Region       string
	sigv4Service      string
	prefix            string
	authUri           *url.URL
	scope             string

	additionalProps iceberg.Properties
}
