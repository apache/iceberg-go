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

// Package sigv4 provides AWS Signature Version 4 signing for the REST catalog
// client. It is an optional backend: importing it (even as a blank import)
// registers the "sigv4" signer so that WithSigV4 / the rest.sigv4-enabled
// property work, and it keeps the AWS SDK out of the core catalog/rest package
// for consumers that do not need it.
//
//	import (
//		"github.com/apache/iceberg-go/catalog/rest"
//		_ "github.com/apache/iceberg-go/catalog/rest/sigv4"
//	)
//
//	cat, err := rest.NewCatalog(ctx, "c", uri, rest.WithSigV4RegionSvc("us-east-1", "s3tables"))
//
// To sign with an explicit aws.Config, pass sigv4.WithAwsConfig alongside
// WithSigV4 / WithSigV4RegionSvc (no blank import needed); the option supplies
// the config while those options still drive the signing region and service.
package sigv4

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

// emptyStringHash is the SHA-256 of the empty string, used as the payload hash
// for requests without a body.
// from https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/signer/v4#Signer.SignHTTP
const emptyStringHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// defaultSigningService is the SigV4 service name used when none is supplied,
// matching the Iceberg Java reference default (AwsProperties.REST_SIGNING_NAME_DEFAULT).
const defaultSigningService = "execute-api"

func init() {
	rest.RegisterSigner(rest.SignerNameSigV4, func(ctx context.Context, cfg rest.SignerConfig) (rest.RequestSigner, error) {
		awscfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("sigv4: load AWS config: %w", err)
		}

		return buildSigner(awscfg, cfg), nil
	})
}

// buildSigner applies the REST-core signing config to an aws.Config and returns
// a ready signer. A non-empty cfg.Region overrides awscfg.Region; an empty
// service falls back to defaultSigningService. The latter covers the
// property-only path (rest.sigv4-enabled without rest.signing-name), matching
// WithSigV4, so signatures stay valid.
func buildSigner(awscfg aws.Config, cfg rest.SignerConfig) *signer {
	if cfg.Region != "" {
		awscfg.Region = cfg.Region
	}

	service := cfg.Service
	if service == "" {
		service = defaultSigningService
	}

	return newSigner(awscfg, service)
}

// signer signs HTTP requests with AWS Signature Version 4. It implements
// rest.RequestSigner.
type signer struct {
	httpSigner v4.HTTPSigner
	cfg        aws.Config
	service    string
}

func newSigner(cfg aws.Config, service string) *signer {
	return &signer{
		httpSigner: v4.NewSigner(),
		cfg:        cfg,
		service:    service,
	}
}

// SignRequest signs r in place with SigV4: it computes the payload hash, sets
// the required x-amz-content-sha256 header, and applies the signature.
func (s *signer) SignRequest(r *http.Request) error {
	var payloadHash string
	if r.Body == nil {
		payloadHash = emptyStringHash
	} else {
		// SigV4 needs the payload hash, so the body must be re-readable.
		// http.NewRequest* sets GetBody for the standard body types; a
		// hand-built request with a raw io.ReadCloser body and no GetBody cannot
		// be signed, so fail with a clear error instead of a nil-func panic.
		if r.GetBody == nil {
			return errors.New("sigv4: cannot sign request whose body is not re-readable (Request.GetBody is nil)")
		}

		rdr, err := r.GetBody()
		if err != nil {
			return err
		}

		h := sha256.New()
		if _, err = io.Copy(h, rdr); err != nil {
			if closeErr := rdr.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}

			return err
		}

		if err = rdr.Close(); err != nil {
			return err
		}

		payloadHash = hex.EncodeToString(h.Sum(nil))
	}

	creds, err := s.cfg.Credentials.Retrieve(r.Context())
	if err != nil {
		return err
	}

	// Set the x-amz-content-sha256 header before signing; SigV4 signature
	// verification requires it.
	r.Header.Set("x-amz-content-sha256", payloadHash)

	return s.httpSigner.SignHTTP(r.Context(), creds, r, payloadHash, s.service, s.cfg.Region, time.Now())
}

// WithAwsConfig returns a rest.Option that signs catalog requests with AWS
// SigV4 using the supplied aws.Config, without consulting the AWS default
// credential chain. It replaces the former rest.WithAwsConfig, whose AWS SDK
// dependency now lives only in this optional sub-package.
//
// Like the blank-import path, it does not by itself enable signing: pair it
// with WithSigV4 or WithSigV4RegionSvc (or the rest.sigv4-enabled property),
// which remain the single source of the signing region and service. Those
// values, plus any server-provided /v1/config overrides, are applied on top of
// cfg when the signer is built (a non-empty region overrides cfg.Region; an
// empty service defaults to "execute-api").
func WithAwsConfig(cfg aws.Config) rest.Option {
	return rest.WithSignerFactory(func(_ context.Context, sc rest.SignerConfig) (rest.RequestSigner, error) {
		return buildSigner(cfg, sc), nil
	})
}
