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
	"context"
	"fmt"
	"net/http"
	"sync"
)

// SignerNameSigV4 is the scheme under which the AWS SigV4 backend registers
// itself (see catalog/rest/sigv4). It is also the value carried by the
// rest.sigv4-enabled property path. It is exported so the backend can register
// under the exact same name the core looks up, rather than a duplicated string
// literal that could silently drift.
const SignerNameSigV4 = "sigv4"

// RequestSigner signs an outgoing catalog HTTP request in place, for example
// with AWS SigV4. Implementations live in optional sub-packages (see
// github.com/apache/iceberg-go/catalog/rest/sigv4) so the core REST client
// depends on no cloud SDK. Signing runs inside the session transport's
// RoundTrip, after auth headers are applied, and may use req.Context.
type RequestSigner interface {
	SignRequest(req *http.Request) error
}

// SignerConfig carries the signing parameters the REST core knows about,
// deliberately free of any cloud-SDK types. A SignerFactory turns it into a
// concrete RequestSigner.
type SignerConfig struct {
	// Region is the signing region (SigV4 signing-region).
	Region string
	// Service is the signing service name (SigV4 signing-name), e.g.
	// "execute-api", "s3tables".
	Service string
}

// SignerFactory builds a RequestSigner from core configuration. A signing
// backend registers one under a scheme name via RegisterSigner, so that a
// blank import of the backend package is enough to enable property- or
// option-driven signing (e.g. WithSigV4 / rest.sigv4-enabled).
type SignerFactory func(ctx context.Context, cfg SignerConfig) (RequestSigner, error)

var (
	signerMu       sync.RWMutex
	signerRegistry = map[string]SignerFactory{}
)

// RegisterSigner registers a signer factory under name (e.g. "sigv4"). It is
// intended to be called from a backend package's init function. Registering
// the same name twice replaces the previous factory.
func RegisterSigner(name string, factory SignerFactory) {
	signerMu.Lock()
	defer signerMu.Unlock()
	signerRegistry[name] = factory
}

func lookupSigner(name string) (SignerFactory, bool) {
	signerMu.RLock()
	defer signerMu.RUnlock()
	f, ok := signerRegistry[name]

	return f, ok
}

// resolveSigner determines the request signer for a session. An explicit
// WithSigner wins; otherwise, when SigV4 is enabled (WithSigV4 or the
// rest.sigv4-enabled property), the registered "sigv4" backend builds one. It
// returns (nil, nil) when signing is not configured, and a helpful error when
// SigV4 is requested but no backend has been imported.
func resolveSigner(ctx context.Context, opts *options) (RequestSigner, error) {
	if opts.signer != nil {
		return opts.signer, nil
	}

	if !opts.enableSigv4 {
		return nil, nil
	}

	factory, ok := lookupSigner(SignerNameSigV4)
	if !ok {
		return nil, fmt.Errorf(
			"rest: SigV4 signing was requested (%s) but no signer backend is registered; add a blank import: import _ %q",
			keyRestSigV4, "github.com/apache/iceberg-go/catalog/rest/sigv4")
	}

	return factory(ctx, SignerConfig{Region: opts.sigv4Region, Service: opts.sigv4Service})
}
