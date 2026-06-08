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
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEndpointFromString(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		want    endpoint
		wantErr bool
	}{
		{in: "GET /v1/{prefix}/namespaces", want: endpoint{http.MethodGet, "/v1/{prefix}/namespaces"}},
		{in: "POST /v1/{prefix}/transactions/commit", want: endpoint{http.MethodPost, "/v1/{prefix}/transactions/commit"}},
		{in: "HEAD /v1/{prefix}/namespaces/{namespace}", want: endpoint{http.MethodHead, "/v1/{prefix}/namespaces/{namespace}"}},
		// Extra surrounding/internal whitespace is tolerated.
		{in: "  DELETE   /v1/{prefix}/namespaces/{namespace}  ", want: endpoint{http.MethodDelete, "/v1/{prefix}/namespaces/{namespace}"}},
		{in: "", wantErr: true},
		{in: "GET", wantErr: true},
		{in: "GET /a /b", wantErr: true},
	}

	for _, tc := range cases {
		got, err := endpointFromString(tc.in)
		if tc.wantErr {
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrRESTError)

			continue
		}
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
}

func TestEndpointRoundTripString(t *testing.T) {
	t.Parallel()

	for _, e := range fallbackEndpoints {
		got, err := endpointFromString(e.String())
		require.NoError(t, err)
		assert.Equal(t, e, got)
	}
}

func TestEndpointSetCheck(t *testing.T) {
	t.Parallel()

	s := newEndpointSet([]endpoint{endpointLoadTable})
	require.NoError(t, s.check(endpointLoadTable))

	err := s.check(endpointCreateTable)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEndpointNotSupported)
	assert.ErrorIs(t, err, ErrRESTError)
	assert.Contains(t, err.Error(), endpointCreateTable.String())

	// A nil set means capabilities were never negotiated: everything is allowed.
	var nilSet endpointSet
	require.NoError(t, nilSet.check(endpointCreateTable))
}

func TestResolveEndpointsFallback(t *testing.T) {
	t.Parallel()

	// When the server advertises nothing, the backward-compatible fallback set
	// is used: the full set of operations iceberg-go knows how to invoke.
	for _, advertised := range [][]string{nil, {}, {"garbage", "alsobad"}} {
		s := resolveEndpoints(advertised)
		for _, e := range fallbackEndpoints {
			assert.Truef(t, s.contains(e), "fallback should contain %s", e)
		}
	}
}

func TestResolveEndpointsAdvertised(t *testing.T) {
	t.Parallel()

	// When the server advertises a set, only those endpoints are honored;
	// unparseable entries are ignored.
	s := resolveEndpoints([]string{
		endpointLoadTable.String(),
		endpointListNamespaces.String(),
		"not-an-endpoint",
	})

	assert.True(t, s.contains(endpointLoadTable))
	assert.True(t, s.contains(endpointListNamespaces))
	assert.False(t, s.contains(endpointCreateTable))
	assert.False(t, s.contains(endpointLoadView))
	assert.Len(t, s, 2)
}
