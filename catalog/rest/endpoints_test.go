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
	"strings"
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

	all := append(append([]endpoint{}, defaultEndpoints...), viewEndpoints...)
	for _, e := range all {
		got, err := endpointFromString(e.String())
		require.NoError(t, err)
		assert.Equal(t, e, got)
	}
}

func TestReqPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		ep      endpoint
		params  []string
		want    []string
		wantErr bool
	}{
		{name: "no params", ep: endpointCommitTransaction, want: []string{"transactions", "commit"}},
		{name: "one param", ep: endpointCreateTable, params: []string{"ns"}, want: []string{"namespaces", "ns", "tables"}},
		{name: "two params", ep: endpointLoadTable, params: []string{"ns", "tbl"}, want: []string{"namespaces", "ns", "tables", "tbl"}},
		{name: "too few", ep: endpointLoadTable, params: []string{"ns"}, wantErr: true},
		{name: "too many", ep: endpointCommitTransaction, params: []string{"extra"}, wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.ep.reqPath(tc.params...)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ErrRESTError)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEndpointConstantsWellFormed(t *testing.T) {
	t.Parallel()

	for _, e := range allEndpoints {
		assert.Truef(t, strings.HasPrefix(e.path, pathPrefix+"/"),
			"%s: path must start with %q", e, pathPrefix)
		_, err := e.reqPath(make([]string, e.nparams())...)
		assert.NoErrorf(t, err, "%s: should render with %d params", e, e.nparams())
	}
}

func TestEndpointSetCheck(t *testing.T) {
	t.Parallel()

	s := newEndpointSet([]endpoint{endpointLoadTable})
	require.NoError(t, s.check(endpointLoadTable))

	err := s.check(endpointCreateTable)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEndpointNotSupported)
	// A capability error is distinct from a transport error.
	assert.NotErrorIs(t, err, ErrRESTError)
	assert.Contains(t, err.Error(), endpointCreateTable.String())

	// A nil set means capabilities were never negotiated: everything is allowed.
	var nilSet endpointSet
	require.NoError(t, nilSet.check(endpointCreateTable))
}

func TestResolveEndpointsFallback(t *testing.T) {
	t.Parallel()

	// No advertised endpoints falls back to the spec default set, without views.
	for _, advertised := range [][]string{nil, {}} {
		s := resolveEndpoints(advertised, false)
		for _, e := range defaultEndpoints {
			assert.Truef(t, s.contains(e), "fallback should contain %s", e)
		}
		assert.False(t, s.contains(endpointListViews))
		// HEAD existence endpoints are excluded so Check*Exists degrades to GET.
		assert.False(t, s.contains(endpointTableExists))
	}

	// View endpoints are merged in only when view-endpoints-supported is set.
	s := resolveEndpoints(nil, true)
	for _, e := range viewEndpoints {
		assert.Truef(t, s.contains(e), "view fallback should contain %s", e)
	}
}

func TestResolveEndpointsAdvertised(t *testing.T) {
	t.Parallel()

	// A non-empty advertised list is authoritative; unparseable entries are
	// dropped and nothing else is assumed.
	s := resolveEndpoints([]string{
		endpointLoadTable.String(),
		endpointListNamespaces.String(),
		"not-an-endpoint",
	}, false)

	assert.True(t, s.contains(endpointLoadTable))
	assert.True(t, s.contains(endpointListNamespaces))
	assert.False(t, s.contains(endpointCreateTable))
	assert.False(t, s.contains(endpointLoadView))
	assert.Len(t, s, 2)
}
