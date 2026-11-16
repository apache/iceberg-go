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

package remotesigning

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		props          map[string]string
		fallback, want string
	}{
		{"table default", nil, "https://catalog/base/v1/p/namespaces/db/tables/t/sign", "https://catalog/base/v1/p/namespaces/db/tables/t/sign"},
		{"modern explicit", map[string]string{EndpointProperty: "https://signer/current", "s3.signer.uri": "https://old"}, "", "https://signer/current"},
		{"modern config ignores legacy endpoint", map[string]string{ConfigProperty: "{}", "signer.endpoint": "https://old/sign"}, "https://catalog/table/sign", "https://catalog/table/sign"},
		{"modern config ignores legacy URI", map[string]string{ConfigProperty: "{}", "signer.uri": "https://old"}, "https://catalog/table/sign", "https://catalog/table/sign"},
		{"modern config ignores legacy S3 URI", map[string]string{ConfigProperty: "{}", "s3.signer.uri": "https://old"}, "https://catalog/table/sign", "https://catalog/table/sign"},
		{"modern config ignores all legacy settings", map[string]string{ConfigProperty: "{}", "signer.endpoint": "%%%", "signer.uri": "%%%", "s3.signer.uri": "%%%"}, "https://catalog/table/sign", "https://catalog/table/sign"},
		{"empty modern config is still present", map[string]string{ConfigProperty: "", "signer.uri": "https://old"}, "https://catalog/table/sign", "https://catalog/table/sign"},
		{"modern config with explicit endpoint", map[string]string{ConfigProperty: "{}", EndpointProperty: "https://signer/current", "signer.endpoint": "https://old/sign"}, "https://catalog/table/sign", "https://signer/current"},
		{"modern relative endpoint", map[string]string{ConfigProperty: "{}", EndpointProperty: "v1/table/sign?tenant=one", "uri": "https://catalog/base", "signer.uri": "https://old"}, "https://catalog/table/sign", "https://catalog/base/v1/table/sign?tenant=one"},
		{"modern escaped relative endpoint", map[string]string{EndpointProperty: "/custom%2Fsign?tenant=a%2Bb", "uri": "https://catalog/base/"}, "", "https://catalog/base/custom%2Fsign?tenant=a%2Bb"},
		{"legacy S3 base", map[string]string{"s3.signer.uri": "https://catalog/base/"}, "", "https://catalog/base/v1/aws/s3/sign"},
		{"legacy absolute endpoint", map[string]string{"signer.endpoint": "https://other/sign"}, "", "https://other/sign"},
		{"legacy relative endpoint", map[string]string{"signer.endpoint": "/custom/sign", "signer.uri": "https://other/base"}, "", "https://other/base/custom/sign"},
		{"legacy query", map[string]string{"signer.endpoint": "custom/sign?tenant=one&token=a%2Bb", "signer.uri": "https://other/base"}, "", "https://other/base/custom/sign?tenant=one&token=a%2Bb"},
		{"legacy escaped path", map[string]string{"signer.endpoint": "custom%2Fsign?tenant=one", "signer.uri": "https://other/base"}, "", "https://other/base/custom%2Fsign?tenant=one"},
		{"legacy catalog base", map[string]string{"signer.endpoint": "sign", "uri": "https://catalog/base"}, "", "https://catalog/base/sign"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Endpoint(tt.props, tt.fallback)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
	for _, endpoint := range []string{"sign#fragment", "//other/sign"} {
		_, err := Endpoint(map[string]string{"signer.endpoint": endpoint, "signer.uri": "https://catalog/base"}, "")
		require.Error(t, err)
	}
	for _, endpoint := range []string{"", "relative", "file:///sign", "https://user:secret@signer/sign", "https://signer/sign#fragment", "https://%"} {
		t.Run("invalid "+endpoint, func(t *testing.T) {
			_, err := Endpoint(map[string]string{EndpointProperty: endpoint}, "")
			require.Error(t, err)
		})
	}
	_, err := Endpoint(map[string]string{ConfigProperty: "{}", "signer.uri": "https://old"}, "")
	require.Error(t, err, "modern config without a table or explicit endpoint must not fall back to the legacy URI")
	_, err = Endpoint(map[string]string{ConfigProperty: "{}", EndpointProperty: "//other/sign", "uri": "https://catalog"}, "")
	require.Error(t, err, "a relative endpoint must not replace the catalog host")
}

func TestEndpointErrorsDoNotIncludeCredentials(t *testing.T) {
	t.Parallel()
	for _, props := range []map[string]string{
		{EndpointProperty: "https://signer.example/%invalid?token=secret-token"},
		{EndpointProperty: "sign", "uri": "https://catalog.example/%invalid?token=secret-token"},
		{"signer.uri": "https://signer.example/%invalid?token=secret-token"},
		{"signer.endpoint": "sign/%invalid?token=secret-token", "signer.uri": "https://signer.example"},
	} {
		_, err := Endpoint(props, "")
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secret-token")
	}
}
