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

package glue

import (
	"context"
	"net/url"
	"os"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestGlueLoadTableWithIsolatedIORegistry(t *testing.T) {
	const scheme = "glue-private-registry"
	const metadataLocation = scheme + "://bucket/table/metadata.json"
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) { return fs, nil }))
	contents, err := os.ReadFile("../../table/testdata/TableMetadataV2Valid.json")
	require.NoError(t, err)
	require.NoError(t, fs.WriteFile(t.Context(), metadataLocation, contents))
	client := &mockGlueClient{}
	client.On("GetTable", mock.Anything, mock.Anything, mock.Anything).Return(&awsglue.GetTableOutput{Table: &types.Table{
		Name: aws.String("tbl"), DatabaseName: aws.String("db"), TableType: aws.String(glueTableType),
		Parameters: map[string]string{tableParamTableType: glueTypeIceberg, tableParamMetadataLocation: metadataLocation},
	}}, nil).Once()
	cat, err := NewCatalog(WithAWSConfig(aws.Config{Region: "us-east-1"}), WithIORegistry(registry))
	require.NoError(t, err)
	cat.glueSvc = client
	tbl, err := cat.LoadTable(t.Context(), TableIdentifier("db", "tbl"))
	require.NoError(t, err)
	loaded, err := tbl.FS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loaded)
	_, err = iceio.LoadFS(t.Context(), nil, metadataLocation)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	client.AssertExpectations(t)
}
