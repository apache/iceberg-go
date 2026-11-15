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
	"errors"
	"net/url"
	"os"
	"testing"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
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

func TestGlueCatalogsWithSameSchemeStayIsolated(t *testing.T) {
	const scheme = "glue-shared-scheme"
	const metadataLocation = scheme + "://bucket/table/metadata.json"
	contents, err := os.ReadFile("../../table/testdata/TableMetadataV2Valid.json")
	require.NoError(t, err)

	filesystems := []*iceio.MemFS{iceio.NewMemFS(), iceio.NewMemFS()}
	catalogs := make([]*Catalog, len(filesystems))
	clients := make([]*mockGlueClient, len(filesystems))
	for i, fs := range filesystems {
		registry := &iceio.Registry{}
		require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
			return fs, nil
		}))
		require.NoError(t, fs.WriteFile(t.Context(), metadataLocation, contents))

		client := &mockGlueClient{}
		client.On("GetTable", mock.Anything, mock.Anything, mock.Anything).Return(&awsglue.GetTableOutput{Table: &types.Table{
			Name: aws.String("tbl"), DatabaseName: aws.String("db"), TableType: aws.String(glueTableType),
			Parameters: map[string]string{tableParamTableType: glueTypeIceberg, tableParamMetadataLocation: metadataLocation},
		}}, nil).Once()
		cat, err := NewCatalog(WithAWSConfig(aws.Config{Region: "us-east-1"}), WithIORegistry(registry))
		require.NoError(t, err)
		cat.glueSvc = client
		catalogs[i] = cat
		clients[i] = client
	}

	// Load after both catalogs exist. A shared/global factory would route
	// both loads to one filesystem even though the URI scheme is identical.
	for i, cat := range catalogs {
		tbl, err := cat.LoadTable(t.Context(), TableIdentifier("db", "tbl"))
		require.NoError(t, err)
		loaded, err := tbl.FS(t.Context())
		require.NoError(t, err)
		require.Same(t, filesystems[i], loaded)
		require.NotSame(t, filesystems[1-i], loaded)
		clients[i].AssertExpectations(t)
	}
}

func TestGlueCreateAndCommitTableUseCatalogIORegistry(t *testing.T) {
	const scheme = "glue-create-commit-registry"
	const warehouse = scheme + "://bucket/warehouse"
	registry := &iceio.Registry{}
	fs := iceio.NewMemFS()
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))

	cat, err := NewCatalog(
		WithAWSConfig(aws.Config{Region: "us-east-1"}),
		WithAWSProperties(AWSProperties{"warehouse": warehouse}),
		WithIORegistry(registry),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	client := &mockGlueClient{}
	cat.glueSvc = client

	identifier := TableIdentifier("db", "tbl")
	var glueTable types.Table
	client.On("GetDatabase", mock.Anything, mock.Anything, mock.Anything).
		Return(&awsglue.GetDatabaseOutput{Database: &types.Database{Name: aws.String("db")}}, nil).Once()
	client.On("CreateTable", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			input := args.Get(1).(*awsglue.CreateTableInput)
			glueTable = types.Table{
				Name: aws.String("tbl"), DatabaseName: aws.String("db"),
				TableType: aws.String(glueTableType), VersionId: aws.String("1"),
				Parameters:        input.TableInput.Parameters,
				StorageDescriptor: input.TableInput.StorageDescriptor,
			}
		}).Return(&awsglue.CreateTableOutput{}, nil).Once()
	client.On("GetTable", mock.Anything, mock.Anything, mock.Anything).
		Return(&awsglue.GetTableOutput{Table: &glueTable}, nil).Twice()
	client.On("UpdateTable", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			input := args.Get(1).(*awsglue.UpdateTableInput)
			glueTable.Parameters = input.TableInput.Parameters
			glueTable.StorageDescriptor = input.TableInput.StorageDescriptor
		}).Return(&awsglue.UpdateTableOutput{}, nil).Once()

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	created, err := cat.CreateTable(t.Context(), identifier, schema)
	require.NoError(t, err)
	loadedFS, err := created.FS(t.Context())
	require.NoError(t, err)
	require.Same(t, fs, loadedFS)

	_, updatedLocation, err := cat.CommitTable(t.Context(), identifier, nil, []table.Update{
		table.NewSetPropertiesUpdate(iceberg.Properties{"committed": "true"}),
	})
	require.NoError(t, err)
	require.NotEqual(t, created.MetadataLocation(), updatedLocation)
	metadataFile, err := fs.Open(t.Context(), updatedLocation)
	require.NoError(t, err)
	require.NoError(t, metadataFile.Close())
	_, err = iceio.LoadFS(t.Context(), nil, updatedLocation)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	client.AssertExpectations(t)
}

type trackingGlueRegistryFS struct {
	*iceio.MemFS
	removed []string
}

func (fs *trackingGlueRegistryFS) Remove(ctx context.Context, path string) error {
	fs.removed = append(fs.removed, path)

	return fs.MemFS.Remove(ctx, path)
}

func TestGlueS3TablesCleanupUsesCatalogIORegistry(t *testing.T) {
	const scheme = "glue-s3tables-cleanup-registry"
	const managedLocation = scheme + "://bucket/table"
	registry := &iceio.Registry{}
	fs := &trackingGlueRegistryFS{MemFS: iceio.NewMemFS()}
	require.NoError(t, registry.Register(scheme, func(context.Context, *url.URL, map[string]string) (iceio.IO, error) {
		return fs, nil
	}))

	cat, err := NewCatalog(WithAWSConfig(aws.Config{Region: "us-east-1"}), WithIORegistry(registry))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cat.Close()) })
	client := &mockGlueClient{}
	cat.glueSvc = client
	client.On("GetDatabase", mock.Anything, mock.Anything, mock.Anything).
		Return(federatedDatabaseOutput("aws:s3tables"), nil).Once()
	client.On("CreateTable", mock.Anything, mock.Anything, mock.Anything).
		Return(&awsglue.CreateTableOutput{}, nil).Once()
	client.On("GetTable", mock.Anything, mock.Anything, mock.Anything).
		Return(&awsglue.GetTableOutput{Table: &types.Table{
			Name: aws.String("tbl"), DatabaseName: aws.String("db"), VersionId: aws.String("1"),
			StorageDescriptor: &types.StorageDescriptor{Location: aws.String(managedLocation)},
		}}, nil).Once()
	var metadataLocation string
	client.On("UpdateTable", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			input := args.Get(1).(*awsglue.UpdateTableInput)
			metadataLocation = input.TableInput.Parameters[tableParamMetadataLocation]
		}).Return((*awsglue.UpdateTableOutput)(nil), errors.New("update failed")).Once()
	client.On("DeleteTable", mock.Anything, mock.Anything, mock.Anything).
		Return(&awsglue.DeleteTableOutput{}, nil).Once()

	schema := iceberg.NewSchema(1, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32})
	_, err = cat.CreateTable(t.Context(), TableIdentifier("db", "tbl"), schema)
	require.ErrorContains(t, err, "update failed")
	require.NotEmpty(t, metadataLocation)
	require.Equal(t, []string{metadataLocation}, fs.removed)
	_, err = fs.Open(t.Context(), metadataLocation)
	require.Error(t, err)
	_, err = iceio.LoadFS(t.Context(), nil, metadataLocation)
	require.ErrorIs(t, err, iceio.ErrIOSchemeNotFound)
	client.AssertExpectations(t)
}
