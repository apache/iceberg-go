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

//go:build integration

package io_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	sqlcat "github.com/apache/iceberg-go/catalog/sql"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/suite"
	"github.com/uptrace/bun/driver/sqliteshim"
	"gocloud.dev/blob/azureblob"
)

const (
	accountName              = "devstoreaccount1"
	accountKey               = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	endpoint                 = "127.0.0.1:11000"
	protocol                 = "http"
	containerName            = "warehouse"
	connectionStringTemplate = "DefaultEndpointsProtocol=%s;AccountName=%s;AccountKey=%s;BlobEndpoint=%s://%s/%s;"
)

type AzureBlobIOTestSuite struct {
	suite.Suite

	ctx context.Context
}

func (s *AzureBlobIOTestSuite) SetupTest() {
	s.ctx = context.Background()

	s.Require().NoError(s.createContainerIfNotExist(containerName))
}

func (s *AzureBlobIOTestSuite) TestAzureBlobWarehouseKey() {
	path := "iceberg-test-azure/test-table-azure"
	containerName := "warehouse"
	properties := iceberg.Properties{
		"uri":                       ":memory:",
		sqlcat.DriverKey:            sqliteshim.ShimName,
		sqlcat.DialectKey:           string(sqlcat.SQLite),
		"type":                      "sql",
		io.AdlsSharedKeyAccountName: accountName,
		io.AdlsSharedKeyAccountKey:  accountKey,
		io.AdlsEndpoint:             endpoint,
		io.AdlsProtocol:             protocol,
	}

	cat, err := catalog.Load(context.Background(), "default", properties)
	s.Require().NoError(err)
	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	s.Require().NoError(c.CreateNamespace(s.ctx, catalog.ToIdentifier("iceberg-test-azure"), nil))

	tbl, err := c.CreateTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("abfs://%s/iceberg/%s", containerName, path)))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	tbl, err = c.LoadTable(s.ctx, catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func (s *AzureBlobIOTestSuite) TestAzuriteWarehouseConnectionString() {
	connectionString := fmt.Sprintf(connectionStringTemplate, protocol, accountName, accountKey, protocol, endpoint, accountName)
	path := "iceberg-test-azure/test-table-azure"
	containerName := "warehouse"
	properties := iceberg.Properties{
		"uri":                       ":memory:",
		sqlcat.DriverKey:            sqliteshim.ShimName,
		sqlcat.DialectKey:           string(sqlcat.SQLite),
		"type":                      "sql",
		io.AdlsSharedKeyAccountName: accountName,
		io.AdlsConnectionStringPrefix + accountName: connectionString,
	}

	cat, err := catalog.Load(context.Background(), "default", properties)
	s.Require().NoError(err)
	s.Require().NotNil(cat)

	c := cat.(*sqlcat.Catalog)
	s.Require().NoError(c.CreateNamespace(s.ctx, catalog.ToIdentifier("iceberg-test-azure"), nil))

	tbl, err := c.CreateTable(s.ctx,
		catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"),
		iceberg.NewSchema(0, iceberg.NestedField{
			Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true, ID: 1,
		}), catalog.WithLocation(fmt.Sprintf("wasb://%s/iceberg/%s", containerName, path)))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	tbl, err = c.LoadTable(s.ctx, catalog.ToIdentifier("iceberg-test-azure", "test-table-azure"))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)
}

func (s *AzureBlobIOTestSuite) createContainerIfNotExist(containerName string) error {
	svcURL, err := azureblob.NewServiceURL(&azureblob.ServiceURLOptions{
		AccountName:   accountName,
		Protocol:      protocol,
		StorageDomain: endpoint,
	})
	if err != nil {
		return err
	}

	sharedKeyCred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return err
	}

	client, err := azblob.NewClientWithSharedKeyCredential(string(svcURL), sharedKeyCred, nil)
	if err != nil {
		return err
	}

	_, err = client.CreateContainer(s.ctx, containerName, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return err
	}

	return nil
}

func TestAzureBlobIOIntegration(t *testing.T) {
	suite.Run(t, new(AzureBlobIOTestSuite))
}
