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

package hadoop_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/internal/recipe"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type HadoopMinIOIntegrationSuite struct {
	suite.Suite

	ctx       context.Context
	cat       *hadoop.Catalog
	props     iceberg.Properties
	warehouse string
}

func (s *HadoopMinIOIntegrationSuite) SetupSuite() {
	_, err := recipe.Start(s.T())
	s.Require().NoError(err)
}

func (s *HadoopMinIOIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.warehouse = fmt.Sprintf("s3a://warehouse/hadoop-minio/%s", uuid.NewString())
	s.props = iceberg.Properties{
		"allow-unsafe-commits":      "true",
		icebergio.S3Region:          "local",
		icebergio.S3AccessKeyID:     "admin",
		icebergio.S3SecretAccessKey: "password",
		// Endpoint is provided by `make integration-env` or recipe.Start.
	}

	cat, err := hadoop.NewCatalog("hadoop_minio_test", s.warehouse, s.props)
	s.Require().NoError(err)
	s.cat = cat
}

func (s *HadoopMinIOIntegrationSuite) TearDownTest() {
	fs, err := icebergio.LoadFS(s.ctx, s.props, s.warehouse)
	if err != nil {
		return
	}

	if remover, ok := fs.(icebergio.RemoveAllIO); ok {
		_ = remover.RemoveAll(s.warehouse)
	}
}

func (s *HadoopMinIOIntegrationSuite) TestNamespaceTableRoundTrip() {
	ns := table.Identifier{"minio_ns"}
	ident := table.Identifier{"minio_ns", "tbl"}

	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))

	exists, err := s.cat.CheckNamespaceExists(s.ctx, ns)
	s.Require().NoError(err)
	s.True(exists)

	schema := iceberg.NewSchema(
		1,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
	)

	tbl, err := s.cat.CreateTable(s.ctx, ident, schema)
	s.Require().NoError(err)
	s.NotNil(tbl)
	s.True(strings.HasPrefix(tbl.MetadataLocation(), s.warehouse+"/minio_ns/tbl/metadata/v1.metadata.json"))

	loaded, err := s.cat.LoadTable(s.ctx, ident)
	s.Require().NoError(err)
	s.Equal(tbl.MetadataLocation(), loaded.MetadataLocation())
	s.Equal(2, len(loaded.Schema().Fields()))

	var tables []table.Identifier
	for found, err := range s.cat.ListTables(s.ctx, ns) {
		s.Require().NoError(err)
		tables = append(tables, found)
	}
	s.Equal([]table.Identifier{ident}, tables)

	s.Require().NoError(s.cat.DropTable(s.ctx, ident))

	tableExists, err := s.cat.CheckTableExists(s.ctx, ident)
	s.Require().NoError(err)
	s.False(tableExists)
}

func TestHadoopMinIOIntegration(t *testing.T) {
	suite.Run(t, new(HadoopMinIOIntegrationSuite))
}
