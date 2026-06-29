// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// //go:build integration

package hadoop_test

// import (
// 	"context"
// 	"strings"
// 	"testing"

// 	"github.com/apache/iceberg-go"
// 	"github.com/apache/iceberg-go/catalog"
// 	"github.com/apache/iceberg-go/catalog/hadoop"
// 	"github.com/apache/iceberg-go/io"
// 	_ "github.com/apache/iceberg-go/io/gocloud"
// 	"github.com/apache/iceberg-go/table"
// 	"github.com/stretchr/testify/suite"
// )

// type HadoopMinioIntegrationSuite struct {
// 	suite.Suite

// 	ctx       context.Context
// 	cat       *hadoop.Catalog
// 	warehouse string
// }

// func (s *HadoopMinioIntegrationSuite) SetupTest() {
// 	s.ctx = context.Background()
// 	s.warehouse = "s3a://warehouse/iceberg/hadoop/" + strings.ReplaceAll(s.T().Name(), "/", "_")

// 	cat, err := hadoop.NewCatalog("hadoop_test", s.warehouse, iceberg.Properties{
// 		"allow-unsafe-commits": "true",
// 		io.S3Region:            "local",
// 		io.S3AccessKeyID:       "admin",
// 		io.S3SecretAccessKey:   "password",
// 		// endpoint is passed via AWS_S3_ENDPOINT env var by recipe.Start.
// 	})
// 	s.Require().NoError(err)
// 	s.cat = cat
// }

// func (s *HadoopMinioIntegrationSuite) TestNamespaceLifecycle() {
// 	ns := table.Identifier{"minio_ns"}

// 	exists, err := s.cat.CheckNamespaceExists(s.ctx, ns)
// 	s.Require().NoError(err)
// 	s.False(exists)

// 	err = s.cat.CreateNamespace(s.ctx, ns, iceberg.Properties{})
// 	s.Require().NoError(err)

// 	exists, err = s.cat.CheckNamespaceExists(s.ctx, ns)
// 	s.Require().NoError(err)
// 	s.True(exists)

// 	namespaces, err := s.cat.ListNamespaces(s.ctx, nil)
// 	s.Require().NoError(err)
// 	s.Contains(namespaces, ns)

// 	s.Require().NoError(s.cat.DropNamespace(s.ctx, ns))

// 	exists, err = s.cat.CheckNamespaceExists(s.ctx, ns)
// 	s.Require().NoError(err)
// 	s.False(exists)
// }

// func (s *HadoopMinioIntegrationSuite) TestTableLifecycle() {
// 	ns := table.Identifier{"table_ns"}
// 	ident := table.Identifier{"table_ns", "tbl"}
// 	schema := iceberg.NewSchema(
// 		1,
// 		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true},
// 		iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String},
// 	)

// 	s.Require().NoError(s.cat.CreateNamespace(s.ctx, ns, nil))

// 	tbl, err := s.cat.CreateTable(s.ctx, ident, schema)
// 	s.Require().NoError(err)
// 	s.NotNil(tbl)
// 	s.Equal(s.warehouse+"/table_ns/tbl", tbl.Location())

// 	exists, err := s.cat.CheckTableExists(s.ctx, ident)
// 	s.Require().NoError(err)
// 	s.True(exists)

// 	loaded, err := s.cat.LoadTable(s.ctx, ident)
// 	s.Require().NoError(err)
// 	s.Equal(tbl.MetadataLocation(), loaded.MetadataLocation())
// 	s.Equal([]string{"id", "data"}, []string{
// 		loaded.Schema().Fields()[0].Name,
// 		loaded.Schema().Fields()[1].Name,
// 	})

// 	var tables []table.Identifier
// 	for tableIdent, err := range s.cat.ListTables(s.ctx, ns) {
// 		s.Require().NoError(err)
// 		tables = append(tables, tableIdent)
// 	}
// 	s.Equal([]table.Identifier{ident}, tables)

// 	// s.Require().NoError(s.cat.DropTable(s.ctx, ident))

// 	// exists, err = s.cat.CheckTableExists(s.ctx, ident)
// 	// s.Require().NoError(err)
// 	// s.False(exists)
// }

// func (s *HadoopMinioIntegrationSuite) TestMissingObjects() {
// 	for _, err := range s.cat.ListTables(s.ctx, table.Identifier{"missing_ns"}) {
// 		s.ErrorIs(err, catalog.ErrNoSuchNamespace)

// 		break
// 	}

// 	_, err := s.cat.LoadTable(s.ctx, table.Identifier{"missing_ns", "missing_table"})
// 	s.ErrorIs(err, catalog.ErrNoSuchTable)

// 	err = s.cat.DropTable(s.ctx, table.Identifier{"missing_ns", "missing_table"})
// 	s.ErrorIs(err, catalog.ErrNoSuchTable)
// }

// func (s *HadoopMinioIntegrationSuite) TestRenameTableUnsupported() {
// 	_, err := s.cat.RenameTable(s.ctx, table.Identifier{"ns", "old"}, table.Identifier{"ns", "new"})
// 	s.Require().Error(err)
// 	s.Contains(err.Error(), "not supported")
// }

// func TestHadoopIntegrationWithMinio(t *testing.T) {
// 	suite.Run(t, new(HadoopMinioIntegrationSuite))
// }
//
