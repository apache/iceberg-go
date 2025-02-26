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

package rest_test

import (
	"context"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/suite"
)

type RestIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	cat *rest.Catalog
}

const TestNamespaceIdent = "TEST NS"

func (s *RestIntegrationSuite) loadCatalog(ctx context.Context) *rest.Catalog {
	cat, err := catalog.Load(ctx, "local", iceberg.Properties{
		"type":               "rest",
		"uri":                "http://localhost:8181",
		io.S3AccessKeyID:     "admin",
		io.S3SecretAccessKey: "password",
	})
	s.Require().NoError(err)
	s.Require().IsType(&rest.Catalog{}, cat)
	return cat.(*rest.Catalog)
}

func (s *RestIntegrationSuite) SetupTest() {
	s.ctx = context.Background()
	s.cat = s.loadCatalog(s.ctx)
}

var (
	tableSchemaNested = iceberg.NewSchemaWithIdentifiers(1,
		[]int{1},
		iceberg.NestedField{
			ID: 1, Name: "foo", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{
			ID: 2, Name: "bar", Type: iceberg.PrimitiveTypes.Int32, Required: true},
		iceberg.NestedField{
			ID: 3, Name: "baz", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &iceberg.ListType{
				ElementID: 5, Element: iceberg.PrimitiveTypes.String, ElementRequired: true}},
		iceberg.NestedField{
			ID: 6, Name: "quux",
			Type: &iceberg.MapType{
				KeyID:   7,
				KeyType: iceberg.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &iceberg.MapType{
					KeyID:         9,
					KeyType:       iceberg.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     iceberg.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
			Required: true},
		iceberg.NestedField{
			ID: 11, Name: "location", Type: &iceberg.ListType{
				ElementID: 12, Element: &iceberg.StructType{
					FieldList: []iceberg.NestedField{
						{ID: 13, Name: "latitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
						{ID: 14, Name: "longitude", Type: iceberg.PrimitiveTypes.Float32, Required: false},
					},
				},
				ElementRequired: true},
			Required: true},
		iceberg.NestedField{
			ID:   15,
			Name: "person",
			Type: &iceberg.StructType{
				FieldList: []iceberg.NestedField{
					{ID: 16, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
					{ID: 17, Name: "age", Type: iceberg.PrimitiveTypes.Int32, Required: true},
				},
			},
			Required: false,
		},
	)
)

func (s *RestIntegrationSuite) ensureNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		iceberg.Properties{"foo": "bar", "prop": "yes"}))
}

func (s *RestIntegrationSuite) TestCreateNamespace() {
	exists, err := s.cat.CheckNamespaceExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	if exists {
		s.Require().NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
	}

	s.NoError(s.cat.CreateNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent), nil))
	s.NoError(s.cat.DropNamespace(s.ctx, catalog.ToIdentifier(TestNamespaceIdent)))
}

func (s *RestIntegrationSuite) TestLoadNamespaceProps() {
	s.ensureNamespace()

	props, err := s.cat.LoadNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent))
	s.Require().NoError(err)
	s.Require().NotNil(props)
	s.Equal("bar", props["foo"])
	s.Equal("yes", props["prop"])
}

func (s *RestIntegrationSuite) TestUpdateNamespaceProps() {
	s.ensureNamespace()

	summary, err := s.cat.UpdateNamespaceProperties(s.ctx, catalog.ToIdentifier(TestNamespaceIdent),
		[]string{"abc"}, iceberg.Properties{"prop": "no"})
	s.Require().NoError(err)
	s.Equal(catalog.PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{"prop"},
		Missing: []string{"abc"},
	}, summary)
}

func (s *RestIntegrationSuite) TestCreateTable() {
	s.ensureNamespace()

	const location = "s3://warehouse/iceberg"

	tbl, err := s.cat.CreateTable(s.ctx,
		catalog.ToIdentifier(TestNamespaceIdent, "test-table"),
		tableSchemaSimple, catalog.WithProperties(iceberg.Properties{"foobar": "baz"}),
		catalog.WithLocation(location))
	s.Require().NoError(err)
	s.Require().NotNil(tbl)

	s.Equal(location, tbl.Location())
	s.Equal("baz", tbl.Properties()["foobar"])

	exists, err := s.cat.CheckTableExists(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table"))
	s.Require().NoError(err)
	s.True(exists)

	s.Require().NoError(s.cat.DropTable(s.ctx, catalog.ToIdentifier(TestNamespaceIdent, "test-table")))
}

func TestRestIntegration(t *testing.T) {
	suite.Run(t, new(RestIntegrationSuite))
}
