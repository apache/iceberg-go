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

package catalog

import (
	"context"
	"errors"

	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type CatalogType string

const (
	REST     CatalogType = "rest"
	Hive     CatalogType = "hive"
	Glue     CatalogType = "glue"
	DynamoDB CatalogType = "dynamodb"
	SQL      CatalogType = "sql"
)

var (
	// ErrNoSuchTable is returned when a table does not exist in the catalog.
	ErrNoSuchTable = errors.New("table does not exist")
)

// WithAwsConfig sets the AWS configuration for the catalog.
func WithAwsConfig(cfg aws.Config) CatalogOption {
	return func(o *CatalogOptions) {
		o.awsConfig = cfg
	}
}

type CatalogOption func(*CatalogOptions)

type CatalogOptions struct {
	awsConfig aws.Config
}

// Catalog for iceberg table operations like create, drop, load, list and others.
type Catalog interface {
	// ListTables returns a list of table identifiers in the catalog, with the returned
	// identifiers containing the information required to load the table via that catalog.
	ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error)
	// LoadTable loads a table from the catalog and returns a Table with the metadata.
	LoadTable(ctx context.Context, identifier table.Identifier, props map[string]string) (*table.Table, error)
	// CatalogType returns the type of the catalog.
	CatalogType() CatalogType
}
