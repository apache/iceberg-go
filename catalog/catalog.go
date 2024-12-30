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
	"crypto/tls"
	"errors"
	"net/url"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
)

type CatalogType string

type AwsProperties map[string]string

const (
	REST     CatalogType = "rest"
	Hive     CatalogType = "hive"
	Glue     CatalogType = "glue"
	DynamoDB CatalogType = "dynamodb"
	SQL      CatalogType = "sql"
)

var (
	// ErrNoSuchTable is returned when a table does not exist in the catalog.
	ErrNoSuchTable            = errors.New("table does not exist")
	ErrNoSuchNamespace        = errors.New("namespace does not exist")
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrTableAlreadyExists     = errors.New("table already exists")
)

// WithAwsConfig sets the AWS configuration for the catalog.
func WithAwsConfig(cfg aws.Config) Option[GlueCatalog] {
	return func(o *options) {
		o.awsConfig = cfg
	}
}

func WithAwsProperties(props AwsProperties) Option[GlueCatalog] {
	return func(o *options) {
		o.awsProperties = props
	}
}

func WithCredential(cred string) Option[RestCatalog] {
	return func(o *options) {
		o.credential = cred
	}
}

func WithOAuthToken(token string) Option[RestCatalog] {
	return func(o *options) {
		o.oauthToken = token
	}
}

func WithTLSConfig(config *tls.Config) Option[RestCatalog] {
	return func(o *options) {
		o.tlsConfig = config
	}
}

func WithWarehouseLocation(loc string) Option[RestCatalog] {
	return func(o *options) {
		o.warehouseLocation = loc
	}
}

func WithMetadataLocation(loc string) Option[RestCatalog] {
	return func(o *options) {
		o.metadataLocation = loc
	}
}

func WithSigV4() Option[RestCatalog] {
	return func(o *options) {
		o.enableSigv4 = true
		o.sigv4Service = "execute-api"
	}
}

func WithSigV4RegionSvc(region, service string) Option[RestCatalog] {
	return func(o *options) {
		o.enableSigv4 = true
		o.sigv4Region = region

		if service == "" {
			o.sigv4Service = "execute-api"
		} else {
			o.sigv4Service = service
		}
	}
}

func WithAuthURI(uri *url.URL) Option[RestCatalog] {
	return func(o *options) {
		o.authUri = uri
	}
}

func WithPrefix(prefix string) Option[RestCatalog] {
	return func(o *options) {
		o.prefix = prefix
	}
}

type Option[T GlueCatalog | RestCatalog] func(*options)

type options struct {
	awsConfig     aws.Config
	awsProperties AwsProperties

	tlsConfig         *tls.Config
	credential        string
	oauthToken        string
	warehouseLocation string
	metadataLocation  string
	enableSigv4       bool
	sigv4Region       string
	sigv4Service      string
	prefix            string
	authUri           *url.URL
}

type PropertiesUpdateSummary struct {
	Removed []string `json:"removed"`
	Updated []string `json:"updated"`
	Missing []string `json:"missing"`
}

// Catalog for iceberg table operations like create, drop, load, list and others.
type Catalog interface {
	// CatalogType returns the type of the catalog.
	CatalogType() CatalogType

	// ListTables returns a list of table identifiers in the catalog, with the returned
	// identifiers containing the information required to load the table via that catalog.
	ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error)
	// LoadTable loads a table from the catalog and returns a Table with the metadata.
	LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error)
	// DropTable tells the catalog to drop the table entirely
	DropTable(ctx context.Context, identifier table.Identifier) error
	// RenameTable tells the catalog to rename a given table by the identifiers
	// provided, and then loads and returns the destination table
	RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error)
	// ListNamespaces returns the list of available namespaces, optionally filtering by a
	// parent namespace
	ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error)
	// CreateNamespace tells the catalog to create a new namespace with the given properties
	CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error
	// DropNamespace tells the catalog to drop the namespace and all tables in that namespace
	DropNamespace(ctx context.Context, namespace table.Identifier) error
	// LoadNamespaceProperties returns the current properties in the catalog for
	// a given namespace
	LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error)
	// UpdateNamespaceProperties allows removing, adding, and/or updating properties of a namespace
	UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier,
		removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error)
}

const (
	keyOauthToken        = "token"
	keyWarehouseLocation = "warehouse"
	keyMetadataLocation  = "metadata_location"
	keyOauthCredential   = "credential"
)

func TableNameFromIdent(ident table.Identifier) string {
	if len(ident) == 0 {
		return ""
	}

	return ident[len(ident)-1]
}

func NamespaceFromIdent(ident table.Identifier) table.Identifier {
	return ident[:len(ident)-1]
}
