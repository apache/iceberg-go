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

package hive

import (
	"github.com/apache/iceberg-go"
)

// Configuration property keys for the Hive catalog.
const (
	// URI is the Thrift URI for the Hive Metastore (e.g., "thrift://localhost:9083")
	URI = "uri"

	// Warehouse is the default warehouse location for tables
	Warehouse = "warehouse"

	// KerberosAuth enables Kerberos authentication
	KerberosAuth = "hive.kerberos-authentication"

	TableTypeKey                = "table_type"
	TableTypeIceberg            = "ICEBERG"
	TableTypeExternalTable      = "EXTERNAL_TABLE"
	MetadataLocationKey         = "metadata_location"
	PreviousMetadataLocationKey = "previous_metadata_location"
	ExternalKey                 = "EXTERNAL"
)

// HiveOptions contains configuration options for the Hive Metastore catalog.
type HiveOptions struct {
	URI          string
	Warehouse    string
	KerberosAuth bool
	props        iceberg.Properties
}

// NewHiveOptions creates a new HiveOptions with default values.
func NewHiveOptions() *HiveOptions {
	return &HiveOptions{
		props: iceberg.Properties{},
	}
}

// ApplyProperties applies properties from an iceberg.Properties map.
func (o *HiveOptions) ApplyProperties(props iceberg.Properties) {
	o.props = props

	if uri, ok := props[URI]; ok {
		o.URI = uri
	}
	if warehouse, ok := props[Warehouse]; ok {
		o.Warehouse = warehouse
	}
	if props.GetBool(KerberosAuth, false) {
		o.KerberosAuth = true
	}
}

// Option is a functional option for configuring the Hive catalog.
type Option func(*HiveOptions)

// WithURI sets the Thrift URI for the Hive Metastore.
func WithURI(uri string) Option {
	return func(o *HiveOptions) {
		o.URI = uri
	}
}

// WithWarehouse sets the default warehouse location.
func WithWarehouse(warehouse string) Option {
	return func(o *HiveOptions) {
		o.Warehouse = warehouse
	}
}

// WithKerberosAuth enables Kerberos authentication.
func WithKerberosAuth(enabled bool) Option {
	return func(o *HiveOptions) {
		o.KerberosAuth = enabled
	}
}

// WithProperties sets additional properties for the catalog.
func WithProperties(props iceberg.Properties) Option {
	return func(o *HiveOptions) {
		o.props = props
	}
}
