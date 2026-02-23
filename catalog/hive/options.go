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
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
)

const (
	// URI is the Thrift URI for the Hive Metastore (e.g., "thrift://localhost:9083")
	URI = "uri"

	// Warehouse is the default warehouse location for tables
	Warehouse = "warehouse"

	TableTypeKey           = "table_type"
	TableTypeIceberg       = "ICEBERG"
	TableTypeExternalTable = "EXTERNAL_TABLE"
	// Ref: https://github.com/apache/hive/blob/7060d94843fdbc548445db6aac84dd60b44641ee/standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/TableType.java#L27
	TableTypeVirtualView = "VIRTUAL_VIEW"
	// Ref: https://github.com/apache/iceberg/blob/2f170322d425a4c6267a9033efa2107c9bfc53db/hive-metastore/src/main/java/org/apache/iceberg/hive/HiveOperationsBase.java#L57
	TableTypeIcebergView        = "ICEBERG_VIEW"
	MetadataLocationKey         = "metadata_location"
	PreviousMetadataLocationKey = "previous_metadata_location"
	ExternalKey                 = "EXTERNAL"

	// Lock configuration property keys
	LockCheckMinWaitTime = "lock-check-min-wait-time"
	LockCheckMaxWaitTime = "lock-check-max-wait-time"
	LockCheckRetries     = "lock-check-retries"

	// Default lock configuration values
	DefaultLockCheckMinWaitTime = 100 * time.Millisecond // 100ms
	DefaultLockCheckMaxWaitTime = 60 * time.Second       // 1 minute
	DefaultLockCheckRetries     = 4
)

type HiveOptions struct {
	URI       string
	Warehouse string
	props     iceberg.Properties

	// Lock configuration for atomic commits
	LockMinWaitTime time.Duration
	LockMaxWaitTime time.Duration
	LockRetries     int
}

func NewHiveOptions() *HiveOptions {
	return &HiveOptions{
		props:           iceberg.Properties{},
		LockMinWaitTime: DefaultLockCheckMinWaitTime,
		LockMaxWaitTime: DefaultLockCheckMaxWaitTime,
		LockRetries:     DefaultLockCheckRetries,
	}
}

func (o *HiveOptions) ApplyProperties(props iceberg.Properties) {
	o.props = props

	if uri, ok := props[URI]; ok {
		o.URI = uri
	}
	if warehouse, ok := props[Warehouse]; ok {
		o.Warehouse = warehouse
	}

	// Parse lock configuration
	if val, ok := props[LockCheckMinWaitTime]; ok {
		if d, err := time.ParseDuration(val); err == nil {
			o.LockMinWaitTime = d
		}
	}
	if val, ok := props[LockCheckMaxWaitTime]; ok {
		if d, err := time.ParseDuration(val); err == nil {
			o.LockMaxWaitTime = d
		}
	}
	if val, ok := props[LockCheckRetries]; ok {
		if i, err := strconv.Atoi(val); err == nil {
			o.LockRetries = i
		}
	}
}

type Option func(*HiveOptions)

// WithURI sets the Thrift URI for the Hive Metastore.
func WithURI(uri string) Option {
	return func(o *HiveOptions) {
		o.URI = uri
	}
}

func WithWarehouse(warehouse string) Option {
	return func(o *HiveOptions) {
		o.Warehouse = warehouse
	}
}

func WithProperties(props iceberg.Properties) Option {
	return func(o *HiveOptions) {
		o.props = props
	}
}
