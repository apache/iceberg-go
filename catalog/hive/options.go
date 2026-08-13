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

	"github.com/DataDog/iceberg-go"
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
	StorageHandlerKey           = "storage_handler"
	IcebergStorageHandler       = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler"
	GCEnabledKey                = "gc.enabled"
	ExternalTablePurgeKey       = "external.table.purge"

	// Lock configuration property keys.
	//
	// Primary names match the Java Hive catalog properties (integer milliseconds).
	// Legacy Go keys remain accepted as aliases so existing configs keep working;
	// when both forms are set, the Java key wins.
	// Ref: https://iceberg.apache.org/docs/nightly/catalog-properties/#hive-metastore-configuration
	LockCheckMinWaitMs = "iceberg.hive.lock-check-min-wait-ms"
	LockCheckMaxWaitMs = "iceberg.hive.lock-check-max-wait-ms"

	// Legacy Go-specific aliases (Go duration strings, e.g. "100ms", "5s").
	LockCheckMinWaitTime = "lock-check-min-wait-time"
	LockCheckMaxWaitTime = "lock-check-max-wait-time"
	// LockCheckRetries is Go-specific. Java bounds acquisition with
	// iceberg.hive.lock-timeout-ms instead of a retry count.
	LockCheckRetries = "lock-check-retries"

	// Default lock configuration values match the Java Hive catalog.
	DefaultLockCheckMinWaitTime = 50 * time.Millisecond // Java: 50ms
	DefaultLockCheckMaxWaitTime = 5 * time.Second       // Java: 5000ms
	DefaultLockCheckRetries     = 4

	// lockCheckBackoffScale matches the Java MetastoreLock lock-check path, which
	// passes 1.5 to Tasks.exponentialBackoff (not the Tasks default of 2.0).
	lockCheckBackoffScale = 1.5
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

	minWait := o.LockMinWaitTime
	maxWait := o.LockMaxWaitTime

	if d, ok := durationFromProps(props, LockCheckMinWaitMs, LockCheckMinWaitTime); ok {
		minWait = d
	}
	if d, ok := durationFromProps(props, LockCheckMaxWaitMs, LockCheckMaxWaitTime); ok {
		maxWait = d
	}

	// Only apply a consistent positive wait window. Invalid values (non-positive
	// or min >= max) are ignored so callers keep the previous/default settings
	// instead of relying on calculateBackoff/applyJitter to paper over bad config.
	if minWait > 0 && maxWait > 0 && minWait < maxWait {
		o.LockMinWaitTime = minWait
		o.LockMaxWaitTime = maxWait
	}

	if val, ok := props[LockCheckRetries]; ok {
		if i, err := strconv.Atoi(val); err == nil && i > 0 {
			o.LockRetries = i
		}
	}
}

// durationFromProps resolves a lock wait duration from properties. The Java
// millisecond key is preferred when present and valid; otherwise the legacy Go
// duration-string alias is used.
func durationFromProps(props iceberg.Properties, javaMsKey, legacyDurationKey string) (time.Duration, bool) {
	if val, ok := props[javaMsKey]; ok {
		if ms, err := strconv.ParseInt(val, 10, 64); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond, true
		}
	}
	if val, ok := props[legacyDurationKey]; ok {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			return d, true
		}
	}

	return 0, false
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
