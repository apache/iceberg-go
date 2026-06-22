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

package table

import (
	"testing"

	"github.com/apache/iceberg-go/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableMetricsReporterDefaultsToNop(t *testing.T) {
	tbl := New(nil, nil, "", nil, nil)
	assert.IsType(t, metrics.NopReporter{}, tbl.MetricsReporter())
}

func TestWithMetricsReporter(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(rep))
	assert.Same(t, rep, tbl.MetricsReporter())

	// A nil reporter option is ignored; the default no-op reporter is kept.
	tblNil := New(nil, nil, "", nil, nil, WithMetricsReporter(nil))
	assert.IsType(t, metrics.NopReporter{}, tblNil.MetricsReporter())
}

func TestScanInheritsReporterFromTable(t *testing.T) {
	rep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(rep))

	scan := tbl.Scan()
	assert.Same(t, rep, scan.Reporter())
}

func TestScanReporterDefaultsToNop(t *testing.T) {
	tbl := New(nil, nil, "", nil, nil)
	assert.IsType(t, metrics.NopReporter{}, tbl.Scan().Reporter())
}

func TestWithReporterOverridesScan(t *testing.T) {
	tableRep := &metrics.InMemoryReporter{}
	scanRep := &metrics.InMemoryReporter{}
	tbl := New(nil, nil, "", nil, nil, WithMetricsReporter(tableRep))

	// Scan-level WithReporter wins over the table's reporter.
	scan := tbl.Scan(WithReporter(scanRep))
	assert.Same(t, scanRep, scan.Reporter())
	require.NotSame(t, tableRep, scan.Reporter())

	// A nil WithReporter is ignored; the inherited reporter is kept.
	scanNil := tbl.Scan(WithReporter(nil))
	assert.Same(t, tableRep, scanNil.Reporter())
}
