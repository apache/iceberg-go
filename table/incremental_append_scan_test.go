// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

package table

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncrementalAppendScanSnapshotBoundaries(t *testing.T) {
	scan := snapshotsTestTable().NewIncrementalAppendScan()
	inclusive, err := scan.FromSnapshotInclusive(101)
	require.NoError(t, err)
	inclusive, err = inclusive.ToSnapshot(102)
	require.NoError(t, err)

	snapshots, err := inclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	require.EqualValues(t, 101, snapshots[0].SnapshotID)

	exclusive := scan.FromSnapshotExclusive(101)
	exclusive, err = exclusive.ToSnapshot(102)
	require.NoError(t, err)
	snapshots, err = exclusive.snapshotsBetween(102)
	require.NoError(t, err)
	require.Empty(t, snapshots, "the only snapshot after 101 is not an append")
}

func TestIncrementalAppendScanRejectsUnknownStart(t *testing.T) {
	_, err := snapshotsTestTable().NewIncrementalAppendScan().FromSnapshotInclusive(999)
	require.Error(t, err)
}
