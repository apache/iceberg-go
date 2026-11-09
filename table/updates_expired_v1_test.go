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
    "context"
    "testing"

    "github.com/stretchr/testify/require"
)

// V1 snapshots store embedded manifests rather than a manifest list. They
// must not cause cleanup to call FileIO.Remove with an empty file path.
func TestRemoveSnapshotsPostCommitSkipsV1MissingManifestList(t *testing.T) {
    const snapshots = `{"snapshot-id":1,"timestamp-ms":1000,"manifests":[]}`
    before, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{snapshots: snapshots}))
    require.NoError(t, err)
    after, err := ParseMetadataString(buildMetaJSON(metaJSONOpts{}))
    require.NoError(t, err)

    fs := newTrackingCallsIO()
    fs.files[""] = []byte("sentinel")
    beforeTable := New(Identifier{"ns", "tbl"}, before, "metadata.json", testFSF(fs), nil)
    afterTable := New(Identifier{"ns", "tbl"}, after, "metadata.json", testFSF(fs), nil)

    update := NewRemoveSnapshotsUpdate([]int64{1}, true)
    require.NoError(t, update.PostCommit(context.Background(), beforeTable, afterTable))
    require.Zero(t, fs.removeCount[""], "must not attempt to remove an empty path")
    require.Contains(t, fs.files, "", "the empty-path sentinel must not be deleted")
}
