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

package sql

import (
	"context"
	"testing"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cancelAwareRemoveIO records removals and, like blobfs, rejects Remove on a
// canceled context.
type cancelAwareRemoveIO struct {
	iceio.IO
	removed []string
}

func (c *cancelAwareRemoveIO) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.removed = append(c.removed, name)

	return nil
}

func TestRemoveUncommittedMetadataIgnoresCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	fs := &cancelAwareRemoveIO{}
	loadFS := func(ctx context.Context) (iceio.IO, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		return fs, nil
	}

	// A commit that failed because ctx was canceled must still remove the
	// metadata file it wrote.
	const location = "s3://bucket/db/tbl/metadata/00001-uncommitted.metadata.json"
	require.NoError(t, removeUncommittedMetadata(ctx, location, loadFS))
	assert.Equal(t, []string{location}, fs.removed)
}
