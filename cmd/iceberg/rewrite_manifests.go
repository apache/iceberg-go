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

package main

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
)

func runRewriteManifests(ctx context.Context, output Output, cat catalog.Catalog, cmd *RewriteManifestsCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)

	var opts []table.RewriteManifestsOpt
	if cmd.TargetManifestSize > 0 {
		opts = append(opts, table.WithManifestTargetSize(cmd.TargetManifestSize))
	}
	if cmd.SpecID >= 0 {
		opts = append(opts, table.WithRewriteSpecID(cmd.SpecID))
	}

	tx := tbl.NewTransaction()
	res, err := tx.RewriteManifests(ctx, opts...)
	if err != nil {
		output.Error(fmt.Errorf("rewrite manifests failed: %w", err))
		osExit(1)
	}

	// No-op — nothing was written or staged, so there is no commit to make. The
	// reason distinguishes an empty table from an already-optimal layout.
	if res.IsNoOp() {
		if res.NoOpReason == table.NoOpNoSnapshot {
			output.Text("Table has no current snapshot; nothing to rewrite.")
		} else {
			output.Text("Manifests already optimal; nothing to rewrite.")
		}

		return
	}

	if _, err := tx.Commit(ctx); err != nil {
		output.Error(fmt.Errorf("commit failed: %w", err))
		osExit(1)
	}

	output.Text(fmt.Sprintf("Rewrote %d manifests into %d.",
		len(res.RewrittenManifests), len(res.AddedManifests)))
}
