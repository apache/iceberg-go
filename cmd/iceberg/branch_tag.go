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
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runBranch(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchCmd) {
	switch {
	case cmd.Create != nil:
		runBranchCreate(ctx, output, cat, cmd.Create)
	}
}

func runTag(ctx context.Context, output Output, cat catalog.Catalog, cmd *TagCmd) {
	switch {
	case cmd.Create != nil:
		runTagCreate(ctx, output, cat, cmd.Create)
	}
}

func runBranchCreate(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchCreateCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	meta := tbl.Metadata()

	for name := range meta.Refs() {
		if name == cmd.BranchName {
			output.Error(fmt.Errorf("ref %q already exists", cmd.BranchName))
			os.Exit(1)
		}
	}

	snapshotID := resolveSnapshotID(output, tbl, cmd.SnapshotID)

	if err := confirmAction(
		fmt.Sprintf("Create branch %q on %s at snapshot %d?", cmd.BranchName, tableIDString(tbl), snapshotID),
		cmd.Yes,
	); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	var maxRefAgeMs int64
	if cmd.MaxRefAge != "" {
		d, err := parseDuration(cmd.MaxRefAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-ref-age: %w", err))
			os.Exit(1)
		}

		maxRefAgeMs = d.Milliseconds()
	}

	var maxSnapshotAgeMs int64
	if cmd.MaxSnapshotAge != "" {
		d, err := parseDuration(cmd.MaxSnapshotAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-snapshot-age: %w", err))
			os.Exit(1)
		}

		maxSnapshotAgeMs = d.Milliseconds()
	}

	var minSnapshotsToKeep int
	if cmd.MinSnapshotsToKeep != nil {
		minSnapshotsToKeep = *cmd.MinSnapshotsToKeep
	}

	update := table.NewSetSnapshotRefUpdate(cmd.BranchName, snapshotID, table.BranchRef,
		maxRefAgeMs, maxSnapshotAgeMs, minSnapshotsToKeep)
	reqs := []table.Requirement{
		table.AssertTableUUID(meta.TableUUID()),
		table.AssertRefSnapshotID(cmd.BranchName, nil),
	}

	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update}); err != nil {
		output.Error(fmt.Errorf("failed to create branch: %w", err))
		os.Exit(1)
	}

	result := RefCreatedResult{
		Table:      tableIDString(tbl),
		RefName:    cmd.BranchName,
		RefType:    string(table.BranchRef),
		SnapshotID: snapshotID,
	}
	if maxRefAgeMs > 0 {
		result.MaxRefAgeMs = &maxRefAgeMs
	}
	if maxSnapshotAgeMs > 0 {
		result.MaxSnapshotAgeMs = &maxSnapshotAgeMs
	}
	if minSnapshotsToKeep > 0 {
		result.MinSnapshotsToKeep = &minSnapshotsToKeep
	}

	output.RefCreated(result)
}

func runTagCreate(ctx context.Context, output Output, cat catalog.Catalog, cmd *TagCreateCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	meta := tbl.Metadata()

	for name := range meta.Refs() {
		if name == cmd.TagName {
			output.Error(fmt.Errorf("ref %q already exists", cmd.TagName))
			os.Exit(1)
		}
	}

	snapshotID := resolveSnapshotID(output, tbl, cmd.SnapshotID)

	if err := confirmAction(
		fmt.Sprintf("Create tag %q on %s at snapshot %d?", cmd.TagName, tableIDString(tbl), snapshotID),
		cmd.Yes,
	); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	var maxRefAgeMs int64
	if cmd.MaxRefAge != "" {
		d, err := parseDuration(cmd.MaxRefAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-ref-age: %w", err))
			os.Exit(1)
		}

		maxRefAgeMs = d.Milliseconds()
	}

	update := table.NewSetSnapshotRefUpdate(cmd.TagName, snapshotID, table.TagRef,
		maxRefAgeMs, 0, 0)
	reqs := []table.Requirement{
		table.AssertTableUUID(meta.TableUUID()),
		table.AssertRefSnapshotID(cmd.TagName, nil),
	}

	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update}); err != nil {
		output.Error(fmt.Errorf("failed to create tag: %w", err))
		os.Exit(1)
	}

	result := RefCreatedResult{
		Table:      tableIDString(tbl),
		RefName:    cmd.TagName,
		RefType:    string(table.TagRef),
		SnapshotID: snapshotID,
	}
	if maxRefAgeMs > 0 {
		result.MaxRefAgeMs = &maxRefAgeMs
	}

	output.RefCreated(result)
}

func resolveSnapshotID(output Output, tbl *table.Table, explicit *int64) int64 {
	if explicit != nil {
		if tbl.Metadata().SnapshotByID(*explicit) == nil {
			output.Error(fmt.Errorf("snapshot %d not found", *explicit))
			os.Exit(1)
		}

		return *explicit
	}

	snap := tbl.Metadata().CurrentSnapshot()
	if snap == nil {
		output.Error(errors.New("table has no current snapshot; specify --snapshot-id explicitly"))
		os.Exit(1)
	}

	return snap.SnapshotID
}

func (t textOutput) RefCreated(result RefCreatedResult) {
	pterm.Printfln("Created %s %q on %s at snapshot %d.",
		result.RefType, result.RefName, result.Table, result.SnapshotID)
}

func (j jsonOutput) RefCreated(result RefCreatedResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
