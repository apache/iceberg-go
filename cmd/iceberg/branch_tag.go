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
	"strings"
	"unicode"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runBranch(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchCmd) {
	switch {
	case cmd.Create != nil:
		runBranchCreate(ctx, output, cat, cmd.Create)
	case cmd.Delete != nil:
		runBranchDelete(ctx, output, cat, cmd.Delete)
	}
}

func runTag(ctx context.Context, output Output, cat catalog.Catalog, cmd *TagCmd) {
	switch {
	case cmd.Create != nil:
		runTagCreate(ctx, output, cat, cmd.Create)
	case cmd.Delete != nil:
		runTagDelete(ctx, output, cat, cmd.Delete)
	}
}

func runBranchCreate(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchCreateCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	meta := tbl.Metadata()
	if err := validateRefName(cmd.BranchName); err != nil {
		output.Error(fmt.Errorf("invalid branch name: %w", err))
		osExit(1)

		return
	}

	if _, found := findSnapshotRef(meta, cmd.BranchName); found {
		output.Error(fmt.Errorf("ref %q already exists", cmd.BranchName))
		osExit(1)

		return
	}

	snapshotID := resolveSnapshotID(output, tbl, cmd.SnapshotID)

	if err := confirmAction(
		fmt.Sprintf("Create branch %q on %s at snapshot %d?", cmd.BranchName, tableIDString(tbl), snapshotID),
		cmd.Yes,
	); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	var maxRefAgeMs int64
	if cmd.MaxRefAge != "" {
		d, err := parseDuration(cmd.MaxRefAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-ref-age: %w", err))
			osExit(1)

			return
		}

		if d <= 0 {
			output.Error(errors.New("invalid --max-ref-age: must be greater than 0"))
			osExit(1)

			return
		}

		maxRefAgeMs = d.Milliseconds()
	}

	var maxSnapshotAgeMs int64
	if cmd.MaxSnapshotAge != "" {
		d, err := parseDuration(cmd.MaxSnapshotAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-snapshot-age: %w", err))
			osExit(1)

			return
		}

		if d <= 0 {
			output.Error(errors.New("invalid --max-snapshot-age: must be greater than 0"))
			osExit(1)

			return
		}

		maxSnapshotAgeMs = d.Milliseconds()
	}

	var minSnapshotsToKeep int
	if cmd.MinSnapshotsToKeep != nil {
		if *cmd.MinSnapshotsToKeep <= 0 {
			output.Error(errors.New("invalid --min-snapshots-to-keep: must be greater than 0"))
			osExit(1)

			return
		}

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
		osExit(1)

		return
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
	if err := validateRefName(cmd.TagName); err != nil {
		output.Error(fmt.Errorf("invalid tag name: %w", err))
		osExit(1)

		return
	}

	if _, found := findSnapshotRef(meta, cmd.TagName); found {
		output.Error(fmt.Errorf("ref %q already exists", cmd.TagName))
		osExit(1)

		return
	}

	snapshotID := resolveSnapshotID(output, tbl, cmd.SnapshotID)

	if err := confirmAction(
		fmt.Sprintf("Create tag %q on %s at snapshot %d?", cmd.TagName, tableIDString(tbl), snapshotID),
		cmd.Yes,
	); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	var maxRefAgeMs int64
	if cmd.MaxRefAge != "" {
		d, err := parseDuration(cmd.MaxRefAge)
		if err != nil {
			output.Error(fmt.Errorf("invalid --max-ref-age: %w", err))
			osExit(1)

			return
		}

		if d <= 0 {
			output.Error(errors.New("invalid --max-ref-age: must be greater than 0"))
			osExit(1)

			return
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
		osExit(1)

		return
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

func validateRefName(name string) error {
	if strings.TrimSpace(name) != name || name == "" {
		return errors.New("name must be non-empty and may not contain leading/trailing whitespace")
	}

	if name == "." || name == ".." {
		return errors.New("name may not be '.' or '..'")
	}

	if strings.ContainsAny(name, "/\\") {
		return errors.New("name may not contain path separators")
	}

	if strings.IndexFunc(name, func(r rune) bool { return unicode.IsControl(r) }) >= 0 {
		return errors.New("name may not contain control characters")
	}

	return nil
}

func runBranchDelete(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchDeleteCmd) {
	if cmd.BranchName == table.MainBranch {
		output.Error(errors.New(`cannot delete the "main" branch`))
		osExit(1)

		return
	}

	runRefDelete(ctx, output, cat, cmd.TableID, cmd.BranchName, table.BranchRef, cmd.Yes)
}

func runTagDelete(ctx context.Context, output Output, cat catalog.Catalog, cmd *TagDeleteCmd) {
	runRefDelete(ctx, output, cat, cmd.TableID, cmd.TagName, table.TagRef, cmd.Yes)
}

func runRefDelete(ctx context.Context, output Output, cat catalog.Catalog, tableID, refName string, refType table.RefType, yes bool) {
	tbl := loadTable(ctx, output, cat, tableID)
	meta := tbl.Metadata()

	ref, found := findSnapshotRef(meta, refName)
	if !found {
		output.Error(fmt.Errorf("%s %q does not exist", refType, refName))
		osExit(1)

		return
	}

	if ref.SnapshotRefType != refType {
		output.Error(fmt.Errorf("ref %q is a %s, not a %s", refName, ref.SnapshotRefType, refType))
		osExit(1)

		return
	}

	if err := confirmAction(
		fmt.Sprintf("Delete %s %q from %s at snapshot %d?", refType, refName, tableIDString(tbl), ref.SnapshotID),
		yes,
	); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	update := table.NewRemoveSnapshotRefUpdate(refName)
	// Pin the ref to the snapshot we observed so the delete only succeeds while
	// the ref is unchanged: any concurrent head change, including a move, delete,
	// or append that advances it, trips the requirement, and the user re-runs
	// against the latest state. This is safe-by-default for a destructive
	// operation and matches the create path's optimistic-concurrency style.
	snapshotID := ref.SnapshotID
	reqs := []table.Requirement{
		table.AssertTableUUID(meta.TableUUID()),
		table.AssertRefSnapshotID(refName, &snapshotID),
	}

	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update}); err != nil {
		output.Error(fmt.Errorf("failed to delete %s: %w", refType, err))
		osExit(1)

		return
	}

	output.RefDeleted(RefDeletedResult{
		Table:      tableIDString(tbl),
		RefName:    refName,
		RefType:    string(refType),
		SnapshotID: snapshotID,
	})
}

func findSnapshotRef(meta table.Metadata, name string) (table.SnapshotRef, bool) {
	for refName, ref := range meta.Refs() {
		if refName == name {
			return ref, true
		}
	}

	return table.SnapshotRef{}, false
}

func resolveSnapshotID(output Output, tbl *table.Table, explicit *int64) int64 {
	if explicit != nil {
		if tbl.Metadata().SnapshotByID(*explicit) == nil {
			output.Error(fmt.Errorf("snapshot %d not found", *explicit))
			osExit(1)

			return 0
		}

		return *explicit
	}

	snap := tbl.Metadata().CurrentSnapshot()
	if snap == nil {
		output.Error(errors.New("table has no current snapshot; specify --snapshot-id explicitly"))
		osExit(1)

		return 0
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

func (t textOutput) RefDeleted(result RefDeletedResult) {
	pterm.Printfln("Deleted %s %q from %s at snapshot %d.",
		result.RefType, result.RefName, result.Table, result.SnapshotID)
}

func (j jsonOutput) RefDeleted(result RefDeletedResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
