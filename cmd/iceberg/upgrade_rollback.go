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
	"fmt"
	"os"
	"strconv"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runUpgrade(ctx context.Context, output Output, cat catalog.Catalog, cmd *UpgradeCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	meta := tbl.Metadata()
	currentVersion := meta.Version()

	if cmd.FormatVersion <= currentVersion {
		output.Error(fmt.Errorf("target format version %d must be greater than current version %d",
			cmd.FormatVersion, currentVersion))
		os.Exit(1)
	}

	result := UpgradeResult{
		DryRun:          cmd.DryRun,
		Table:           tableIDString(tbl),
		PreviousVersion: currentVersion,
		TargetVersion:   cmd.FormatVersion,
		SpecURL:         specURL(cmd.FormatVersion),
	}

	if cmd.DryRun {
		output.UpgradeResult(result)

		return
	}

	prompt := fmt.Sprintf("Upgrade %s from format version %d to %d?",
		tableIDString(tbl), currentVersion, cmd.FormatVersion)
	if err := confirmAction(prompt, cmd.Yes); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	tx := tbl.NewTransaction()
	if err := tx.UpgradeFormatVersion(cmd.FormatVersion); err != nil {
		output.Error(fmt.Errorf("upgrade failed: %w", err))
		os.Exit(1)
	}

	if _, err := tx.Commit(ctx); err != nil {
		output.Error(fmt.Errorf("commit failed: %w", err))
		os.Exit(1)
	}

	output.UpgradeResult(result)
}

func runRollback(ctx context.Context, output Output, cat catalog.Catalog, cmd *RollbackCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	meta := tbl.Metadata()

	snap := meta.SnapshotByID(cmd.SnapshotID)
	if snap == nil {
		output.Error(fmt.Errorf("snapshot %d not found in table %s", cmd.SnapshotID, tableIDString(tbl)))
		os.Exit(1)
	}

	var previousSnapshotID *int64
	if cs := meta.CurrentSnapshot(); cs != nil {
		id := cs.SnapshotID
		previousSnapshotID = &id
	}

	prompt := fmt.Sprintf("Roll back %s to snapshot %d?", tableIDString(tbl), cmd.SnapshotID)
	if err := confirmAction(prompt, cmd.Yes); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	update := table.NewSetSnapshotRefUpdate(table.MainBranch, cmd.SnapshotID, table.BranchRef, 0, 0, 0)
	reqs := []table.Requirement{table.AssertTableUUID(meta.TableUUID())}

	if _, _, err := cat.CommitTable(ctx, tbl.Identifier(), reqs, []table.Update{update}); err != nil {
		output.Error(fmt.Errorf("rollback failed: %w", err))
		os.Exit(1)
	}

	result := RollbackResult{
		Table:                  tableIDString(tbl),
		PreviousSnapshotID:     previousSnapshotID,
		RolledBackToSnapshotID: cmd.SnapshotID,
	}

	output.RollbackResult(result)
}

func specURL(version int) string {
	switch version {
	case 1:
		return "https://iceberg.apache.org/spec/#version-1-analytic-data-tables"
	case 2:
		return "https://iceberg.apache.org/spec/#version-2-row-level-deletes"
	case 3:
		return "https://iceberg.apache.org/spec/#version-3-extended-types-and-features"
	default:
		return "https://iceberg.apache.org/spec/"
	}
}

func (t textOutput) UpgradeResult(result UpgradeResult) {
	if result.DryRun {
		pterm.Printfln("[DRY RUN] Would upgrade %s from format version %d to %d.",
			result.Table, result.PreviousVersion, result.TargetVersion)
	} else {
		pterm.Printfln("Upgraded %s from format version %d to %d.",
			result.Table, result.PreviousVersion, result.TargetVersion)
	}

	pterm.Printfln("Spec: %s", result.SpecURL)
}

func (j jsonOutput) UpgradeResult(result UpgradeResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}

func (t textOutput) RollbackResult(result RollbackResult) {
	prev := "none"
	if result.PreviousSnapshotID != nil {
		prev = strconv.FormatInt(*result.PreviousSnapshotID, 10)
	}

	pterm.Printfln("Rolled back %s to snapshot %d (previous: %s).",
		result.Table, result.RolledBackToSnapshotID, prev)
}

func (j jsonOutput) RollbackResult(result RollbackResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
