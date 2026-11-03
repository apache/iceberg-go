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
)

func runBranchFastForward(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchFastForwardCmd) {
	if err := validateRefName(cmd.BranchName); err != nil {
		output.Error(fmt.Errorf("invalid target branch: %w", err))
		osExit(1)

		return
	}
	if err := validateRefName(cmd.SourceRef); err != nil {
		output.Error(fmt.Errorf("invalid source ref: %w", err))
		osExit(1)

		return
	}

	tbl := loadTable(ctx, output, cat, cmd.TableID)
	tx, err := tbl.NewTransactionOnBranch(cmd.BranchName)
	if err != nil {
		output.Error(fmt.Errorf("cannot begin branch transaction: %w", err))
		osExit(1)

		return
	}
	if err := tx.FastForwardBranch(cmd.BranchName, cmd.SourceRef); err != nil {
		output.Error(fmt.Errorf("cannot fast-forward branch: %w", err))
		osExit(1)

		return
	}

	newHead := tbl.SnapshotByName(cmd.SourceRef)
	oldHead := tbl.SnapshotByName(cmd.BranchName)
	var prompt string
	if oldHead == nil {
		prompt = fmt.Sprintf("Create branch %q on %s at snapshot %d from ref %q?",
			cmd.BranchName, tableIDString(tbl), newHead.SnapshotID, cmd.SourceRef)
	} else {
		prompt = fmt.Sprintf("Fast-forward branch %q on %s from snapshot %d to %d (ref %q)?",
			cmd.BranchName, tableIDString(tbl), oldHead.SnapshotID, newHead.SnapshotID, cmd.SourceRef)
	}
	if err := confirmAction(prompt, cmd.Yes); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	updated, err := tx.Commit(ctx)
	if err != nil {
		output.Error(fmt.Errorf("failed to fast-forward branch: %w", err))
		osExit(1)

		return
	}
	output.Text(fmt.Sprintf("Fast-forwarded branch %q on %s to snapshot %d",
		cmd.BranchName, tableIDString(updated), updated.SnapshotByName(cmd.BranchName).SnapshotID))
}

func runBranchReplace(ctx context.Context, output Output, cat catalog.Catalog, cmd *BranchReplaceCmd) {
	if err := validateRefName(cmd.BranchName); err != nil {
		output.Error(fmt.Errorf("invalid branch name: %w", err))
		osExit(1)

		return
	}

	tbl := loadTable(ctx, output, cat, cmd.TableID)
	tx, err := tbl.NewTransactionOnBranch(cmd.BranchName)
	if err != nil {
		output.Error(fmt.Errorf("cannot begin branch transaction: %w", err))
		osExit(1)

		return
	}
	if err := tx.ReplaceBranch(cmd.BranchName, cmd.SnapshotID); err != nil {
		output.Error(fmt.Errorf("cannot replace branch: %w", err))
		osExit(1)

		return
	}

	oldHead := tbl.SnapshotByName(cmd.BranchName)
	if err := confirmAction(fmt.Sprintf("Replace branch %q on %s from snapshot %d to %d?",
		cmd.BranchName, tableIDString(tbl), oldHead.SnapshotID, cmd.SnapshotID), cmd.Yes); err != nil {
		output.Error(err)
		osExit(1)

		return
	}

	updated, err := tx.Commit(ctx)
	if err != nil {
		output.Error(fmt.Errorf("failed to replace branch: %w", err))
		osExit(1)

		return
	}
	output.Text(fmt.Sprintf("Replaced branch %q on %s with snapshot %d",
		cmd.BranchName, tableIDString(updated), cmd.SnapshotID))
}
