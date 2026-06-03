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
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runExpireSnapshots(ctx context.Context, output Output, cat catalog.Catalog, cmd *ExpireSnapshotsCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)

	var opts []table.ExpireSnapshotsOpt

	if cmd.OlderThan != "" {
		d, err := parseDuration(cmd.OlderThan)
		if err != nil {
			output.Error(fmt.Errorf("invalid --older-than: %w", err))
			os.Exit(1)
		}

		opts = append(opts, table.WithOlderThan(d))
	}

	if cmd.RetainLast != nil {
		opts = append(opts, table.WithRetainLast(*cmd.RetainLast))
	}

	opts = append(opts, table.WithPostCommit(false))

	tx := tbl.NewTransaction()
	if err := tx.ExpireSnapshots(opts...); err != nil {
		output.Error(fmt.Errorf("expire snapshots failed: %w", err))
		os.Exit(1)
	}

	staged, err := tx.StagedTable()
	if err != nil {
		output.Error(fmt.Errorf("staging table failed: %w", err))
		os.Exit(1)
	}

	expired := diffSnapshots(tbl.Metadata().Snapshots(), staged.Metadata().Snapshots())

	result := ExpireSnapshotsResult{
		DryRun:               cmd.DryRun,
		Table:                tableIDString(tbl),
		ExpiredSnapshotCount: len(expired),
		ExpiredSnapshots:     expired,
	}

	if cmd.DryRun {
		output.ExpireSnapshotsResult(result)

		return
	}

	if len(expired) == 0 {
		output.ExpireSnapshotsResult(result)

		return
	}

	prompt := fmt.Sprintf("Expire %d snapshot(s) from %s?", len(expired), tableIDString(tbl))
	if err := confirmAction(prompt, cmd.Yes); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	realOpts := make([]table.ExpireSnapshotsOpt, 0, len(opts)-1)
	for i, o := range opts {
		if i == len(opts)-1 {
			continue
		}

		realOpts = append(realOpts, o)
	}

	realTx := tbl.NewTransaction()
	if err := realTx.ExpireSnapshots(realOpts...); err != nil {
		output.Error(fmt.Errorf("expire snapshots failed: %w", err))
		os.Exit(1)
	}

	if _, err := realTx.Commit(ctx); err != nil {
		output.Error(fmt.Errorf("commit failed: %w", err))
		os.Exit(1)
	}

	result.DryRun = false
	output.ExpireSnapshotsResult(result)
}

func diffSnapshots(before, after []table.Snapshot) []SnapshotEntry {
	afterSet := make(map[int64]struct{}, len(after))
	for _, s := range after {
		afterSet[s.SnapshotID] = struct{}{}
	}

	var expired []SnapshotEntry

	for _, s := range before {
		if _, ok := afterSet[s.SnapshotID]; ok {
			continue
		}

		op := ""
		addedFiles := "-"
		deletedFiles := "-"

		if s.Summary != nil {
			op = string(s.Summary.Operation)
			if v, ok := s.Summary.Properties["added-data-files"]; ok {
				addedFiles = v
			}
			if v, ok := s.Summary.Properties["deleted-data-files"]; ok {
				deletedFiles = v
			}
		}

		expired = append(expired, SnapshotEntry{
			SnapshotID:       s.SnapshotID,
			Timestamp:        time.UnixMilli(s.TimestampMs).UTC().Format(time.RFC3339),
			ParentSnapshotID: s.ParentSnapshotID,
			Operation:        op,
			AddedDataFiles:   addedFiles,
			DeletedDataFiles: deletedFiles,
		})
	}

	return expired
}

func (t textOutput) ExpireSnapshotsResult(result ExpireSnapshotsResult) {
	if result.ExpiredSnapshotCount == 0 {
		pterm.Println("No snapshots to expire.")

		return
	}

	if result.DryRun {
		pterm.Printfln("[DRY RUN] %d snapshots would be expired:", result.ExpiredSnapshotCount)
	} else {
		pterm.Printfln("Expired %d snapshots from %s.", result.ExpiredSnapshotCount, result.Table)
	}

	data := pterm.TableData{{"SNAPSHOT ID", "TIMESTAMP", "OP"}}

	for _, e := range result.ExpiredSnapshots {
		data = append(data, []string{
			strconv.FormatInt(e.SnapshotID, 10),
			e.Timestamp,
			e.Operation,
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) ExpireSnapshotsResult(result ExpireSnapshotsResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
