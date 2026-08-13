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

func runCleanOrphanFiles(ctx context.Context, output Output, cat catalog.Catalog, cmd *CleanOrphanFilesCmd) {
	olderThan, err := parseDuration(cmd.OlderThan)
	if err != nil {
		output.Error(fmt.Errorf("invalid --older-than: %w", err))
		os.Exit(1)
	}

	tbl := loadTable(ctx, output, cat, cmd.TableID)

	opts := []table.OrphanCleanupOption{
		table.WithFilesOlderThan(olderThan),
	}

	if cmd.Location != "" {
		opts = append(opts, table.WithLocation(cmd.Location))
	}

	plan, err := tbl.PlanOrphanFiles(ctx, opts...)
	if err != nil {
		output.Error(fmt.Errorf("orphan file scan failed: %w", err))
		os.Exit(1)
	}

	planFiles := plan.Files()
	result := table.OrphanCleanupResult{
		OrphanFileLocations: planFiles,
		OrphanFiles:         plan.OrphanFiles(),
		TotalSizeBytes:      plan.TotalSizeBytes(),
	}
	cliResult := buildCleanOrphanFilesResult(tbl, result, cmd.DryRun)

	if cmd.DryRun {
		output.CleanOrphanFilesResult(cliResult)

		return
	}

	if len(planFiles) == 0 {
		output.CleanOrphanFilesResult(cliResult)

		return
	}

	// Text output shows the exact plan before confirmation. JSON output emits
	// only the final result so one invocation produces one JSON document.
	if shouldPrintCleanOrphanPreview(output) {
		previewResult := buildCleanOrphanFilesResult(tbl, result, true)
		output.CleanOrphanFilesResult(previewResult)
	}

	prompt := fmt.Sprintf("Delete %d orphan file(s) (%s) from %s?",
		len(planFiles), formatBytes(plan.TotalSizeBytes()), tableIDString(tbl))
	if err := confirmAction(prompt, cmd.Yes); err != nil {
		output.Error(err)
		os.Exit(1)
	}

	deleteResult, err := tbl.ExecuteOrphanCleanup(ctx, plan)
	if err != nil {
		output.Error(fmt.Errorf("orphan file deletion failed: %w", err))
		os.Exit(1)
	}

	cliResult = buildCleanOrphanFilesResult(tbl, deleteResult, false)
	output.CleanOrphanFilesResult(cliResult)
}

func shouldPrintCleanOrphanPreview(output Output) bool {
	switch output.(type) {
	case jsonOutput:
		return false
	default:
		return true
	}
}

func buildCleanOrphanFilesResult(tbl *table.Table, result table.OrphanCleanupResult, dryRun bool) CleanOrphanFilesResult {
	var entries []OrphanFileEntry
	if dryRun {
		entries = make([]OrphanFileEntry, 0, len(result.OrphanFiles))
		for _, f := range result.OrphanFiles {
			entries = append(entries, OrphanFileEntry{
				Path:      f.Path,
				SizeBytes: f.SizeBytes,
			})
		}
	} else {
		deletedSet := make(map[string]struct{}, len(result.DeletedFiles))
		for _, f := range result.DeletedFiles {
			deletedSet[f] = struct{}{}
		}

		entries = make([]OrphanFileEntry, 0, len(result.DeletedFiles))
		for _, f := range result.OrphanFiles {
			if _, ok := deletedSet[f.Path]; !ok {
				continue
			}

			entries = append(entries, OrphanFileEntry{
				Path:      f.Path,
				SizeBytes: f.SizeBytes,
			})
		}
	}

	return CleanOrphanFilesResult{
		DryRun:          dryRun,
		Table:           tableIDString(tbl),
		OrphanFileCount: len(entries),
		TotalSizeBytes:  result.TotalSizeBytes,
		OrphanFiles:     entries,
	}
}

func (t textOutput) CleanOrphanFilesResult(result CleanOrphanFilesResult) {
	if result.OrphanFileCount == 0 {
		pterm.Println("No orphan files found.")

		return
	}

	sizeStr := formatBytes(result.TotalSizeBytes)

	if result.DryRun {
		pterm.Printfln("[DRY RUN] %d orphan files found (%s):", result.OrphanFileCount, sizeStr)
	} else {
		pterm.Printfln("Deleted %d orphan files (%s) from %s.", result.OrphanFileCount, sizeStr, result.Table)
	}

	data := pterm.TableData{{"#", "PATH", "SIZE_BYTES"}}

	for i, f := range result.OrphanFiles {
		data = append(data, []string{
			strconv.Itoa(i + 1),
			f.Path,
			formatBytes(f.SizeBytes),
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) CleanOrphanFilesResult(result CleanOrphanFilesResult) {
	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
