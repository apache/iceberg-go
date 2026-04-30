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
	"os"
	"strconv"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/table/compaction"
	"github.com/pterm/pterm"
)

type compactConfig struct {
	tableID                     string
	targetFileSize              int64
	analyzeOnly                 bool
	partialProgress             bool
	preserveDeadEqualityDeletes bool
}

func compact(ctx context.Context, output Output, cat catalog.Catalog, cfg compactConfig) {
	tbl := loadTable(ctx, output, cat, cfg.tableID)

	compCfg := compaction.DefaultConfig()
	if cfg.targetFileSize > 0 {
		compCfg.TargetFileSizeBytes = cfg.targetFileSize
		// Derive min/max using the same ratios as DefaultConfig (75%/180%).
		compCfg.MinFileSizeBytes = compCfg.TargetFileSizeBytes * 3 / 4
		compCfg.MaxFileSizeBytes = compCfg.TargetFileSizeBytes * 9 / 5
	}

	plan, err := compaction.Analyze(ctx, tbl, compCfg)
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	if len(plan.Groups) == 0 {
		output.Text("No files need compaction.")

		return
	}

	printCompactionPlan(output, plan)

	if cfg.analyzeOnly {
		return
	}

	compactRun(ctx, output, tbl, plan, cfg)
}

func printCompactionPlan(output Output, plan compaction.Plan) {
	var candidateBytes int64
	for _, g := range plan.Groups {
		candidateBytes += g.TotalSizeBytes
	}

	candidateFiles := plan.TotalInputFiles - plan.SkippedFiles

	output.Text(fmt.Sprintf("Compaction Plan: %d files scanned, %d to rewrite, %d already optimal",
		plan.TotalInputFiles, candidateFiles, plan.SkippedFiles))
	output.Text(fmt.Sprintf("  Groups:            %d", len(plan.Groups)))
	output.Text(fmt.Sprintf("  Est. output files: %d", plan.EstOutputFiles))
	output.Text("  Input size:        " + formatBytes(candidateBytes))

	// Only render the per-group table for text output.
	// JSON output uses output.Text() above which wraps in structured format.
	if _, ok := output.(textOutput); ok {
		data := pterm.TableData{{"#", "Partition", "Files", "Size", "Delete Files"}}
		for i, g := range plan.Groups {
			partKey := g.PartitionKey
			if partKey == "" {
				partKey = "(unpartitioned)"
			}

			data = append(data, []string{
				strconv.Itoa(i + 1),
				partKey,
				strconv.Itoa(len(g.Tasks)),
				formatBytes(g.TotalSizeBytes),
				strconv.Itoa(g.DeleteFileCount),
			})
		}

		pterm.DefaultTable.
			WithHasHeader(true).
			WithHeaderRowSeparator("-").
			WithData(data).Render()
	}
}

func compactRun(ctx context.Context, output Output, tbl *table.Table, plan compaction.Plan, cfg compactConfig) {
	candidateFiles := plan.TotalInputFiles - plan.SkippedFiles
	output.Text(fmt.Sprintf("Compacting %d groups (%d files)...",
		len(plan.Groups), candidateFiles))

	groups := make([]table.CompactionTaskGroup, len(plan.Groups))
	for i, g := range plan.Groups {
		groups[i] = table.CompactionTaskGroup{
			PartitionKey:   g.PartitionKey,
			Tasks:          g.Tasks,
			TotalSizeBytes: g.TotalSizeBytes,
		}
	}

	tx := tbl.NewTransaction()
	rewriteOpts := table.RewriteDataFilesOptions{
		PartialProgress: cfg.partialProgress,
	}

	// Cleanup of dead equality-delete files. The executor only
	// orchestrates the commit; the policy + walk live in
	// table/compaction. Skipped in partial-progress mode (per-group
	// cleanup is a follow-up) and when the caller opts out.
	if !cfg.partialProgress && !cfg.preserveDeadEqualityDeletes {
		snap := tbl.CurrentSnapshot()
		if snap != nil {
			fs, err := tbl.FS(ctx)
			if err != nil {
				output.Error(fmt.Errorf("open fs for eq-delete cleanup: %w", err))
				os.Exit(1)
			}
			rewrittenSet := make(map[string]struct{})
			for _, g := range plan.Groups {
				for _, task := range g.Tasks {
					rewrittenSet[task.File.FilePath()] = struct{}{}
				}
			}
			deadEqDeletes, err := compaction.CollectDeadEqualityDeletes(ctx, fs, snap, rewrittenSet)
			if err != nil {
				output.Error(fmt.Errorf("collect dead equality deletes: %w", err))
				os.Exit(1)
			}
			rewriteOpts.ExtraDeleteFilesToRemove = deadEqDeletes
		}
	}

	result, err := tx.RewriteDataFiles(ctx, groups, rewriteOpts)
	if err != nil {
		if result != nil && result.RewrittenGroups > 0 {
			output.Text(fmt.Sprintf("  (partial: %d groups committed before failure)", result.RewrittenGroups))
		}
		output.Error(fmt.Errorf("compaction failed: %w", err))
		os.Exit(1)
	}

	if _, err := tx.Commit(ctx); err != nil {
		output.Error(fmt.Errorf("commit failed: %w", err))
		os.Exit(1)
	}

	totalRemovedDeletes := result.RemovedPositionDeleteFiles + result.RemovedEqualityDeleteFiles
	output.Text(fmt.Sprintf("Done. Rewrote %d -> %d files. Removed %d delete files (%d position, %d equality).",
		result.RemovedDataFiles, result.AddedDataFiles,
		totalRemovedDeletes, result.RemovedPositionDeleteFiles, result.RemovedEqualityDeleteFiles))
	output.Text(fmt.Sprintf("  Size: %s -> %s",
		formatBytes(result.BytesBefore), formatBytes(result.BytesAfter)))
}

func formatBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return strconv.FormatInt(b, 10) + " B"
	}
}
