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
	"os"
	"strconv"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runPartitionStats(ctx context.Context, output Output, cat catalog.Catalog, cmd *PartitionStatsCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	output.PartitionStats(tbl, cmd.SnapshotID, cmd.All)
}

func buildPartitionStatsEntries(tbl *table.Table, snapshotID *int64, all bool) []PartitionStatsEntry {
	var entries []PartitionStatsEntry

	meta := tbl.Metadata()

	if !all && snapshotID == nil {
		snap := meta.CurrentSnapshot()
		if snap == nil {
			return entries
		}

		sid := snap.SnapshotID
		snapshotID = &sid
	}

	for ps := range meta.PartitionStatistics() {
		if snapshotID != nil && ps.SnapshotID != *snapshotID {
			continue
		}

		entries = append(entries, PartitionStatsEntry{
			SnapshotID: ps.SnapshotID,
			Path:       ps.StatisticsPath,
			SizeBytes:  ps.FileSizeInBytes,
		})
	}

	return entries
}

func (t textOutput) PartitionStats(tbl *table.Table, snapshotID *int64, all bool) {
	entries := buildPartitionStatsEntries(tbl, snapshotID, all)

	if len(entries) == 0 {
		pterm.Println("No partition statistics files found.")

		return
	}

	data := pterm.TableData{{"SNAPSHOT ID", "PATH", "SIZE"}}

	for _, e := range entries {
		data = append(data, []string{
			strconv.FormatInt(e.SnapshotID, 10),
			e.Path,
			formatBytes(e.SizeBytes),
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) PartitionStats(tbl *table.Table, snapshotID *int64, all bool) {
	entries := buildPartitionStatsEntries(tbl, snapshotID, all)

	result := struct {
		Table                    string                `json:"table"`
		PartitionStatisticsFiles []PartitionStatsEntry `json:"partition_statistics_files"`
	}{
		Table:                    tableIDString(tbl),
		PartitionStatisticsFiles: entries,
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
