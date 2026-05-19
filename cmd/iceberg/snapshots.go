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
	"time"

	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

func runSnapshots(ctx context.Context, output Output, cat catalog.Catalog, cmd *SnapshotsCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	output.Snapshots(tbl)
}

func runRefs(ctx context.Context, output Output, cat catalog.Catalog, cmd *RefsCmd) {
	tbl := loadTable(ctx, output, cat, cmd.TableID)
	output.Refs(tbl, cmd.Type)
}

func buildSnapshotEntries(tbl *table.Table) []SnapshotEntry {
	snapshots := tbl.Metadata().Snapshots()
	entries := make([]SnapshotEntry, 0, len(snapshots))

	for i := len(snapshots) - 1; i >= 0; i-- {
		s := snapshots[i]

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

		entries = append(entries, SnapshotEntry{
			SnapshotID:       s.SnapshotID,
			Timestamp:        time.UnixMilli(s.TimestampMs).UTC().Format(time.RFC3339),
			ParentSnapshotID: s.ParentSnapshotID,
			Operation:        op,
			AddedDataFiles:   addedFiles,
			DeletedDataFiles: deletedFiles,
		})
	}

	return entries
}

func buildRefEntries(tbl *table.Table, filterType string) []RefEntry {
	var entries []RefEntry

	for name, ref := range tbl.Metadata().Refs() {
		refType := string(ref.SnapshotRefType)
		if filterType != "" && refType != filterType {
			continue
		}

		entries = append(entries, RefEntry{
			Name:               name,
			Type:               refType,
			SnapshotID:         ref.SnapshotID,
			MaxRefAgeMs:        ref.MaxRefAgeMs,
			MaxSnapshotAgeMs:   ref.MaxSnapshotAgeMs,
			MinSnapshotsToKeep: ref.MinSnapshotsToKeep,
		})
	}

	return entries
}

func (t textOutput) Snapshots(tbl *table.Table) {
	entries := buildSnapshotEntries(tbl)

	if len(entries) == 0 {
		pterm.Println("No snapshots found.")

		return
	}

	data := pterm.TableData{{"SNAPSHOT ID", "TIMESTAMP", "PARENT", "OP", "+FILES", "-FILES"}}

	for _, e := range entries {
		parent := "-"
		if e.ParentSnapshotID != nil {
			parent = strconv.FormatInt(*e.ParentSnapshotID, 10)
		}

		data = append(data, []string{
			strconv.FormatInt(e.SnapshotID, 10),
			e.Timestamp,
			parent,
			e.Operation,
			e.AddedDataFiles,
			e.DeletedDataFiles,
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) Snapshots(tbl *table.Table) {
	entries := buildSnapshotEntries(tbl)

	result := struct {
		Table     string          `json:"table"`
		Snapshots []SnapshotEntry `json:"snapshots"`
	}{
		Table:     tableIDString(tbl),
		Snapshots: entries,
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}

func (t textOutput) Refs(tbl *table.Table, filterType string) {
	entries := buildRefEntries(tbl, filterType)

	if len(entries) == 0 {
		pterm.Println("No refs found.")

		return
	}

	data := pterm.TableData{{"NAME", "TYPE", "SNAPSHOT ID", "MAX REF AGE", "MAX SNAP AGE", "MIN SNAPS"}}

	for _, e := range entries {
		minSnaps := "-"
		if e.MinSnapshotsToKeep != nil {
			minSnaps = strconv.Itoa(*e.MinSnapshotsToKeep)
		}

		data = append(data, []string{
			e.Name,
			e.Type,
			strconv.FormatInt(e.SnapshotID, 10),
			formatDurationMs(e.MaxRefAgeMs),
			formatDurationMs(e.MaxSnapshotAgeMs),
			minSnaps,
		})
	}

	pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (j jsonOutput) Refs(tbl *table.Table, filterType string) {
	entries := buildRefEntries(tbl, filterType)

	result := struct {
		Table string     `json:"table"`
		Refs  []RefEntry `json:"refs"`
	}{
		Table: tableIDString(tbl),
		Refs:  entries,
	}

	if err := json.NewEncoder(os.Stdout).Encode(result); err != nil {
		j.Error(err)
	}
}
