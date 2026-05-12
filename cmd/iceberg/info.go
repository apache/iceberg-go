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
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/pterm/pterm"
)

type InfoCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type TableInfo struct {
	Table             string     `json:"table"`
	UUID              string     `json:"uuid"`
	FormatVersion     int        `json:"format_version"`
	Location          string     `json:"location"`
	LastUpdated       string     `json:"last_updated"`
	CurrentSnapshotID *int64     `json:"current_snapshot_id"`
	NextRowID         *int64     `json:"next_row_id,omitempty"`
	SchemaID          int        `json:"schema_id"`
	SchemaFieldCount  int        `json:"schema_field_count"`
	PartitionSpec     string     `json:"partition_spec"`
	SortOrder         string     `json:"sort_order"`
	SnapshotCount     int        `json:"snapshot_count"`
	Refs              RefSummary `json:"refs"`
	PropertyCount     int        `json:"property_count"`
}

type RefSummary struct {
	Branches int `json:"branches"`
	Tags     int `json:"tags"`
}

func tableIDString(tbl *table.Table) string {
	return strings.Join(tbl.Identifier(), ".")
}

func buildTableInfo(tbl *table.Table) TableInfo {
	meta := tbl.Metadata()

	lastUpdated := time.UnixMilli(meta.LastUpdatedMillis()).UTC().Format(time.RFC3339)

	var currentSnapshotID *int64
	if snap := meta.CurrentSnapshot(); snap != nil {
		id := snap.SnapshotID
		currentSnapshotID = &id
	}

	var nextRowID *int64
	if rid := meta.NextRowID(); rid > 0 {
		nextRowID = &rid
	}

	schema := meta.CurrentSchema()

	specStr := meta.PartitionSpec().String()

	var sortOrderStr string
	if meta.SortOrder().Len() == 0 {
		sortOrderStr = "unsorted"
	} else {
		sortOrderStr = meta.SortOrder().String()
	}

	var refs RefSummary
	for _, ref := range meta.Refs() {
		switch ref.SnapshotRefType {
		case table.BranchRef:
			refs.Branches++
		case table.TagRef:
			refs.Tags++
		}
	}

	return TableInfo{
		Table:             tableIDString(tbl),
		UUID:              meta.TableUUID().String(),
		FormatVersion:     meta.Version(),
		Location:          meta.Location(),
		LastUpdated:       lastUpdated,
		CurrentSnapshotID: currentSnapshotID,
		NextRowID:         nextRowID,
		SchemaID:          schema.ID,
		SchemaFieldCount:  schema.NumFields(),
		PartitionSpec:     specStr,
		SortOrder:         sortOrderStr,
		SnapshotCount:     len(meta.Snapshots()),
		Refs:              refs,
		PropertyCount:     len(meta.Properties()),
	}
}

func pluralize(n int, singular, plural string) string {
	if n == 1 {
		return "1 " + singular
	}

	return strconv.Itoa(n) + " " + plural
}

func (t textOutput) Info(tbl *table.Table) {
	info := buildTableInfo(tbl)

	snapStr := "-"
	if info.CurrentSnapshotID != nil {
		snapStr = strconv.FormatInt(*info.CurrentSnapshotID, 10)
	}

	schemaStr := fmt.Sprintf("%d (%d fields)", info.SchemaID, info.SchemaFieldCount)
	refsStr := pluralize(info.Refs.Branches, "branch", "branches") + ", " + pluralize(info.Refs.Tags, "tag", "tags")

	data := pterm.TableData{
		{"Table:", info.Table},
		{"UUID:", info.UUID},
		{"Format version:", strconv.Itoa(info.FormatVersion)},
		{"Location:", info.Location},
		{"Last updated:", info.LastUpdated},
		{"Current snapshot:", snapStr},
	}

	if info.NextRowID != nil {
		data = append(data, []string{"Next row ID:", strconv.FormatInt(*info.NextRowID, 10)})
	}

	data = append(data,
		[]string{"Schema ID:", schemaStr},
		[]string{"Partition spec:", info.PartitionSpec},
		[]string{"Sort order:", info.SortOrder},
		[]string{"Snapshots:", strconv.Itoa(info.SnapshotCount)},
		[]string{"Refs:", refsStr},
		[]string{"Properties:", strconv.Itoa(info.PropertyCount)},
	)

	pterm.DefaultTable.WithData(data).Render()
}

func (j jsonOutput) Info(tbl *table.Table) {
	info := buildTableInfo(tbl)
	if err := json.NewEncoder(os.Stdout).Encode(info); err != nil {
		j.Error(err)
	}
}
