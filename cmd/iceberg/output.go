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
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"github.com/google/uuid"
	"github.com/pterm/pterm"
	"github.com/pterm/pterm/putils"
)

type Output interface {
	Identifiers([]table.Identifier)
	DescribeTable(*table.Table)
	Files(tbl *table.Table, history bool)
	DescribeProperties(iceberg.Properties)
	Text(string)
	Schema(*iceberg.Schema)
	Spec(iceberg.PartitionSpec)
	Uuid(uuid.UUID)
	Error(error)
}

type textOutput struct{}

func (textOutput) Identifiers(idlist []table.Identifier) {
	data := pterm.TableData{[]string{"IDs"}}
	for _, ids := range idlist {
		data = append(data, []string{strings.Join(ids, ".")})
	}

	pterm.DefaultTable.
		WithBoxed(true).
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (t textOutput) DescribeTable(tbl *table.Table) {
	propData := pterm.TableData{{"key", "value"}}
	for k, v := range tbl.Metadata().Properties() {
		propData = append(propData, []string{k, v})
	}
	propTable := pterm.DefaultTable.
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(propData)

	snapshotList := pterm.LeveledList{}
	for _, s := range tbl.Metadata().Snapshots() {
		var manifest string
		if s.ManifestList != "" {
			manifest = ": " + s.ManifestList
		}

		snapshotList = append(snapshotList, pterm.LeveledListItem{
			Level: 0, Text: fmt.Sprintf("Snapshot %d, schema %d%s",
				s.SnapshotID, *s.SchemaID, manifest),
		})
	}

	snapshotTreeNode := putils.TreeFromLeveledList(snapshotList)
	snapshotTreeNode.Text = "Snapshots"

	pterm.DefaultTable.
		WithData(pterm.TableData{
			{"Table format version", strconv.Itoa(tbl.Metadata().Version())},
			{"Metadata location", tbl.MetadataLocation()},
			{"Table UUID", tbl.Metadata().TableUUID().String()},
			{"Last updated", strconv.Itoa(int(tbl.Metadata().LastUpdatedMillis()))},
			{"Sort Order", tbl.SortOrder().String()},
			{"Partition Spec", tbl.Spec().String()},
		}).Render()

	t.Schema(tbl.Schema())
	snap := ""
	if tbl.CurrentSnapshot() != nil {
		snap = tbl.CurrentSnapshot().String()
	}
	pterm.DefaultTable.
		WithData(pterm.TableData{
			{"Current Snapshot", snap},
		}).Render()
	pterm.DefaultTree.WithRoot(snapshotTreeNode).Render()
	pterm.Println("Properties")
	propTable.Render()
}

func (t textOutput) Files(tbl *table.Table, history bool) {
	var snapshots []table.Snapshot
	if history {
		snapshots = tbl.Metadata().Snapshots()
	} else {
		snap := tbl.CurrentSnapshot()
		if snap != nil {
			snapshots = []table.Snapshot{*snap}
		}
	}

	snapshotTree := pterm.LeveledList{}
	for _, snap := range snapshots {
		manifest := snap.ManifestList
		if manifest != "" {
			manifest = ": " + manifest
		}

		snapshotTree = append(snapshotTree, pterm.LeveledListItem{
			Level: 0,
			Text: fmt.Sprintf("Snapshot %d, schema %d%s",
				snap.SnapshotID, *snap.SchemaID, manifest),
		})

		afs, err := tbl.FS(context.TODO())
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}
		manifestList, err := snap.Manifests(afs)
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}

		for _, m := range manifestList {
			snapshotTree = append(snapshotTree, pterm.LeveledListItem{
				Level: 1, Text: "Manifest: " + m.FilePath(),
			})
			datafiles, err := m.FetchEntries(afs, false)
			if err != nil {
				t.Error(err)
				os.Exit(1)
			}
			for _, e := range datafiles {
				snapshotTree = append(snapshotTree, pterm.LeveledListItem{
					Level: 2, Text: "Datafile: " + e.DataFile().FilePath(),
				})
			}
		}
	}

	node := putils.TreeFromLeveledList(snapshotTree)
	node.Text = "Snapshots: " + strings.Join(tbl.Identifier(), ".")
	pterm.DefaultTree.WithRoot(node).Render()
}

func (textOutput) DescribeProperties(props iceberg.Properties) {
	data := pterm.TableData{[]string{"Key", "Value"}}
	for k, v := range props {
		data = append(data, []string{k, v})
	}

	pterm.DefaultTable.
		WithBoxed(true).
		WithHasHeader(true).
		WithHeaderRowSeparator("-").
		WithData(data).Render()
}

func (textOutput) Text(val string) {
	fmt.Println(val)
}

func (textOutput) Schema(schema *iceberg.Schema) {
	schemaTree := pterm.LeveledList{}
	var addChildren func(iceberg.NestedField, int)
	addChildren = func(nf iceberg.NestedField, depth int) {
		if nested, ok := nf.Type.(iceberg.NestedType); ok {
			for _, n := range nested.Fields() {
				schemaTree = append(schemaTree, pterm.LeveledListItem{
					Level: depth, Text: n.String(),
				})
				addChildren(n, depth+1)
			}
		}
	}

	for _, f := range schema.Fields() {
		schemaTree = append(schemaTree, pterm.LeveledListItem{
			Level: 0, Text: f.String(),
		})
		addChildren(f, 1)
	}
	schemaTreeNode := putils.TreeFromLeveledList(schemaTree)
	schemaTreeNode.Text = "Current Schema, id=" + strconv.Itoa(schema.ID)
	pterm.DefaultTree.WithRoot(schemaTreeNode).Render()
}

func (textOutput) Spec(spec iceberg.PartitionSpec) {
	fmt.Println(spec)
}

func (textOutput) Uuid(u uuid.UUID) {
	if u.String() != "" {
		fmt.Println(u.String())
	} else {
		fmt.Println("missing")
	}
}

func (textOutput) Error(err error) {
	log.Fatal(err)
}

type jsonOutput struct{}

func (j jsonOutput) Identifiers(idList []table.Identifier) {
	type dataType struct {
		Identifiers []table.Identifier `json:"identifiers"`
	}

	data := dataType{Identifiers: idList}
	if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) DescribeTable(tbl *table.Table) {
	type dataType struct {
		Metadata         table.Metadata        `json:"metadata,omitempty"`
		MetadataLocation string                `json:"metadata-location,omitempty"`
		SortOrder        table.SortOrder       `json:"sort-order,omitempty"`
		CurrentSnapshot  *table.Snapshot       `json:"current-snapshot,omitempty"`
		Spec             iceberg.PartitionSpec `json:"spec,omitempty"`
		Schema           *iceberg.Schema       `json:"schema,omitempty"`
	}

	data := dataType{
		Metadata:         tbl.Metadata(),
		MetadataLocation: tbl.MetadataLocation(),
		SortOrder:        tbl.SortOrder(),
		CurrentSnapshot:  tbl.CurrentSnapshot(),
		Spec:             tbl.Spec(),
		Schema:           tbl.Schema(),
	}
	if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Files(tbl *table.Table, history bool) {
	if history {
		type dataType struct {
			Snapshots []table.Snapshot `json:"snapshots"`
		}

		data := dataType{
			Snapshots: tbl.Metadata().Snapshots(),
		}
		if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
			j.Error(err)
		}
	} else {
		type dataType struct {
			Snapshot *table.Snapshot `json:"snapshot"`
		}

		data := dataType{
			Snapshot: tbl.CurrentSnapshot(),
		}
		if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
			j.Error(err)
		}
	}
}

func (j jsonOutput) DescribeProperties(props iceberg.Properties) {
	if err := json.NewEncoder(os.Stdout).Encode(props); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Text(s string) {
	type dataType struct {
		Data string `json:"data"`
	}

	data := dataType{
		Data: s,
	}
	if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Schema(schema *iceberg.Schema) {
	if err := json.NewEncoder(os.Stdout).Encode(schema); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Spec(spec iceberg.PartitionSpec) {
	if err := json.NewEncoder(os.Stdout).Encode(spec); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Uuid(u uuid.UUID) {
	type dataType struct {
		UUID uuid.UUID `json:"uuid"`
	}

	data := dataType{
		UUID: u,
	}
	if err := json.NewEncoder(os.Stdout).Encode(data); err != nil {
		j.Error(err)
	}
}

func (j jsonOutput) Error(err error) {
	log.Fatal(err)
}
