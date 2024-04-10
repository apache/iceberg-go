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

type text struct{}

func (text) Identifiers(idlist []table.Identifier) {
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

func (t text) DescribeTable(tbl *table.Table) {
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
	pterm.DefaultTable.
		WithData(pterm.TableData{
			{"Current Snapshot", tbl.CurrentSnapshot().String()},
		}).Render()
	pterm.DefaultTree.WithRoot(snapshotTreeNode).Render()
	pterm.Println("Properties")
	propTable.Render()
}

func (t text) Files(tbl *table.Table, history bool) {
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

		manifestList, err := snap.Manifests(tbl.Bucket())
		if err != nil {
			t.Error(err)
			os.Exit(1)
		}

		for _, m := range manifestList {
			snapshotTree = append(snapshotTree, pterm.LeveledListItem{
				Level: 1, Text: "Manifest: " + m.FilePath(),
			})
			datafiles, err := m.FetchEntries(tbl.Bucket(), false)
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

func (text) DescribeProperties(props iceberg.Properties) {
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

func (text) Text(val string) {
	fmt.Println(val)
}

func (text) Schema(schema *iceberg.Schema) {
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

func (text) Spec(spec iceberg.PartitionSpec) {
	fmt.Println(spec)
}

func (text) Uuid(u uuid.UUID) {
	if u.String() != "" {
		fmt.Println(u.String())
	} else {
		fmt.Println("missing")
	}
}

func (text) Error(err error) {
	log.Fatal(err)
}
