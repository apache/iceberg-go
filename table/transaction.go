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

package table

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/pterm/pterm"
)

type schemaCompatVisitor struct {
	provided *iceberg.Schema

	errorData pterm.TableData
}

func checkSchemaCompat(requested, provided *iceberg.Schema) error {
	sc := &schemaCompatVisitor{
		provided:  provided,
		errorData: pterm.TableData{{"", "Table Field", "Requested Field"}},
	}

	_, compat := iceberg.PreOrderVisit(requested, sc)

	return compat
}

func checkArrowSchemaCompat(requested *iceberg.Schema, provided *arrow.Schema, downcastNanoToMicro bool) error {
	mapping := requested.NameMapping()
	providedSchema, err := ArrowSchemaToIceberg(provided, downcastNanoToMicro, mapping)
	if err != nil {
		return err
	}

	return checkSchemaCompat(requested, providedSchema)
}

func (sc *schemaCompatVisitor) isFieldCompat(lhs iceberg.NestedField) bool {
	rhs, ok := sc.provided.FindFieldByID(lhs.ID)
	if !ok {
		if lhs.Required {
			sc.errorData = append(sc.errorData,
				[]string{"❌", lhs.String(), "missing"})

			return false
		}
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), "missing"})

		return true
	}

	if lhs.Required && !rhs.Required {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	if lhs.Type.Equals(rhs.Type) {
		sc.errorData = append(sc.errorData,
			[]string{"✅", lhs.String(), rhs.String()})

		return true
	}

	// we only check that parent node is also of the same type
	// we check the type of the child nodes as we traverse them later
	switch lhs.Type.(type) {
	case *iceberg.StructType:
		if rhs, ok := rhs.Type.(*iceberg.StructType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.ListType:
		if rhs, ok := rhs.Type.(*iceberg.ListType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	case *iceberg.MapType:
		if rhs, ok := rhs.Type.(*iceberg.MapType); ok {
			sc.errorData = append(sc.errorData,
				[]string{"✅", lhs.String(), rhs.String()})

			return true
		}
	}

	if _, err := iceberg.PromoteType(rhs.Type, lhs.Type); err != nil {
		sc.errorData = append(sc.errorData,
			[]string{"❌", lhs.String(), rhs.String()})

		return false
	}

	sc.errorData = append(sc.errorData,
		[]string{"✅", lhs.String(), rhs.String()})

	return true
}

func (sc *schemaCompatVisitor) Schema(s *iceberg.Schema, v func() bool) bool {
	if !v() {
		pterm.DisableColor()
		tbl := pterm.DefaultTable.WithHasHeader(true).WithData(sc.errorData)
		tbl.Render()
		txt, _ := tbl.Srender()
		pterm.EnableColor()
		panic("mismatch in fields:\n" + txt)
	}

	return true
}

func (sc *schemaCompatVisitor) Struct(st iceberg.StructType, v []func() bool) bool {
	out := true
	for _, res := range v {
		out = res() && out
	}

	return out
}

func (sc *schemaCompatVisitor) Field(n iceberg.NestedField, v func() bool) bool {
	return sc.isFieldCompat(n) && v()
}

func (sc *schemaCompatVisitor) List(l iceberg.ListType, v func() bool) bool {
	return sc.isFieldCompat(l.ElementField()) && v()
}

func (sc *schemaCompatVisitor) Map(m iceberg.MapType, vk, vv func() bool) bool {
	return sc.isFieldCompat(m.KeyField()) && sc.isFieldCompat(m.ValueField()) && vk() && vv()
}

func (sc *schemaCompatVisitor) Primitive(p iceberg.PrimitiveType) bool {
	return true
}

type snapshotUpdate struct {
	txn           *Transaction
	io            io.WriteFileIO
	snapshotProps iceberg.Properties
}

func (s snapshotUpdate) fastAppend() *snapshotProducer {
	return newFastAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

func (s snapshotUpdate) mergeAppend() *snapshotProducer {
	return newMergeAppendFilesProducer(OpAppend, s.txn, s.io, nil, s.snapshotProps)
}

type Transaction struct {
	tbl  *Table
	meta *MetadataBuilder

	reqs []Requirement

	mx        sync.Mutex
	committed bool
}

func (t *Transaction) apply(updates []Update, reqs []Requirement) error {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return errors.New("transaction has already been committed")
	}

	current, err := t.meta.Build()
	if err != nil {
		return err
	}

	for _, r := range reqs {
		if err := r.Validate(current); err != nil {
			return err
		}
	}

	existing := map[string]struct{}{}
	for _, r := range t.reqs {
		existing[r.GetType()] = struct{}{}
	}

	for _, r := range reqs {
		if _, ok := existing[r.GetType()]; !ok {
			t.reqs = append(t.reqs, r)
		}
	}

	prevUpdates, prevLastUpdated := len(t.meta.updates), t.meta.lastUpdatedMS
	for _, u := range updates {
		if err := u.Apply(t.meta); err != nil {
			return err
		}
	}

	if prevUpdates < len(t.meta.updates) {
		if prevLastUpdated == t.meta.lastUpdatedMS {
			t.meta.lastUpdatedMS = time.Now().UnixMilli()
		}
	}

	return nil
}

func (t *Transaction) updateSnapshot(props iceberg.Properties) snapshotUpdate {
	return snapshotUpdate{
		txn:           t,
		io:            t.tbl.fs.(io.WriteFileIO),
		snapshotProps: props,
	}
}

func (t *Transaction) appendSnapshotProducer(props iceberg.Properties) *snapshotProducer {
	manifestMerge := t.meta.props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault)

	updateSnapshot := t.updateSnapshot(props)
	if manifestMerge {
		return updateSnapshot.mergeAppend()
	}

	return updateSnapshot.fastAppend()
}

func (t *Transaction) SetProperties(props iceberg.Properties) error {
	if len(props) > 0 {
		return t.apply([]Update{NewSetPropertiesUpdate(props)}, nil)
	}

	return nil
}

func (t *Transaction) Append(rdr array.RecordReader, snapshotProps iceberg.Properties) error {
	if err := checkArrowSchemaCompat(t.meta.CurrentSchema(), rdr.Schema(), false); err != nil {
		return err
	}

	t.appendSnapshotProducer(snapshotProps)

	return nil
}

func (t *Transaction) AddFiles(files []string, snapshotProps iceberg.Properties, checkDuplicates bool) error {
	set := make(map[string]string)
	for _, f := range files {
		set[f] = f
	}

	if len(set) != len(files) {
		return errors.New("file paths must be unique for AppendDataFiles")
	}

	if checkDuplicates {
		// TODO: implement
	}

	if t.meta.NameMapping() == nil {
		nameMapping := t.meta.CurrentSchema().NameMapping()
		mappingJson, err := json.Marshal(nameMapping)
		if err != nil {
			return err
		}
		err = t.SetProperties(iceberg.Properties{DefaultNameMappingKey: string(mappingJson)})
		if err != nil {
			return err
		}
	}

	updater := t.updateSnapshot(snapshotProps).fastAppend()

	dataFiles := parquetFilesToDataFiles(t.tbl.fs, t.meta, slices.Values(files))
	for df, err := range dataFiles {
		if err != nil {
			return err
		}
		updater.appendDataFile(df)
	}

	updates, reqs, err := updater.commit()
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

func (t *Transaction) StagedTable() (*Table, error) {
	updatedMeta, err := t.meta.Build()
	if err != nil {
		return nil, err
	}

	return New(t.tbl.identifier, updatedMeta,
		updatedMeta.Location(), t.tbl.fs, t.tbl.cat), nil
}

func (t *Transaction) Commit(ctx context.Context) (*Table, error) {
	t.mx.Lock()
	defer t.mx.Unlock()

	if t.committed {
		return nil, errors.New("transaction has already been committed")
	}

	t.committed = true

	if len(t.meta.updates) > 0 {
		t.reqs = append(t.reqs, AssertTableUUID(t.meta.uuid))

		return t.tbl.doCommit(ctx, t.meta.updates, t.reqs)
	}

	return t.tbl, nil
}
