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
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
)

type Operation string

const (
	OpAppend    Operation = "append"
	OpReplace   Operation = "replace"
	OpOverwrite Operation = "overwrite"
	OpDelete    Operation = "delete"
)

var (
	ErrInvalidOperation = errors.New("invalid operation value")
	ErrMissingOperation = errors.New("missing operation key")
)

// ValidOperation ensures that a given string is one of the valid operation
// types: append,replace,overwrite,delete
func ValidOperation(s string) (Operation, error) {
	switch s {
	case "append", "replace", "overwrite", "delete":
		return Operation(s), nil
	}

	return "", fmt.Errorf("%w: found '%s'", ErrInvalidOperation, s)
}

const (
	operationKey = "operation"

	addedDataFilesKey         = "added-data-files"
	addedDeleteFilesKey       = "added-delete-files"
	addedEqDeletesKey         = "added-equality-deletes"
	addedFileSizeKey          = "added-files-size"
	addedPosDeletesKey        = "added-position-deletes"
	addedPosDeleteFilesKey    = "added-position-delete-files"
	addedRecordsKey           = "added-records"
	addedEqDeleteFilesKey     = "added-equality-delete-files"
	deletedDataFilesKey       = "deleted-data-files"
	deletedRecordsKey         = "deleted-records"
	removedDeleteFilesKey     = "removed-delete-files"
	removedEqDeletesKey       = "removed-equality-deletes"
	removedEqDeleteFilesKey   = "removed-equality-delete-files"
	removedFileSizeKey        = "removed-files-size"
	removedPosDeletesKey      = "removed-position-deletes"
	removedPosDeleteFilesKey  = "removed-position-delete-files"
	totalEqDeletesKey         = "total-equality-deletes"
	totalPosDeletesKey        = "total-position-deletes"
	totalDataFilesKey         = "total-data-files"
	totalDeleteFilesKey       = "total-delete-files"
	totalRecordsKey           = "total-records"
	totalFileSizeKey          = "total-files-size"
	changedPartitionCountProp = "changed-partition-count"
	changedPartitionPrefix    = "partitions."
)

type updateMetrics struct {
	addedFileSize         int64
	removedFileSize       int64
	addedDataFiles        int64
	removedDataFiles      int64
	addedEqDeleteFiles    int64
	removedEqDeleteFiles  int64
	addedPosDeleteFiles   int64
	removedPosDeleteFiles int64
	addedDeleteFiles      int64
	removedDeleteFiles    int64
	addedRecords          int64
	deletedRecords        int64
	addedPosDeletes       int64
	removedPosDeletes     int64
	addedEqDeletes        int64
	removedEqDeletes      int64
}

func (m *updateMetrics) addDataFile(df iceberg.DataFile) error {
	m.addedFileSize += df.FileSizeBytes()
	switch df.ContentType() {
	case iceberg.EntryContentData:
		m.addedDataFiles++
		m.addedRecords += df.Count()
	case iceberg.EntryContentPosDeletes:
		m.addedDeleteFiles++
		m.addedPosDeleteFiles++
		m.addedPosDeletes += df.Count()
	case iceberg.EntryContentEqDeletes:
		m.addedDeleteFiles++
		m.addedEqDeleteFiles++
		m.addedEqDeletes += df.Count()
	default:
		return fmt.Errorf("unknown data file content: %s", df.ContentType())
	}

	return nil
}

func (m *updateMetrics) removeFile(df iceberg.DataFile) error {
	m.removedFileSize += df.FileSizeBytes()
	switch df.ContentType() {
	case iceberg.EntryContentData:
		m.removedDataFiles++
		m.deletedRecords += df.Count()
	case iceberg.EntryContentPosDeletes:
		m.removedDeleteFiles++
		m.removedPosDeleteFiles++
		m.removedPosDeletes += df.Count()
	case iceberg.EntryContentEqDeletes:
		m.removedDeleteFiles++
		m.removedEqDeleteFiles++
		m.removedEqDeletes += df.Count()
	default:
		return fmt.Errorf("unknown data file content: %s", df.ContentType())
	}

	return nil
}

func setWhenPositive(props iceberg.Properties, key string, value int64) {
	if value > 0 {
		props[key] = strconv.FormatInt(value, 10)
	}
}

func (m *updateMetrics) toProps() iceberg.Properties {
	props := iceberg.Properties{}
	setWhenPositive(props, addedFileSizeKey, m.addedFileSize)
	setWhenPositive(props, removedFileSizeKey, m.removedFileSize)
	setWhenPositive(props, addedDataFilesKey, m.addedDataFiles)
	setWhenPositive(props, deletedDataFilesKey, m.removedDataFiles)
	setWhenPositive(props, addedEqDeleteFilesKey, m.addedEqDeleteFiles)
	setWhenPositive(props, removedEqDeleteFilesKey, m.removedEqDeleteFiles)
	setWhenPositive(props, addedPosDeleteFilesKey, m.addedPosDeleteFiles)
	setWhenPositive(props, removedPosDeleteFilesKey, m.removedPosDeleteFiles)
	setWhenPositive(props, addedDeleteFilesKey, m.addedDeleteFiles)
	setWhenPositive(props, removedDeleteFilesKey, m.removedDeleteFiles)
	setWhenPositive(props, addedRecordsKey, m.addedRecords)
	setWhenPositive(props, deletedRecordsKey, m.deletedRecords)
	setWhenPositive(props, addedPosDeletesKey, m.addedPosDeletes)
	setWhenPositive(props, removedPosDeletesKey, m.removedPosDeletes)
	setWhenPositive(props, addedEqDeletesKey, m.addedEqDeletes)
	setWhenPositive(props, removedEqDeletesKey, m.removedEqDeletes)

	return props
}

// Summary stores the summary information for a snapshot indicating
// the operation that created the snapshot, and various properties
// which might exist in the summary.
type Summary struct {
	Operation  Operation
	Properties iceberg.Properties
}

func (s *Summary) String() string {
	out := string(s.Operation)
	if s.Properties != nil {
		data, _ := json.Marshal(s.Properties)
		out += ", " + string(data)
	}

	return out
}

func (s *Summary) Equals(other *Summary) bool {
	if s == other {
		return true
	}

	if s != nil && other == nil {
		return false
	}

	if s.Operation != other.Operation {
		return false
	}

	if len(s.Properties) == 0 && len(other.Properties) == 0 {
		return true
	}

	return maps.Equal(s.Properties, other.Properties)
}

func (s *Summary) UnmarshalJSON(b []byte) (err error) {
	alias := map[string]string{}
	if err = json.Unmarshal(b, &alias); err != nil {
		return err
	}

	op, ok := alias[operationKey]
	if !ok {
		return ErrMissingOperation
	}

	if s.Operation, err = ValidOperation(op); err != nil {
		return err
	}

	delete(alias, operationKey)
	s.Properties = alias

	return nil
}

func (s *Summary) MarshalJSON() ([]byte, error) {
	props := maps.Clone(s.Properties)
	if s.Operation != "" {
		if props == nil {
			props = make(map[string]string)
		}
		props[operationKey] = string(s.Operation)
	}

	return json.Marshal(props)
}

type Snapshot struct {
	SnapshotID       int64    `json:"snapshot-id"`
	ParentSnapshotID *int64   `json:"parent-snapshot-id,omitempty"`
	SequenceNumber   int64    `json:"sequence-number"`
	TimestampMs      int64    `json:"timestamp-ms"`
	ManifestList     string   `json:"manifest-list,omitempty"`
	Summary          *Summary `json:"summary,omitempty"`
	SchemaID         *int     `json:"schema-id,omitempty"`
}

func (s Snapshot) String() string {
	var op, parent, schema string

	if s.Summary != nil {
		op = s.Summary.String() + ": "
	}
	if s.ParentSnapshotID != nil {
		parent = ", parent_id=" + strconv.FormatInt(*s.ParentSnapshotID, 10)
	}
	if s.SchemaID != nil {
		schema = ", schema_id=" + strconv.Itoa(*s.SchemaID)
	}

	return fmt.Sprintf("%sid=%d%s%s, sequence_number=%d, timestamp_ms=%d, manifest_list=%s",
		op, s.SnapshotID, parent, schema, s.SequenceNumber, s.TimestampMs, s.ManifestList)
}

func (s Snapshot) Equals(other Snapshot) bool {
	switch {
	case s.ParentSnapshotID == nil && other.ParentSnapshotID != nil:
		fallthrough
	case s.ParentSnapshotID != nil && other.ParentSnapshotID == nil:
		fallthrough
	case s.SchemaID == nil && other.SchemaID != nil:
		fallthrough
	case s.SchemaID != nil && other.SchemaID == nil:
		return false
	}

	return s.SnapshotID == other.SnapshotID &&
		((s.ParentSnapshotID == other.ParentSnapshotID) || (*s.ParentSnapshotID == *other.ParentSnapshotID)) &&
		((s.SchemaID == other.SchemaID) || (*s.SchemaID == *other.SchemaID)) &&
		s.SequenceNumber == other.SequenceNumber &&
		s.TimestampMs == other.TimestampMs &&
		s.ManifestList == other.ManifestList &&
		s.Summary.Equals(other.Summary)
}

func (s Snapshot) Manifests(fio iceio.IO) (_ []iceberg.ManifestFile, err error) {
	if s.ManifestList != "" {
		f, err := fio.Open(s.ManifestList)
		if err != nil {
			return nil, fmt.Errorf("could not open manifest file: %w", err)
		}
		defer internal.CheckedClose(f, &err)

		return iceberg.ReadManifestList(f)
	}

	return nil, nil
}

func (s Snapshot) dataFiles(fio iceio.IO, fileFilter set[iceberg.ManifestEntryContent]) iter.Seq2[iceberg.DataFile, error] {
	return func(yield func(iceberg.DataFile, error) bool) {
		manifests, err := s.Manifests(fio)
		if err != nil {
			yield(nil, err)

			return
		}

		for _, m := range manifests {
			dataFiles, err := m.FetchEntries(fio, false)
			if err != nil {
				yield(nil, err)

				return
			}

			for _, f := range dataFiles {
				if fileFilter != nil {
					if _, ok := fileFilter[f.DataFile().ContentType()]; !ok {
						continue
					}
				}
				if !yield(f.DataFile(), nil) {
					return
				}
			}
		}
	}
}

type MetadataLogEntry struct {
	MetadataFile string `json:"metadata-file"`
	TimestampMs  int64  `json:"timestamp-ms"`
}

type SnapshotLogEntry struct {
	SnapshotID  int64 `json:"snapshot-id"`
	TimestampMs int64 `json:"timestamp-ms"`
}

type SnapshotSummaryCollector struct {
	metrics                          updateMetrics
	partitionMetrics                 map[string]updateMetrics
	maxChangedPartitionsForSummaries int
}

func (s *SnapshotSummaryCollector) setPartitionSummaryLimit(limit int) {
	s.maxChangedPartitionsForSummaries = limit
}

func (s *SnapshotSummaryCollector) updatePartitionMetrics(partitionPath string, df iceberg.DataFile, isAddFile bool) error {
	if s.partitionMetrics == nil {
		s.partitionMetrics = make(map[string]updateMetrics)
	}

	metrics := s.partitionMetrics[partitionPath]
	if isAddFile {
		if err := metrics.addDataFile(df); err != nil {
			return err
		}
	} else {
		if err := metrics.removeFile(df); err != nil {
			return err
		}
	}
	s.partitionMetrics[partitionPath] = metrics

	return nil
}

func (s *SnapshotSummaryCollector) addFile(df iceberg.DataFile, sc *iceberg.Schema, spec iceberg.PartitionSpec) error {
	if err := s.metrics.addDataFile(df); err != nil {
		return err
	}

	if len(df.Partition()) > 0 {
		partitionPath := spec.PartitionToPath(
			getPartitionRecord(df, spec.PartitionType(sc)), sc)

		return s.updatePartitionMetrics(partitionPath, df, true)
	}

	return nil
}

func (s *SnapshotSummaryCollector) removeFile(df iceberg.DataFile, sc *iceberg.Schema, spec iceberg.PartitionSpec) error {
	if err := s.metrics.removeFile(df); err != nil {
		return err
	}

	if len(df.Partition()) > 0 {
		partitionPath := spec.PartitionToPath(
			getPartitionRecord(df, spec.PartitionType(sc)), sc)

		return s.updatePartitionMetrics(partitionPath, df, false)
	}

	return nil
}

func (s *SnapshotSummaryCollector) partitionSummary(metrics *updateMetrics) string {
	props := metrics.toProps()

	return strings.Join(slices.Sorted(func(yield func(s string) bool) {
		for k, v := range props {
			if !yield(fmt.Sprintf("%s=%s", k, v)) {
				return
			}
		}
	}), ",")
}

func (s *SnapshotSummaryCollector) build() iceberg.Properties {
	props := s.metrics.toProps()
	changedPartitionsSize := len(s.partitionMetrics)
	setWhenPositive(props, changedPartitionCountProp, int64(changedPartitionsSize))
	if changedPartitionsSize <= s.maxChangedPartitionsForSummaries {
		for partPath, updateMetricsPart := range s.partitionMetrics {
			if summary := s.partitionSummary(&updateMetricsPart); len(summary) > 0 {
				props[changedPartitionPrefix+partPath] = summary
			}
		}
	}

	return props
}

func updateSnapshotSummaries(sum Summary, previous iceberg.Properties) (Summary, error) {
	switch sum.Operation {
	case OpAppend, OpOverwrite, OpDelete:
	default:
		return sum, fmt.Errorf("%w: operation: %s", iceberg.ErrNotImplemented, sum.Operation)
	}

	if sum.Properties == nil {
		sum.Properties = make(iceberg.Properties)
	}

	if previous == nil {
		previous = iceberg.Properties{
			totalDataFilesKey:   "0",
			totalDeleteFilesKey: "0",
			totalRecordsKey:     "0",
			totalFileSizeKey:    "0",
			totalPosDeletesKey:  "0",
			totalEqDeletesKey:   "0",
		}
	}

	updateTotals := func(totalProp, addedProp, removedProp string) {
		newTotal := previous.GetInt(totalProp, 0)
		newTotal += sum.Properties.GetInt(addedProp, 0)
		newTotal -= sum.Properties.GetInt(removedProp, 0)

		if newTotal >= 0 {
			sum.Properties[totalProp] = strconv.Itoa(newTotal)
		}
	}

	updateTotals(totalDataFilesKey, addedDataFilesKey, deletedDataFilesKey)
	updateTotals(totalDeleteFilesKey, addedDeleteFilesKey, removedDeleteFilesKey)
	updateTotals(totalRecordsKey, addedRecordsKey, deletedRecordsKey)
	updateTotals(totalFileSizeKey, addedFileSizeKey, removedFileSizeKey)
	updateTotals(totalPosDeletesKey, addedPosDeletesKey, removedPosDeletesKey)
	updateTotals(totalEqDeletesKey, addedEqDeletesKey, removedEqDeletesKey)

	return sum, nil
}
