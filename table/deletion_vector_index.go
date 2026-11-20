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
	"fmt"

	"github.com/apache/iceberg-go"
	iceberginternal "github.com/apache/iceberg-go/internal"
	tblutils "github.com/apache/iceberg-go/table/internal"
)

func dvPartitionsMatch(
	data, dv map[int]any,
	spec *iceberg.PartitionSpec,
	schema *iceberg.Schema,
) bool {
	if len(data) != len(dv) {
		return false
	}

	for partitionFieldID, dataValue := range data {
		dvValue, ok := dv[partitionFieldID]
		if !ok || !dvPartitionValuesEqual(partitionFieldID, dataValue, dvValue, spec, schema) {
			return false
		}
	}

	return true
}

func dvPartitionValuesEqual(
	partitionFieldID int,
	dataValue, dvValue any,
	spec *iceberg.PartitionSpec,
	schema *iceberg.Schema,
) bool {
	_, dataIsFloat32 := dataValue.(float32)
	_, dataIsFloat64 := dataValue.(float64)
	_, dvIsFloat32 := dvValue.(float32)
	_, dvIsFloat64 := dvValue.(float64)
	mixedFloatWidths := (dataIsFloat32 && dvIsFloat64) || (dataIsFloat64 && dvIsFloat32)
	if mixedFloatWidths && partitionFieldAllowsFloatWidening(partitionFieldID, spec, schema) {
		// Either manifest can retain the old width after schema evolution.
		return promotedPartitionValuesEqual(dataValue, dvValue)
	}

	return dvPartitionComparisonKey(dataValue) == dvPartitionComparisonKey(dvValue)
}

// Manifest decoders and int-to-long promotions can represent the same integer
// partition value with different Go types. Other values use the shared keys.
func dvPartitionComparisonKey(value any) any {
	switch value := value.(type) {
	case int:
		return int64(value)
	case int32:
		return int64(value)
	case iceberg.Date:
		return int64(value)
	case iceberg.Time:
		return int64(value)
	case iceberg.Timestamp:
		return int64(value)
	case iceberg.TimestampNano:
		return int64(value)
	default:
		return tblutils.ComparablePartitionKey(value)
	}
}

func partitionFieldAllowsFloatWidening(
	partitionFieldID int,
	spec *iceberg.PartitionSpec,
	schema *iceberg.Schema,
) bool {
	if spec == nil || schema == nil {
		return false
	}

	for i := range spec.NumFields() {
		field := spec.Field(i)
		if field.FieldID != partitionFieldID ||
			len(field.SourceIDs) != 1 ||
			!isIdentityPartitionTransform(field.Transform) {
			continue
		}

		sourceField, ok := schema.FindFieldByID(field.SourceIDs[0])
		// Historical partition specs can outlive their source column. If the
		// source still exists, require the current schema to show the allowed
		// float-to-double promotion. If it was dropped, the retained identity
		// field and exact widening comparison are the available type context.
		return !ok || sourceField.Type.Equals(iceberg.PrimitiveTypes.Float64)
	}

	return false
}

func isIdentityPartitionTransform(transform iceberg.Transform) bool {
	switch transform := transform.(type) {
	case iceberg.IdentityTransform:
		return true
	case *iceberg.IdentityTransform:
		return transform != nil
	default:
		return false
	}
}

// buildDVIndex indexes deletion vectors by the data file path they reference.
// A missing reference must fail planning instead of silently exposing deleted
// rows. The spec also requires at most one DV per data file.
func buildDVIndex(dvEntries []iceberg.ManifestEntry) (map[string]iceberg.ManifestEntry, error) {
	dvIndex := make(map[string]iceberg.ManifestEntry, len(dvEntries))
	for _, del := range dvEntries {
		file := del.DataFile()
		ref := iceberginternal.BorrowedDataFileReferencedDataFile(file)
		if ref == nil || *ref == "" {
			return nil, fmt.Errorf("%w: deletion vector %q is missing referenced_data_file",
				ErrInvalidMetadata, file.FilePath())
		}
		if _, exists := dvIndex[*ref]; exists {
			return nil, fmt.Errorf("%w: can't index multiple deletion vectors for %s", ErrInvalidMetadata, *ref)
		}
		dvIndex[*ref] = del
	}

	return dvIndex, nil
}

type dvSpecLookup func(int) (*iceberg.PartitionSpec, error)

// dvSpecLookupForMetadata adapts read-only metadata, where the historical
// spec may be absent. A rewrite can supply the builder's strict lookup.
func dvSpecLookupForMetadata(specs partitionSpecLookup) dvSpecLookup {
	if specs == nil {
		return nil
	}

	return func(id int) (*iceberg.PartitionSpec, error) {
		return specs.PartitionSpecByID(id), nil
	}
}

// matchDVToData returns the deletion vector that applies to the given data
// entry, if any. A DV applies when the data file's sequence number is less
// than or equal to the DV's sequence number.
//
// SequenceNum reports the -1 sentinel when an entry's sequence number is
// unset (see manifest.go). Entries arrive here already inherited, so a
// committed ADDED entry always carries a real (>= 0) sequence number; an
// unset value comes from an EXISTING/DELETED entry missing its required
// explicit sequence number, or a not-yet-committed manifest. Such an
// indeterminate sequence number — on either side — is treated as "applies":
// comparing a real data sequence number against an unset DV sequence (-1)
// would never satisfy dataSeq <= -1 and would silently drop the DV,
// resurfacing deleted rows; an unset data sequence likewise satisfies
// -1 <= dvSeq for any known DV sequence.
func matchDVToData(
	dataEntry iceberg.ManifestEntry,
	dvIndex map[string]iceberg.ManifestEntry,
	specByID dvSpecLookup,
	schema *iceberg.Schema,
) ([]iceberg.DataFile, error) {
	dataFile := dataEntry.DataFile()
	dvEntry, ok := dvIndex[dataFile.FilePath()]
	if !ok {
		return nil, nil
	}
	var spec *iceberg.PartitionSpec
	if specByID != nil {
		var err error
		spec, err = specByID(int(dataFile.SpecID()))
		if err != nil {
			return nil, fmt.Errorf("failed to get partition spec %d for data file %s: %w",
				dataFile.SpecID(), dataFile.FilePath(), err)
		}
	}
	if dvSeq := dvEntry.DataSequenceNumber(); dvSeq >= 0 && dataEntry.DataSequenceNumber() > dvSeq {
		return nil, nil
	}

	dvFile := dvEntry.DataFile()
	// A path reference alone does not make a DV applicable: the partition
	// spec and values must match the data file (Iceberg spec, Scan Planning).
	dvPartition := dataFilePartition(dvFile)
	dataPartition := dataFilePartition(dataFile)
	if dvFile.SpecID() != dataFile.SpecID() {
		return nil, fmt.Errorf("%w: deletion vector %s uses partition spec %d with partition %v, but data file %s uses partition spec %d with partition %v",
			ErrInvalidMetadata, dvFile.FilePath(), dvFile.SpecID(), dvPartition,
			dataFile.FilePath(), dataFile.SpecID(), dataPartition)
	}
	if !dvPartitionsMatch(dataPartition, dvPartition, spec, schema) {
		return nil, fmt.Errorf("%w: deletion vector %s has partition %v, which does not match data file %s partition %v",
			ErrInvalidMetadata, dvFile.FilePath(), dvPartition, dataFile.FilePath(), dataPartition)
	}

	return []iceberg.DataFile{dvFile}, nil
}
