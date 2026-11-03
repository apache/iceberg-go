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
	"fmt"
	"strconv"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/twmb/avro/ocf"

	"github.com/apache/iceberg-go"
)

// SnapshotValidationIssueCode describes a violation of the Iceberg snapshot
// manifest and content-file invariants.
type SnapshotValidationIssueCode string

const (
	SnapshotIssueDuplicateManifest           SnapshotValidationIssueCode = "duplicate-manifest"
	SnapshotIssueDuplicateLiveFile           SnapshotValidationIssueCode = "duplicate-live-file"
	SnapshotIssueDuplicateDeletionVector     SnapshotValidationIssueCode = "duplicate-deletion-vector"
	SnapshotIssueMissingDeletionVectorTarget SnapshotValidationIssueCode = "missing-deletion-vector-target"
	SnapshotIssueInvalidDeletionVectorRange  SnapshotValidationIssueCode = "invalid-deletion-vector-range"
	SnapshotIssueUnknownPartitionSpec        SnapshotValidationIssueCode = "unknown-partition-spec"
	SnapshotIssueIncompatibleManifestVersion SnapshotValidationIssueCode = "incompatible-manifest-version"
	SnapshotIssueInvalidManifestContent      SnapshotValidationIssueCode = "invalid-manifest-content"
	SnapshotIssueInvalidFileContent          SnapshotValidationIssueCode = "invalid-file-content"
	SnapshotIssueInvalidFileFormat           SnapshotValidationIssueCode = "invalid-file-format"
	SnapshotIssueEmptyManifestPath           SnapshotValidationIssueCode = "empty-manifest-path"
	SnapshotIssueEmptyFilePath               SnapshotValidationIssueCode = "empty-file-path"
	SnapshotIssueManifestCountMismatch       SnapshotValidationIssueCode = "manifest-count-mismatch"
)

// SnapshotValidationIssue identifies a manifest-level or live-file-level
// problem. FirstManifestPath is populated for duplicate paths. Detail gives
// additional context such as the mismatched manifest count.
type SnapshotValidationIssue struct {
	Code              SnapshotValidationIssueCode
	ManifestPath      string
	FilePath          string
	FirstManifestPath string
	PartitionSpecID   int32
	Detail            string
}

// SnapshotValidationReport describes the metadata inspected for one snapshot.
// The audit does not read data-file contents or verify their storage existence.
type SnapshotValidationReport struct {
	SnapshotID       int64
	ManifestsChecked int64
	LiveFilesChecked int64
	Issues           []SnapshotValidationIssue
	Complete         bool
}

// Valid is true only after the entire audit completes without issues.
func (r SnapshotValidationReport) Valid() bool {
	return r.Complete && len(r.Issues) == 0
}

// ValidateSnapshot audits the manifest list and manifest entries of a snapshot.
//
// It checks that the manifest list references known partition specs, that
// manifest-list descriptors and file contents are valid for the table's format version, and
// that a content file has at most one live reference across all manifests.
// It also checks that each live deletion vector has a target path and that no data
// file has more than one live deletion vector.
// Deleted entries do not count as live references. Manifest entry counts are
// checked only when the manifest list provides them (v1 may omit them).
// For embedded v1 manifests, missing header spec IDs are inferred only when
// the partition fields match exactly one known partition spec.
//
// The snapshot is never modified. I/O errors and cancellation return the
// partial report with Complete=false and a non-nil error. Cancellation is
// checked between reads; in-progress reads may not be interruptible. The audit
// reads projected manifest metadata without column statistics and does not
// read or stat data files.
//
// Spec: https://iceberg.apache.org/spec/#content-file-uniqueness
// Spec: https://iceberg.apache.org/spec/#manifest-lists
func (i InspectTable) ValidateSnapshot(ctx context.Context, snapshotID int64) (*SnapshotValidationReport, error) {
	t := i.tbl
	if t.metadata == nil {
		return nil, fmt.Errorf("%w: table metadata is nil", ErrInvalidMetadata)
	}

	snapshot := t.metadata.SnapshotByID(snapshotID)
	if snapshot == nil {
		return nil, fmt.Errorf("%w: snapshot %d not found", ErrSnapshotNotFound, snapshotID)
	}
	report := &SnapshotValidationReport{SnapshotID: snapshotID}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	fsF := sharedSnapshotManifestFSF(t.fsF)
	fs, err := fsF(ctx)
	if err != nil {
		return report, fmt.Errorf("load snapshot %d FileIO: %w", snapshotID, err)
	}
	if fs == nil {
		return report, fmt.Errorf("snapshot %d FileIO is nil", snapshotID)
	}

	manifestSet, err := t.manifestSetWithFSF(ctx, *snapshot, fsF)
	if err != nil {
		return report, fmt.Errorf("read snapshot %d manifest list: %w", snapshotID, err)
	}
	manifests := manifestSet.borrowAllManifests()
	if err := ctx.Err(); err != nil {
		return report, err
	}

	specs := t.metadata.PartitionSpecs()
	specIDs := make(map[int]struct{}, len(specs))
	for _, spec := range specs {
		specIDs[spec.ID()] = struct{}{}
	}
	version := t.metadata.FormatVersion()
	seenManifests := make(map[string]struct{}, len(manifests))
	// The map intentionally tracks every live content file, regardless of its
	// partition spec, entry status (ADDED/EXISTING), or file format.
	seenFiles := make(map[deletionVectorBlobKey]string)
	seenVectorTargets := make(map[string]snapshotValidationVector)

	for _, manifest := range manifests {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		report.ManifestsChecked++
		if manifest == nil {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueEmptyManifestPath, Detail: "manifest entry is nil",
			})

			continue
		}
		path := manifest.FilePath()
		if path == "" {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueEmptyManifestPath,
			})

			continue
		}
		if _, exists := seenManifests[path]; exists {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueDuplicateManifest, ManifestPath: path,
			})
		}
		seenManifests[path] = struct{}{}

		if snapshot.ManifestList == "" && snapshot.ManifestLocations != nil {
			specID, known, err := embeddedValidationSpecID(ctx, fs, path, specs)
			if err != nil {
				return report, fmt.Errorf("read snapshot %d manifest %q header: %w", snapshotID, path, err)
			}
			if known {
				validateSnapshotSpec(report, path, specID, specIDs)
			}
		} else {
			validateSnapshotSpec(report, path, manifest.PartitionSpecID(), specIDs)
		}
		validateSnapshotManifestDescriptor(report, manifest, version)

		var added, existing, deleted int64
		for entry, entryErr := range iceberg.EntriesWithProjection(fs, manifest, false, iceberg.ManifestEntryProjection{}) {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			if entryErr != nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: %w", snapshotID, path, entryErr)
			}
			if entry == nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: nil entry", snapshotID, path)
			}

			switch entry.Status() {
			case iceberg.EntryStatusDELETED:
				deleted++
			case iceberg.EntryStatusADDED:
				added++
			case iceberg.EntryStatusEXISTING:
				existing++
			default:
				return report, fmt.Errorf("read snapshot %d manifest %q: unexpected entry status %d",
					snapshotID, path, entry.Status())
			}

			if entry.Status() != iceberg.EntryStatusDELETED {
				report.LiveFilesChecked++
			}
			file := entry.DataFile()
			if file == nil {
				return report, fmt.Errorf("read snapshot %d manifest %q: nil data file", snapshotID, path)
			}
			filePath := file.FilePath()
			if filePath == "" {
				report.Issues = append(report.Issues, SnapshotValidationIssue{
					Code: SnapshotIssueEmptyFilePath, ManifestPath: path,
				})

				continue
			}
			validateSnapshotFile(report, version, manifest, file)
			// Tombstones still have to obey the manifest's content and file-format
			// constraints, but they are not live references for uniqueness checks.
			if entry.Status() == iceberg.EntryStatusDELETED {
				continue
			}
			key := deletionVectorBlobKey{path: filePath}
			validVectorRange := true
			if IsDeletionVector(file) {
				offset, length := file.ContentOffset(), file.ContentSizeInBytes()
				if offset == nil || length == nil || *offset < 0 || *length <= 0 {
					validVectorRange = false
					report.Issues = append(report.Issues, SnapshotValidationIssue{
						Code: SnapshotIssueInvalidDeletionVectorRange, ManifestPath: path, FilePath: filePath,
						Detail: "content_offset must be non-negative and content_size_in_bytes must be positive",
					})
				} else {
					key.offset, key.length = *offset, *length
				}
				target := file.ReferencedDataFile()
				if target == nil || *target == "" {
					report.Issues = append(report.Issues, SnapshotValidationIssue{
						Code: SnapshotIssueMissingDeletionVectorTarget, ManifestPath: path, FilePath: filePath,
					})
				} else if validVectorRange {
					if previous, exists := seenVectorTargets[*target]; exists {
						// A repeated reference to the same blob is already reported by
						// the content-file uniqueness check below.
						if previous.key != key {
							report.Issues = append(report.Issues, SnapshotValidationIssue{
								Code: SnapshotIssueDuplicateDeletionVector, ManifestPath: path,
								FirstManifestPath: previous.manifest, FilePath: filePath, Detail: *target,
							})
						}
					} else {
						seenVectorTargets[*target] = snapshotValidationVector{key: key, manifest: path}
					}
				}
			}
			// An invalid blob location cannot safely participate in uniqueness
			// checks. Keep auditing later entries instead of failing the scan.
			if !validVectorRange {
				continue
			}
			if firstManifest, exists := seenFiles[key]; exists {
				report.Issues = append(report.Issues, SnapshotValidationIssue{
					Code: SnapshotIssueDuplicateLiveFile, ManifestPath: path,
					FirstManifestPath: firstManifest, FilePath: filePath,
				})
			} else {
				seenFiles[key] = path
			}
		}
		validateManifestEntryCount(report, path, "added", manifest.AddedDataFiles(), added, manifest.Version())
		validateManifestEntryCount(report, path, "existing", manifest.ExistingDataFiles(), existing, manifest.Version())
		validateManifestEntryCount(report, path, "deleted", manifest.DeletedDataFiles(), deleted, manifest.Version())
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	report.Complete = true

	return report, nil
}

type snapshotValidationVector struct {
	key      deletionVectorBlobKey
	manifest string
}

// Older v1 embedded snapshots have no manifest-list descriptor. Prefer their
// optional header spec ID, or infer one only when the required partition-spec
// fields identify exactly one spec in the table metadata.
func embeddedValidationSpecID(
	ctx context.Context, fs iceio.IO, path string, specs []iceberg.PartitionSpec,
) (int32, bool, error) {
	f, err := fs.Open(ctx, path)
	if err != nil {
		return 0, false, err
	}
	defer f.Close()
	reader, err := ocf.NewReader(f)
	if err != nil {
		return 0, false, err
	}
	defer reader.Close()
	raw, present := reader.Metadata()["partition-spec-id"]
	if present {
		id, err := strconv.ParseInt(string(raw), 10, 32)
		if err != nil || id < 0 {
			return 0, false, fmt.Errorf("invalid partition-spec-id %q", raw)
		}

		return int32(id), true, nil
	}

	var fields []iceberg.PartitionField
	if err := json.Unmarshal(reader.Metadata()["partition-spec"], &fields); err != nil {
		return 0, false, fmt.Errorf("invalid v1 partition-spec: %w", err)
	}
	// Legacy v1 headers may omit field-id, with sequential IDs from 1000.
	for i := range fields {
		if fields[i].FieldID == 0 {
			fields[i].FieldID = iceberg.PartitionDataIDStart + i
		}
	}
	var matched int32
	found := false
	for _, candidate := range specs {
		actual := iceberg.NewPartitionSpecID(candidate.ID(), fields...)
		if !actual.Equals(candidate) {
			continue
		}
		// Equivalent historical specs cannot be distinguished safely.
		if found {
			return 0, false, nil
		}
		matched, found = int32(candidate.ID()), true
	}
	// A missing ID is legal in v1. When no spec matches, leave the
	// partition-spec ID inconclusive rather than guessing from spec 0.
	return matched, found, nil
}

func validateSnapshotSpec(report *SnapshotValidationReport, path string, specID int32, specIDs map[int]struct{}) {
	if _, found := specIDs[int(specID)]; !found {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueUnknownPartitionSpec, ManifestPath: path, PartitionSpecID: specID,
		})
	}
}

func validateSnapshotManifestDescriptor(
	report *SnapshotValidationReport,
	manifest iceberg.ManifestFile,
	version int,
) {
	path := manifest.FilePath()
	if manifest.Version() < 1 || manifest.Version() > version {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueIncompatibleManifestVersion, ManifestPath: path,
			Detail: fmt.Sprintf("manifest v%d in table v%d", manifest.Version(), version),
		})
	}
	content := manifest.ManifestContent()
	if content != iceberg.ManifestContentData &&
		(version == 1 || content != iceberg.ManifestContentDeletes) {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidManifestContent, ManifestPath: path,
			Detail: fmt.Sprintf("manifest content %s in table v%d", content, version),
		})
	}
}

func validateSnapshotFile(
	report *SnapshotValidationReport,
	version int,
	manifest iceberg.ManifestFile,
	file iceberg.DataFile,
) {
	path := manifest.FilePath()
	content := file.ContentType()
	manifestContent := manifest.ManifestContent()
	if (manifestContent == iceberg.ManifestContentData && content != iceberg.EntryContentData) ||
		(manifestContent == iceberg.ManifestContentDeletes &&
			content != iceberg.EntryContentPosDeletes && content != iceberg.EntryContentEqDeletes) ||
		(version == 1 && content != iceberg.EntryContentData) {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidFileContent, ManifestPath: path,
			FilePath: file.FilePath(),
			Detail:   fmt.Sprintf("file content %s in manifest %s", content, manifestContent),
		})
	}

	switch file.FileFormat() {
	case iceberg.AvroFile, iceberg.OrcFile, iceberg.ParquetFile:
	case iceberg.PuffinFile:
		if version < 3 || content != iceberg.EntryContentPosDeletes {
			report.Issues = append(report.Issues, SnapshotValidationIssue{
				Code: SnapshotIssueInvalidFileFormat, ManifestPath: path, FilePath: file.FilePath(),
				Detail: "Puffin is only valid for positional deletes in v3",
			})
		}
	default:
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueInvalidFileFormat, ManifestPath: path, FilePath: file.FilePath(),
			Detail: fmt.Sprintf("unknown file format %q", file.FileFormat()),
		})
	}
}

func validateManifestEntryCount(
	report *SnapshotValidationReport, manifestPath, kind string, expected int32, actual int64, version int,
) {
	// Only v1 manifest lists permit omitted counts. Decoders represent null
	// with -1. Negative counts in v2/v3 (and values below -1 in v1) are invalid.
	if expected == -1 && version == 1 {
		return
	}
	if expected < 0 {
		report.Issues = append(report.Issues, SnapshotValidationIssue{
			Code: SnapshotIssueManifestCountMismatch, ManifestPath: manifestPath,
			Detail: fmt.Sprintf("%s entries: invalid count %d in v%d manifest list", kind, expected, version),
		})

		return
	}
	if int64(expected) == actual {
		return
	}
	report.Issues = append(report.Issues, SnapshotValidationIssue{
		Code: SnapshotIssueManifestCountMismatch, ManifestPath: manifestPath,
		Detail: fmt.Sprintf("%s entries: manifest list reports %d, found %d", kind, expected, actual),
	})
}
