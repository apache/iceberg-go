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
	"errors"
	"fmt"
	"slices"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/io"
	"github.com/google/uuid"
)

var (
	ErrNoManifestsToRewrite = errors.New("no manifests selected for rewriting")
	ErrManifestRewriteFailed = errors.New("manifest rewrite operation failed")
)

// ManifestRewriteStrategy defines how manifests should be selected and rewritten
type ManifestRewriteStrategy int

const (
	// RewriteSmallManifests rewrites manifests that are smaller than a target size
	RewriteSmallManifests ManifestRewriteStrategy = iota
	// RewriteAllManifests rewrites all manifests regardless of size
	RewriteAllManifests
	// RewriteByCount rewrites manifests when there are too many small ones
	RewriteByCount
)

// ManifestRewriteOptions contains configuration for manifest rewriting operations
type ManifestRewriteOptions struct {
	// Strategy determines how manifests are selected for rewriting
	Strategy ManifestRewriteStrategy
	// TargetSizeBytes is the target size for new manifests
	TargetSizeBytes int64
	// MinCountToRewrite is minimum number of manifests to trigger rewriting
	MinCountToRewrite int
	// MaxManifestCount is the maximum number of manifests after rewriting
	MaxManifestCount int
	// SnapshotID specifies which snapshot's manifests to rewrite (0 for current)
	SnapshotID int64
}

// DefaultManifestRewriteOptions returns sensible defaults for manifest rewriting
func DefaultManifestRewriteOptions() ManifestRewriteOptions {
	return ManifestRewriteOptions{
		Strategy:          RewriteSmallManifests,
		TargetSizeBytes:   ManifestTargetSizeBytesDefault,
		MinCountToRewrite: ManifestMinMergeCountDefault,
		MaxManifestCount:  100, // reasonable default
		SnapshotID:        0,   // current snapshot
	}
}

// RewriteManifests represents the interface for rewriting manifest files in a table
// This is equivalent to the Java org.apache.iceberg.RewriteManifests interface.
type RewriteManifests interface {
	// RewriteManifests rewrites manifest files according to the specified options
	RewriteManifests(ctx context.Context, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
	
	// RewriteManifestsBySpec rewrites manifests for specific partition specs
	RewriteManifestsBySpec(ctx context.Context, specIDs []int, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
	
	// RewriteManifestsByPredicate rewrites manifests that match a given predicate
	RewriteManifestsByPredicate(ctx context.Context, predicate func(iceberg.ManifestFile) bool, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error
}

// BaseRewriteManifests implements the RewriteManifests interface with flexible configuration.
// This is equivalent to the Java org.apache.iceberg.BaseRewriteManifests class.
type BaseRewriteManifests struct {
	txn               *Transaction
	conflictDetection bool
	caseSensitive     bool
	targetSizeBytes   int64
	minCountToRewrite int
}

// NewRewriteManifests creates a new BaseRewriteManifests instance with default settings
func NewRewriteManifests(txn *Transaction) *BaseRewriteManifests {
	props := txn.meta.props
	return &BaseRewriteManifests{
		txn:               txn,
		conflictDetection: true,
		caseSensitive:     true,
		targetSizeBytes:   int64(props.GetInt(ManifestTargetSizeBytesKey, ManifestTargetSizeBytesDefault)),
		minCountToRewrite: props.GetInt(ManifestMinMergeCountKey, ManifestMinMergeCountDefault),
	}
}

// WithConflictDetection enables or disables conflict detection
func (rm *BaseRewriteManifests) WithConflictDetection(enabled bool) *BaseRewriteManifests {
	rm.conflictDetection = enabled
	return rm
}

// WithCaseSensitive sets case sensitivity for column name resolution
func (rm *BaseRewriteManifests) WithCaseSensitive(caseSensitive bool) *BaseRewriteManifests {
	rm.caseSensitive = caseSensitive
	return rm
}

// WithTargetSizeBytes sets the target size for new manifests
func (rm *BaseRewriteManifests) WithTargetSizeBytes(size int64) *BaseRewriteManifests {
	rm.targetSizeBytes = size
	return rm
}

// WithMinCountToRewrite sets the minimum count of manifests to trigger rewriting
func (rm *BaseRewriteManifests) WithMinCountToRewrite(count int) *BaseRewriteManifests {
	rm.minCountToRewrite = count
	return rm
}

// RewriteManifests rewrites manifest files according to the specified options
func (rm *BaseRewriteManifests) RewriteManifests(ctx context.Context, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error {
	// Get the target snapshot
	var targetSnapshot *Snapshot
	if options.SnapshotID == 0 {
		targetSnapshot = rm.txn.meta.currentSnapshot()
	} else {
		var err error
		targetSnapshot, err = rm.txn.meta.SnapshotByID(options.SnapshotID)
		if err != nil {
			return err
		}
	}
	
	if targetSnapshot == nil {
		return fmt.Errorf("snapshot not found: %d", options.SnapshotID)
	}

	// Get filesystem
	fs, err := rm.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Get all manifests from the target snapshot
	manifests, err := targetSnapshot.Manifests(fs)
	if err != nil {
		return err
	}

	// Apply strategy to select manifests for rewriting
	manifestsToRewrite, err := rm.selectManifestsForRewriting(manifests, options)
	if err != nil {
		return err
	}

	if len(manifestsToRewrite) == 0 {
		return ErrNoManifestsToRewrite
	}

	// Perform the rewrite operation
	return rm.performManifestRewrite(ctx, targetSnapshot, manifestsToRewrite, options, snapshotProps)
}

// RewriteManifestsBySpec rewrites manifests for specific partition specs
func (rm *BaseRewriteManifests) RewriteManifestsBySpec(ctx context.Context, specIDs []int, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error {
	predicate := func(mf iceberg.ManifestFile) bool {
		return slices.Contains(specIDs, int(mf.PartitionSpecID()))
	}
	
	return rm.RewriteManifestsByPredicate(ctx, predicate, options, snapshotProps)
}

// RewriteManifestsByPredicate rewrites manifests that match a given predicate
func (rm *BaseRewriteManifests) RewriteManifestsByPredicate(ctx context.Context, predicate func(iceberg.ManifestFile) bool, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error {
	// Get the target snapshot
	var targetSnapshot *Snapshot
	if options.SnapshotID == 0 {
		targetSnapshot = rm.txn.meta.currentSnapshot()
	} else {
		var err error
		targetSnapshot, err = rm.txn.meta.SnapshotByID(options.SnapshotID)
		if err != nil {
			return err
		}
	}
	
	if targetSnapshot == nil {
		return fmt.Errorf("snapshot not found: %d", options.SnapshotID)
	}

	// Get filesystem
	fs, err := rm.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Get all manifests from the target snapshot
	manifests, err := targetSnapshot.Manifests(fs)
	if err != nil {
		return err
	}

	// Filter manifests by predicate
	var filteredManifests []iceberg.ManifestFile
	for _, mf := range manifests {
		if predicate(mf) {
			filteredManifests = append(filteredManifests, mf)
		}
	}

	// Apply strategy to select manifests for rewriting
	manifestsToRewrite, err := rm.selectManifestsForRewriting(filteredManifests, options)
	if err != nil {
		return err
	}

	if len(manifestsToRewrite) == 0 {
		return ErrNoManifestsToRewrite
	}

	// Perform the rewrite operation
	return rm.performManifestRewrite(ctx, targetSnapshot, manifestsToRewrite, options, snapshotProps)
}

// selectManifestsForRewriting applies the strategy to select which manifests should be rewritten
func (rm *BaseRewriteManifests) selectManifestsForRewriting(manifests []iceberg.ManifestFile, options ManifestRewriteOptions) ([]iceberg.ManifestFile, error) {
	switch options.Strategy {
	case RewriteSmallManifests:
		return rm.selectSmallManifests(manifests, options.TargetSizeBytes), nil
	case RewriteAllManifests:
		return manifests, nil
	case RewriteByCount:
		return rm.selectByCount(manifests, options), nil
	default:
		return nil, fmt.Errorf("unknown rewrite strategy: %d", options.Strategy)
	}
}

// selectSmallManifests selects manifests that are smaller than the target size
func (rm *BaseRewriteManifests) selectSmallManifests(manifests []iceberg.ManifestFile, targetSize int64) []iceberg.ManifestFile {
	var selected []iceberg.ManifestFile
	for _, mf := range manifests {
		if mf.Length() < targetSize {
			selected = append(selected, mf)
		}
	}
	return selected
}

// selectByCount selects manifests based on count thresholds
func (rm *BaseRewriteManifests) selectByCount(manifests []iceberg.ManifestFile, options ManifestRewriteOptions) []iceberg.ManifestFile {
	if len(manifests) < options.MinCountToRewrite {
		return nil // not enough manifests to trigger rewriting
	}
	
	// Group by partition spec to handle them separately
	specGroups := make(map[int][]iceberg.ManifestFile)
	for _, mf := range manifests {
		specID := int(mf.PartitionSpecID())
		specGroups[specID] = append(specGroups[specID], mf)
	}
	
	var selected []iceberg.ManifestFile
	for _, group := range specGroups {
		if len(group) >= options.MinCountToRewrite {
			// Select small manifests from this group
			smallManifests := rm.selectSmallManifests(group, options.TargetSizeBytes)
			selected = append(selected, smallManifests...)
		}
	}
	
	return selected
}

// performManifestRewrite executes the actual manifest rewriting operation
func (rm *BaseRewriteManifests) performManifestRewrite(ctx context.Context, targetSnapshot *Snapshot, manifestsToRewrite []iceberg.ManifestFile, options ManifestRewriteOptions, snapshotProps iceberg.Properties) error {
	fs, err := rm.txn.tbl.fsF(ctx)
	if err != nil {
		return err
	}

	// Create manifest rewrite snapshot producer
	rewriteProducer := rm.newManifestRewriteSnapshotProducer(fs, snapshotProps, manifestsToRewrite, options)

	// Commit the operation
	updates, reqs, err := rewriteProducer.commit()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrManifestRewriteFailed, err)
	}

	return rm.txn.apply(updates, reqs)
}

// newManifestRewriteSnapshotProducer creates a snapshot producer for manifest rewriting
func (rm *BaseRewriteManifests) newManifestRewriteSnapshotProducer(fs io.IO, snapshotProps iceberg.Properties, manifestsToRewrite []iceberg.ManifestFile, options ManifestRewriteOptions) *snapshotProducer {
	writeFS := fs.(io.WriteFileIO)
	commitUUID := uuid.New()
	
	prod := createSnapshotProducer(OpReplace, rm.txn, writeFS, &commitUUID, snapshotProps)
	prod.producerImpl = &manifestRewriteFiles{
		base:              prod,
		manifestsToRewrite: manifestsToRewrite,
		options:           options,
	}

	return prod
} 

// manifestRewriteFiles implements the producerImpl interface for manifest rewriting operations
type manifestRewriteFiles struct {
	base               *snapshotProducer
	manifestsToRewrite []iceberg.ManifestFile
	options            ManifestRewriteOptions
}

func (mrf *manifestRewriteFiles) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	// Group manifests by partition spec ID for rewriting
	manifestsBySpec := make(map[int][]iceberg.ManifestFile)
	manifestsToKeep := make([]iceberg.ManifestFile, 0)
	
	// Separate manifests to rewrite from those to keep
	rewriteSet := make(map[string]bool)
	for _, mf := range mrf.manifestsToRewrite {
		rewriteSet[mf.FilePath()] = true
	}
	
	for _, mf := range manifests {
		if rewriteSet[mf.FilePath()] {
			specID := int(mf.PartitionSpecID())
			manifestsBySpec[specID] = append(manifestsBySpec[specID], mf)
		} else {
			manifestsToKeep = append(manifestsToKeep, mf)
		}
	}
	
	// Rewrite each group of manifests by spec
	for specID, specManifests := range manifestsBySpec {
		rewrittenManifests, err := mrf.rewriteManifestsForSpec(specID, specManifests)
		if err != nil {
			return nil, err
		}
		manifestsToKeep = append(manifestsToKeep, rewrittenManifests...)
	}
	
	return manifestsToKeep, nil
}

func (mrf *manifestRewriteFiles) existingManifests() ([]iceberg.ManifestFile, error) {
	// Get all manifests from the current snapshot except those being rewritten
	current := mrf.base.txn.meta.currentSnapshot()
	if current == nil {
		return nil, nil
	}
	
	manifests, err := current.Manifests(mrf.base.io)
	if err != nil {
		return nil, err
	}
	
	// Filter out manifests that are being rewritten
	rewriteSet := make(map[string]bool)
	for _, mf := range mrf.manifestsToRewrite {
		rewriteSet[mf.FilePath()] = true
	}
	
	var existing []iceberg.ManifestFile
	for _, mf := range manifests {
		if !rewriteSet[mf.FilePath()] {
			existing = append(existing, mf)
		}
	}
	
	return existing, nil
}

func (mrf *manifestRewriteFiles) deletedEntries() ([]iceberg.ManifestEntry, error) {
	// For manifest rewriting, there are no deleted entries since we're just
	// reorganizing existing data files across different manifest files
	return nil, nil
}

// rewriteManifestsForSpec rewrites manifests for a specific partition spec
func (mrf *manifestRewriteFiles) rewriteManifestsForSpec(specID int, manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	if len(manifests) == 0 {
		return nil, nil
	}
	
	// Collect all entries from the manifests to be rewritten
	var allEntries []iceberg.ManifestEntry
	for _, mf := range manifests {
		entries, err := mrf.base.fetchManifestEntry(mf, false)
		if err != nil {
			return nil, err
		}
		allEntries = append(allEntries, entries...)
	}
	
	if len(allEntries) == 0 {
		return nil, nil
	}
	
	// Create new manifests based on the target size
	newManifests, err := mrf.createNewManifests(specID, allEntries)
	if err != nil {
		return nil, err
	}
	
	return newManifests, nil
}

// createNewManifests creates new manifest files from the given entries
func (mrf *manifestRewriteFiles) createNewManifests(specID int, entries []iceberg.ManifestEntry) ([]iceberg.ManifestFile, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	
	// Get the partition spec for this specID
	spec := mrf.base.spec(specID)
	// Check if spec is empty (which means it wasn't found)
	if spec.ID() == 0 && specID != 0 {
		return nil, fmt.Errorf("partition spec not found: %d", specID)
	}
	
	var newManifests []iceberg.ManifestFile
	
	// Estimate entries per manifest based on target size
	// This is a simple heuristic - in practice you might want more sophisticated sizing
	avgEntrySize := mrf.estimateAverageEntrySize(entries)
	entriesPerManifest := int(mrf.options.TargetSizeBytes / avgEntrySize)
	if entriesPerManifest == 0 {
		entriesPerManifest = 1
	}
	
	// Create manifests in batches
	for i := 0; i < len(entries); i += entriesPerManifest {
		end := i + entriesPerManifest
		if end > len(entries) {
			end = len(entries)
		}
		
		manifestFile, err := mrf.writeManifest(spec, entries[i:end])
		if err != nil {
			return nil, err
		}
		
		newManifests = append(newManifests, manifestFile)
	}
	
	return newManifests, nil
}

// writeManifest writes a manifest file with the given entries
func (mrf *manifestRewriteFiles) writeManifest(spec iceberg.PartitionSpec, entries []iceberg.ManifestEntry) (iceberg.ManifestFile, error) {
	writer, path, counter, err := mrf.base.newManifestWriter(spec)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	
	for _, entry := range entries {
		// Use the appropriate method based on entry status
		switch entry.Status() {
		case iceberg.EntryStatusADDED:
			if err := writer.Add(entry); err != nil {
				return nil, err
			}
		case iceberg.EntryStatusEXISTING:
			if err := writer.Existing(entry); err != nil {
				return nil, err
			}
		case iceberg.EntryStatusDELETED:
			if err := writer.Delete(entry); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unknown entry status: %v", entry.Status())
		}
	}
	
	return writer.ToManifestFile(path, counter.Count)
}

// estimateAverageEntrySize estimates the average size of manifest entries
func (mrf *manifestRewriteFiles) estimateAverageEntrySize(entries []iceberg.ManifestEntry) int64 {
	if len(entries) == 0 {
		return 1024 // default estimate
	}
	
	// Simple heuristic: manifest entries are typically small compared to data files
	// Use a conservative estimate that accounts for metadata overhead
	const avgManifestEntrySize = 512 // bytes per entry estimate
	return avgManifestEntrySize
} 