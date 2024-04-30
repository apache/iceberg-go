package table

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/go-kit/log/level"
	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

type snapshotWriter struct {
	commit     func(ctx context.Context, v int) error
	version    int
	snapshotID int64
	bucket     objstore.Bucket
	table      Table
	options    writerOptions

	schemaChanged bool
	schema        *iceberg.Schema
	spec          iceberg.PartitionSpec
	entries       []iceberg.ManifestEntry
}

func (s *snapshotWriter) metadataDir() string {
	return filepath.Join(s.table.Location(), metadataDirName)
}

func (s *snapshotWriter) dataDir() string {
	return filepath.Join(s.table.Location(), dataDirName)
}

// Append a new new Parquet data file to the table. Append does not write into partitions but instead just adds the data file directly to the table.
// If the table is partitioned, the upper and lower bounds of the entries data file as well as the manifest will still be populated from the Parquet file.
func (s *snapshotWriter) Append(ctx context.Context, r io.Reader) error {
	b := &bytes.Buffer{}
	rdr := io.TeeReader(r, b) // Read file into memory while uploading

	// TODO(thor): We may want to pass in the filename as an option.
	dataFile := filepath.Join(s.dataDir(), fmt.Sprintf("%s%s", generateULID(), parquetFileExt))
	if err := s.bucket.Upload(ctx, dataFile, rdr); err != nil {
		return err
	}

	// Create manifest entry
	entry, schema, err := iceberg.ManifestEntryV1FromParquet(dataFile, int64(b.Len()), s.schema, bytes.NewReader(b.Bytes()))
	if err != nil {
		return err
	}

	// If merge schema is disabled; ensure that the schema isn't changing.
	if s.schema != nil && !s.schema.Equals(schema) {
		if !s.options.mergeSchema {
			return fmt.Errorf("unexpected schema mismatch: %v != %v", s.schema, schema)
		}

		s.schemaChanged = true
	}

	s.schema = schema

	// Update the partition spec if the schema has changed so that the spec ID's match the new field IDs
	if spec := s.table.Metadata().PartitionSpec(); s.schemaChanged && !spec.IsUnpartitioned() {
		updatedFields := make([]iceberg.PartitionField, 0, spec.NumFields())
		for i := 0; i < spec.NumFields(); i++ {
			field := spec.Field(i)
			schemaField, ok := s.schema.FindFieldByName(field.Name)
			if !ok {
				return fmt.Errorf("partition field %s not found in schema", field.Name)
			}

			updatedFields = append(updatedFields, iceberg.PartitionField{
				SourceID:  schemaField.ID,
				Name:      field.Name,
				Transform: field.Transform,
			})
		}
		s.spec = iceberg.NewPartitionSpec(updatedFields...)
	}

	s.entries = append(s.entries, entry)
	return nil
}

func (s *snapshotWriter) Close(ctx context.Context) error {

	// Upload the manifest file
	manifestFile := fmt.Sprintf("%s%s", generateULID(), manifestFileExt)
	path := filepath.Join(s.metadataDir(), manifestFile)
	manifest := s.entries
	currentSnapshot := s.table.CurrentSnapshot()
	var previousManifests []iceberg.ManifestFile
	if currentSnapshot != nil {
		var err error
		previousManifests, err = currentSnapshot.Manifests(s.bucket)
		if err != nil {
			return err
		}
	}

	appendMode := false
	// If the schema hasn't changed we can append to the previous manifest file if it's not too large and we aren't in fast append mode.
	// Otherweise we always create a new manifest file.
	if !s.schemaChanged && len(previousManifests) != 0 && !s.options.fastAppendMode && s.options.manifestSizeBytes > 0 {
		// Check the size of the previous manifest file
		latest := previousManifests[len(previousManifests)-1]
		if latest.Length() < int64(s.options.manifestSizeBytes) { // Append to the latest manifest
			previous, _, err := latest.FetchEntries(s.bucket, false)
			if err != nil {
				return err
			}
			manifest = append(previous, s.entries...)
			appendMode = true
		}
	}

	// Write the manifest file
	if err := s.uploadManifest(ctx, path, func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestV1(w, s.schema, manifest)
	}); err != nil {
		return err
	}

	// Fetch the size of the manifest file
	attr, err := s.bucket.Attributes(ctx, path)
	if err != nil {
		return err
	}

	// Create manifest list
	bldr := iceberg.NewManifestV1Builder(path, attr.Size, 0, s.snapshotID).
		AddedFiles(int32(len(s.entries))).
		ExistingFiles(int32(len(manifest) - len(s.entries)))

	// Add partition information if the table is partitioned
	if !s.spec.IsUnpartitioned() {
		bldr.Partitions(summarizeFields(s.spec, s.schema, manifest))
	}

	newmanifest := bldr.Build()
	var manifestList []iceberg.ManifestFile
	if appendMode { // Replace the last manifest if we are in append mode
		manifestList = append(previousManifests[:len(previousManifests)-1], newmanifest)
	} else {
		manifestList = append(previousManifests, newmanifest)
	}

	// Upload the manifest list
	manifestListFile := fmt.Sprintf("snap-%v-%s%s", s.snapshotID, generateULID(), manifestFileExt)
	manifestListPath := filepath.Join(s.metadataDir(), manifestListFile)
	if err := s.uploadManifest(ctx, manifestListPath, func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestListV1(w, manifestList)
	}); err != nil {
		return err
	}

	// Create snapshot data
	snapshot := Snapshot{
		SnapshotID:   s.snapshotID,
		TimestampMs:  time.Now().UnixMilli(),
		ManifestList: manifestListPath,
		Summary: &Summary{
			Operation: OpAppend,
		},
	}
	md, staleMetadataFiles, staleSnapshots, err := s.addSnapshot(ctx, s.table, snapshot, s.schema)
	if err != nil {
		return err
	}

	// Upload the metadata
	path = filepath.Join(s.metadataDir(), fmt.Sprintf("v%v.metadata%s", s.version+1, metadataFileExt))
	js, err := json.Marshal(md)
	if err != nil {
		return err
	}

	if err := s.bucket.Upload(ctx, path, bytes.NewReader(js)); err != nil {
		return err
	}

	// Commit the snapshot to the table
	if err := s.commit(ctx, s.version+1); err != nil {
		return err
	}

	// Cleanup stale snapshot files and metadata files (if configured)
	s.cleanup(ctx, staleMetadataFiles, staleSnapshots, md)
	return nil
}

func (s *snapshotWriter) cleanup(ctx context.Context, metadataFiles []string, staleSnapshots []Snapshot, md Metadata) {
	// Delete stale metadata files
	if s.options.metadataDeleteAfterCommit {
		for _, file := range metadataFiles {
			if err := s.bucket.Delete(ctx, file); err != nil {
				level.Error(s.options.logger).Log("msg", "failed to delete old file", "file", file, "err", err)
			}
		}
	}

	if len(staleSnapshots) == 0 {
		return
	}

	// Cleanup snapshots; First find if any manifests are no longer reachable by the expired snapshots and remove those.
	// Then remove the snapshot files.

	// Find all the reachable manifest files from snapshots
	currentManifests := map[string]struct{}{}
	for _, snapshot := range md.Snapshots() {
		manifests, err := snapshot.Manifests(s.bucket)
		if err != nil {
			level.Error(s.options.logger).Log("msg", "failed to fetch manifests", "snapshot", snapshot.SnapshotID, "file", snapshot.ManifestList, "err", err)
			continue
		}

		for _, manifest := range manifests {
			currentManifests[manifest.FilePath()] = struct{}{}
		}
	}

	oldManifests := map[string]struct{}{}
	for _, snapshot := range staleSnapshots {
		manifests, err := snapshot.Manifests(s.bucket)
		if err != nil {
			level.Error(s.options.logger).Log("msg", "failed to fetch manifests", "snapshot", snapshot.SnapshotID, "file", snapshot.ManifestList, "err", err)
			continue
		}

		for _, manifest := range manifests {
			oldManifests[manifest.FilePath()] = struct{}{}
		}
	}

	// Remove stale manifest files
	for manifest := range oldManifests {
		if _, ok := currentManifests[manifest]; ok {
			continue
		}

		if err := s.bucket.Delete(ctx, manifest); err != nil {
			level.Error(s.options.logger).Log("msg", "failed to delete stale manifest", "file", manifest, "err", err)
		}
	}

	// Delete stale snapshot files
	for _, snapshot := range staleSnapshots {
		if err := s.bucket.Delete(ctx, snapshot.ManifestList); err != nil {
			level.Error(s.options.logger).Log("msg", "failed to delete stale snapshot", "snapshot", snapshot.SnapshotID, "file", snapshot.ManifestList, "err", err)
		}
	}
}

func (s *snapshotWriter) addSnapshot(ctx context.Context, t Table, snapshot Snapshot, schema *iceberg.Schema) (Metadata, []string, []Snapshot, error) {
	metadata := CloneMetadataV1(t.Metadata())
	ts := time.Now().UnixMilli()

	if !t.Metadata().CurrentSchema().Equals(schema) {
		// need to only update the schema ID if it has changed
		schema.ID = metadata.CurrentSchema().ID + 1
	} else {
		schema.ID = metadata.CurrentSchema().ID
	}

	// Expire old snapshots if requested
	snapshots := metadata.Snapshots()
	staleSnapshots := []Snapshot{}
	if s.options.expireSnapshotsOlderThan != 0 {
		snapshots = []Snapshot{}
		for _, snapshot := range metadata.Snapshots() {
			if time.Since(time.UnixMilli(snapshot.TimestampMs)) <= s.options.expireSnapshotsOlderThan {
				snapshots = append(snapshots, snapshot)
			} else {
				staleSnapshots = append(staleSnapshots, snapshot)
			}
		}
	}

	log := metadata.GetMetadataLog()
	staleMetadataFiles := []string{}
	if s.options.metadataPreviousVersionsMax > 0 {
		log = append(log, MetadataLogEntry{
			MetadataFile: s.table.MetadataLocation(),
			TimestampMs:  ts,
		})

		// Truncate up to the maximum number of previous versions
		if len(log) > s.options.metadataPreviousVersionsMax {
			staleCount := len(log) - s.options.metadataPreviousVersionsMax
			for i := 0; i < staleCount; i++ {
				staleMetadataFiles = append(staleMetadataFiles, log[i].MetadataFile)
			}
			log = log[len(log)-s.options.metadataPreviousVersionsMax:]
		}
	}

	return metadata.
		WithSchema(schema).
		WithSchemas(nil). // Only retain a single schema
		WithLastUpdatedMs(ts).
		WithCurrentSnapshotID(snapshot.SnapshotID).
		WithSnapshots(append(snapshots, snapshot)).
		WithPartitionSpecs([]iceberg.PartitionSpec{s.spec}). // Only retain a single partition spec
		WithMetadataLog(log).
		Build(), staleMetadataFiles, staleSnapshots, nil
}

// uploadManifest uploads a manifest to the iceberg table. It's a wrapper around the bucket upload which requires a io.Reader and the manifest write functions which requires a io.Writer.
func (s *snapshotWriter) uploadManifest(ctx context.Context, path string, write func(ctx context.Context, w io.Writer) error) error {
	r, w := io.Pipe()

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		defer w.Close()
		return write(ctx, w)
	})

	errg.Go(func() error {
		return s.bucket.Upload(ctx, path, r)
	})

	return errg.Wait()
}

// summarizeFields returns the field summaries for the given partition spec and manifest entries.
func summarizeFields(spec iceberg.PartitionSpec, schema *iceberg.Schema, entries []iceberg.ManifestEntry) []iceberg.FieldSummary {
	fieldSummaries := []iceberg.FieldSummary{}

	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		typ := schema.Field(field.SourceID).Type

		// Find the entry with the lower/upper bounds for the field
		u, l := []byte{}, []byte{}
		for _, entry := range entries {
			upper := entry.DataFile().UpperBoundValues()[field.SourceID]
			lower := entry.DataFile().LowerBoundValues()[field.SourceID]

			if len(u) == 0 || compare(u, upper, typ) < 0 {
				u = upper
			}

			if len(l) == 0 || compare(lower, l, typ) < 0 {
				l = lower
			}
		}

		fieldSummaries = append(fieldSummaries, iceberg.FieldSummary{
			LowerBound: &l,
			UpperBound: &u,
		})
	}

	return fieldSummaries
}

func compare(a, b []byte, typ iceberg.Type) int {
	switch typ.Type() {
	case "boolean":
		return bytes.Compare(a, b)
	case "int":
		a := int32(binary.LittleEndian.Uint32(a))
		b := int32(binary.LittleEndian.Uint32(b))
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	case "float":
		a := float32(binary.LittleEndian.Uint32(a))
		b := float32(binary.LittleEndian.Uint32(b))
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	case "long":
		a := int64(binary.LittleEndian.Uint64(a))
		b := int64(binary.LittleEndian.Uint64(b))
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	case "double":
		a := float64(binary.LittleEndian.Uint64(a))
		b := float64(binary.LittleEndian.Uint64(b))
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	case "string":
		return bytes.Compare(a, b)
	case "binary":
		return bytes.Compare(a, b)
	default:
		panic(fmt.Sprintf("unsupported type %v", typ))
	}
}
