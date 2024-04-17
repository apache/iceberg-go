package table

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"time"

	"github.com/polarsignals/iceberg-go"
	"github.com/thanos-io/objstore"
	"golang.org/x/sync/errgroup"
)

const (
	hdfsVersionHintFile = "version-hint.text"
	parquetFileExt      = ".parquet"
)

type hdfsTable struct {
	version int // The version of the table that has been loaded
	*baseTable
}

func NewHDFSTable(ver int, ident Identifier, meta Metadata, location string, bucket objstore.Bucket) Table {
	return &hdfsTable{
		version: ver,
		baseTable: &baseTable{
			identifier:       ident,
			metadata:         meta,
			metadataLocation: location,
			bucket:           bucket,
		},
	}
}

func (t *hdfsTable) SnapshotWriter(options ...WriterOption) (SnapshotWriter, error) {
	writer := &hdfsSnapshotWriter{
		options:    writerOptions{},
		snapshotID: rand.Int63(),
		bucket:     t.bucket,
		version:    t.version,
		table:      t,
	}

	for _, options := range options {
		options(&writer.options)
	}

	return writer, nil
}

type hdfsSnapshotWriter struct {
	version    int
	snapshotID int64
	bucket     objstore.Bucket
	table      Table
	options    writerOptions

	schema  *iceberg.Schema
	entries []iceberg.ManifestEntry
}

func (s *hdfsSnapshotWriter) metadataDir() string {
	return filepath.Join(s.table.Location(), metadataDirName)
}

func (s *hdfsSnapshotWriter) dataDir() string {
	return filepath.Join(s.table.Location(), dataDirName)
}

// it might be worth setting this as an option.
func (s *hdfsSnapshotWriter) Append(ctx context.Context, r io.Reader) error {
	b := &bytes.Buffer{}
	rdr := io.TeeReader(r, b) // Read file into memory while uploading

	// TODO(thor): We may want to pass in the filename as an option.
	dataFile := filepath.Join(s.dataDir(), fmt.Sprintf("%s%s", generateULID(), parquetFileExt))
	if err := s.bucket.Upload(ctx, dataFile, rdr); err != nil {
		return err
	}

	// Create manifest entry
	entry, schema, err := iceberg.ManifestEntryV1FromParquet(dataFile, int64(b.Len()), bytes.NewReader(b.Bytes()))
	if err != nil {
		return err
	}

	// If merge schema is disabled; ensure that the schema isn't changing.
	if !s.options.mergeSchema {
		if s.schema != nil && !s.schema.Equals(schema) {
			return fmt.Errorf("schema mismatch: %v != %v", s.schema, schema)
		}
	}

	// Merge the schema with the table schema
	if s.schema == nil {
		s.schema, err = s.table.Metadata().CurrentSchema().Merge(schema)
		if err != nil {
			return err
		}
	} else {
		s.schema, err = s.schema.Merge(schema)
		if err != nil {
			return err
		}
	}

	s.entries = append(s.entries, entry)
	return nil
}

func (s *hdfsSnapshotWriter) Close(ctx context.Context) error {

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

	// If not in fast append mode; check if we can append to the previous manifest file.
	appendMode := false
	if len(previousManifests) != 0 && !s.options.fastAppendMode && s.options.manifestSizeBytes > 0 {
		// Check the size of the previous manifest file
		latest := previousManifests[len(previousManifests)-1]
		if latest.Length() < int64(s.options.manifestSizeBytes) { // Append to the latest manifest
			previous, err := latest.FetchEntries(s.bucket, false)
			if err != nil {
				return err
			}
			manifest = append(previous, s.entries...)
			appendMode = true
		}
	}

	// Write the manifest file
	if err := s.uploadManifest(ctx, path, func(ctx context.Context, w io.Writer) error {
		return iceberg.WriteManifestV1(w, manifest)
	}); err != nil {
		return err
	}

	// Fetch the size of the manifest file
	attr, err := s.bucket.Attributes(ctx, path)
	if err != nil {
		return err
	}

	// Create manifest list
	newmanifest := iceberg.NewManifestV1Builder(path, attr.Size, 0, s.snapshotID).
		AddedFiles(int32(len(s.entries))).
		ExistingFiles(int32(len(manifest) - len(s.entries))).
		Build()
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
	md, err := s.addSnapshot(ctx, s.table, snapshot, s.schema)
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

	// Upload the version hint
	hint := []byte(fmt.Sprintf("%v", s.version+1))
	path = filepath.Join(s.metadataDir(), hdfsVersionHintFile)
	if err := s.bucket.Upload(ctx, path, bytes.NewReader(hint)); err != nil {
		return err
	}

	return nil
}

func (s *hdfsSnapshotWriter) addSnapshot(ctx context.Context, t Table, snapshot Snapshot, schema *iceberg.Schema) (Metadata, error) {
	metadata := t.Metadata()

	if !t.Metadata().CurrentSchema().Equals(schema) {
		// need to only update the schema ID if it has changed
		schema.ID = metadata.CurrentSchema().ID + 1
	} else {
		schema.ID = metadata.CurrentSchema().ID
	}

	// Expire old snapshots if requested
	snapshots := metadata.Snapshots()
	if s.options.expireSnapshotsOlderThan != 0 {
		snapshots = []Snapshot{}
		for _, snapshot := range metadata.Snapshots() {
			if time.Since(time.UnixMilli(snapshot.TimestampMs)) <= s.options.expireSnapshotsOlderThan {
				snapshots = append(snapshots, snapshot)
			}
		}
	}

	return NewMetadataV1Builder(
		metadata.Location(),
		schema,
		time.Now().UnixMilli(),
		schema.NumFields(),
	).
		WithTableUUID(metadata.TableUUID()).
		WithCurrentSchemaID(schema.ID).
		WithCurrentSnapshotID(snapshot.SnapshotID).
		WithSnapshots(append(snapshots, snapshot)).
		Build(), nil
}

// uploadManifest uploads a manifest to the iceberg table. It's a wrapper around the bucket upload which requires a io.Reader and the manifest write functions which requires a io.Writer.
func (s *hdfsSnapshotWriter) uploadManifest(ctx context.Context, path string, write func(ctx context.Context, w io.Writer) error) error {
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
