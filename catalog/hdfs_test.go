package catalog

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func Test_HDFS(t *testing.T) {
	bucket := objstore.NewInMemBucket()

	catalog := NewHDFS("/", bucket)

	tablePath := filepath.Join("db", "test_table")

	ctx := context.Background()
	tbl, err := catalog.CreateTable(ctx, tablePath, iceberg.NewSchema(0), iceberg.Properties{})
	require.NoError(t, err)

	writer, err := tbl.SnapshotWriter(
		table.WithMergeSchema(),
	)
	require.NoError(t, err)

	type RowType struct{ FirstName, LastName string }

	b := &bytes.Buffer{}
	err = parquet.Write(b, []RowType{
		{FirstName: "Bob"},
		{FirstName: "Alice"},
	})
	require.NoError(t, err)

	require.NoError(t, writer.Append(ctx, b))

	require.NoError(t, writer.Close(ctx))

	// Read the data back
	tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	require.NoError(t, err)

	require.Equal(t, tbl.Location(), tablePath)
	snapshot := tbl.CurrentSnapshot()
	require.NotNil(t, snapshot)

	manifests, err := snapshot.Manifests(bucket)
	require.NoError(t, err)

	require.Len(t, manifests, 1)
	entries, _, err := manifests[0].FetchEntries(bucket, false)
	require.NoError(t, err)

	require.Len(t, entries, 1)

	rc, err := bucket.Get(ctx, entries[0].DataFile().FilePath())
	require.NoError(t, err)
	t.Cleanup(func() { rc.Close() })

	buf, err := io.ReadAll(rc)
	require.NoError(t, err)

	_, err = parquet.OpenFile(bytes.NewReader(buf), int64(len(buf)))
	require.NoError(t, err)

	t.Run("AppendManifestWithExpiration", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithExpireSnapshotsOlderThan(time.Nanosecond),
			table.WithManifestSizeBytes(1024*1024),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Charlie"},
			{FirstName: "David"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		// Expect where to only be one snapshot (the one we just added)
		require.Len(t, tbl.Metadata().Snapshots(), 1)

		// Expect there to be only one manifest file (it was appended to)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 1)
	})

	t.Run("FastAppendWithoutExpiration", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithManifestSizeBytes(1024*1024),
			table.WithFastAppend(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Batman"},
			{FirstName: "Robin"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().Snapshots(), 2)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 2)
	})

	t.Run("MergeSchema", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMergeSchema(),
			table.WithManifestSizeBytes(1024*1024),
			table.WithFastAppend(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		type NewRowType struct{ FirstName, MiddleName, LastName string }
		err = parquet.Write(b, []NewRowType{
			{
				FirstName:  "Thomas",
				MiddleName: "Woodrow",
				LastName:   "Wilson",
			},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().Snapshots(), 3)
		mfst, err := tbl.CurrentSnapshot().Manifests(bucket)
		require.NoError(t, err)
		require.Len(t, mfst, 3)

		require.Len(t, tbl.Metadata().CurrentSchema().Fields(), 3)
	})

	t.Run("RemoveStaleMetadata", func(t *testing.T) {
		w, err := tbl.SnapshotWriter(
			table.WithMetadataPreviousVersionsMax(1),
			table.WithMetadataDeleteAfterCommit(),
		)
		require.NoError(t, err)

		b := &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Steve"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		// Expect the metadata log to have been created.
		require.Len(t, tbl.Metadata().GetMetadataLog(), 1)
		file := tbl.Metadata().GetMetadataLog()[0].MetadataFile
		_, err = bucket.Attributes(ctx, file)
		require.NoError(t, err)

		w, err = tbl.SnapshotWriter(
			table.WithMetadataPreviousVersionsMax(1),
			table.WithMetadataDeleteAfterCommit(),
		)
		require.NoError(t, err)

		b = &bytes.Buffer{}
		err = parquet.Write(b, []RowType{
			{FirstName: "Erwin"},
		})
		require.NoError(t, err)
		require.NoError(t, w.Append(ctx, b))
		require.NoError(t, w.Close(ctx))

		tbl, err = catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
		require.NoError(t, err)

		require.Len(t, tbl.Metadata().GetMetadataLog(), 1)
		// Validate that the file was deleted
		_, err = bucket.Attributes(ctx, file)
		require.True(t, bucket.IsObjNotFoundErr(err))
	})
}
