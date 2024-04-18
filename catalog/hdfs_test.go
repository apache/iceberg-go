package catalog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/polarsignals/iceberg-go"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func Test_HDFS(t *testing.T) {
	warehouse := filepath.Join("testdata", t.Name()) // TODO: ideally we can get this working in-memory
	require.NoError(t, os.MkdirAll(warehouse, 0o755))
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll("testdata"))
	})

	bucket, err := filesystem.NewBucket(warehouse)
	require.NoError(t, err)

	wh, err := filepath.Abs(warehouse)
	require.NoError(t, err)

	catalog := NewHDFS(wh, bucket)

	tablePath := filepath.Join(wh, "db", "test_table")

	ctx := context.Background()
	tbl, err := catalog.CreateTable(ctx, tablePath, iceberg.NewSchema(0), iceberg.Properties{})
	require.NoError(t, err)

	writer, err := tbl.SnapshotWriter()
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
	table, err := catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	require.NoError(t, err)

	require.Equal(t, table.Location(), tablePath)
	snapshot := table.CurrentSnapshot()
	require.NotNil(t, snapshot)

	manifests, err := snapshot.Manifests(NewIcebucket(wh, bucket))
	require.NoError(t, err)

	require.Len(t, manifests, 1)
	entries, err := manifests[0].FetchEntries(NewIcebucket(wh, bucket), false)
	require.NoError(t, err)

	require.Len(t, entries, 1)
}
