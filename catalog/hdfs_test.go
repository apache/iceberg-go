package catalog

import (
	"bytes"
	"context"
	"io"
	"path/filepath"
	"testing"

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
	table, err := catalog.LoadTable(ctx, []string{tablePath}, iceberg.Properties{})
	require.NoError(t, err)

	require.Equal(t, table.Location(), tablePath)
	snapshot := table.CurrentSnapshot()
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
}
