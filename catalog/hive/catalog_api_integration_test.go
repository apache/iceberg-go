//go:build integration

package hive

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	ice "github.com/apache/iceberg-go"
	catpkg "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	compose "github.com/testcontainers/testcontainers-go/modules/compose"
)

var (
	composeErr error
	cm         compose.ComposeStack
)

// TestMain starts the Hive Metastore using docker-compose for all tests.
// To run these tests manually:
//
//	docker-compose -f catalog/hive/testdata/docker-compose.yml up -d
//	go test -tags=integration ./catalog/hive
func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cm, composeErr = compose.NewDockerComposeWith(compose.WithStackFiles("testdata/docker-compose.yml"))
	if composeErr == nil {
		composeErr = cm.Up(ctx)
	}
	code := m.Run()
	if composeErr == nil {
		cm.Down(ctx, compose.RemoveOrphans(true), compose.RemoveImagesLocal)
	}
	os.Exit(code)
}

func setupHiveCatalog(t *testing.T) *Catalog {
	t.Helper()
	if composeErr != nil {
		t.Skipf("compose not available: %v", composeErr)
	}
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", "localhost:9083", time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
		return false
	}, time.Minute, time.Second)

	cfg := Config{Host: "localhost", Port: 9083, Auth: "NONE"}
	cat, err := NewHiveCatalog(cfg)
	if err != nil {
		t.Skipf("metastore not available: %v", err)
	}
	return cat
}

func randName(prefix string) string {
	return prefix + strings.ReplaceAll(uuid.NewString(), "-", "")
}

func TestCreateNamespace(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	t.Logf("create namespace %s", ns[0])
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	exists, err := cat.CheckNamespaceExists(ctx, ns)
	require.NoError(t, err)
	require.True(t, exists)

	err = cat.CreateNamespace(ctx, ns, nil)
	require.ErrorIs(t, err, catpkg.ErrNamespaceAlreadyExists)
}

func TestListNamespaces(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns1 := table.Identifier{randName("ns_")}
	ns2 := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns1, nil))
	require.NoError(t, cat.CreateNamespace(ctx, ns2, nil))
	t.Cleanup(func() {
		_ = cat.DropNamespace(ctx, ns1)
		_ = cat.DropNamespace(ctx, ns2)
	})

	nss, err := cat.ListNamespaces(ctx, nil)
	require.NoError(t, err)
	require.Contains(t, nss, ns1)
	require.Contains(t, nss, ns2)

	_, err = cat.ListNamespaces(ctx, table.Identifier{"missing"})
	require.Error(t, err)
}

func TestDropNamespace(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	require.NoError(t, cat.DropNamespace(ctx, ns))
	exists, err := cat.CheckNamespaceExists(ctx, ns)
	require.NoError(t, err)
	require.False(t, exists)

	err = cat.DropNamespace(ctx, ns)
	require.ErrorIs(t, err, catpkg.ErrNoSuchNamespace)
}

func TestCheckNamespaceExists(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	exists, err := cat.CheckNamespaceExists(ctx, ns)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = cat.CheckNamespaceExists(ctx, table.Identifier{randName("missing_")})
	require.NoError(t, err)
	require.False(t, exists)
}

func TestLoadNamespaceProperties(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	props := ice.Properties{"a": "1"}
	require.NoError(t, cat.CreateNamespace(ctx, ns, props))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loaded, err := cat.LoadNamespaceProperties(ctx, ns)
	require.NoError(t, err)
	require.Equal(t, props, loaded)

	_, err = cat.LoadNamespaceProperties(ctx, table.Identifier{"missing"})
	require.ErrorIs(t, err, catpkg.ErrNoSuchNamespace)
}

func TestUpdateNamespaceProperties(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, ice.Properties{"k": "v", "rm": "x"}))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	summary, err := cat.UpdateNamespaceProperties(ctx, ns, []string{"rm"}, ice.Properties{"k": "v2", "new": "y"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"rm"}, summary.Removed)
	require.ElementsMatch(t, []string{"k", "new"}, summary.Updated)

	loaded, err := cat.LoadNamespaceProperties(ctx, ns)
	require.NoError(t, err)
	require.Equal(t, ice.Properties{"k": "v2", "new": "y"}, loaded)

	_, err = cat.UpdateNamespaceProperties(ctx, table.Identifier{"missing"}, nil, nil)
	require.ErrorIs(t, err, catpkg.ErrNoSuchNamespace)
}

func tableSchema() *ice.Schema {
	return ice.NewSchema(0,
		ice.NestedField{ID: 1, Name: "id", Type: ice.PrimitiveTypes.Int32, Required: true},
		ice.NestedField{ID: 2, Name: "data", Type: ice.PrimitiveTypes.String, Required: false},
	)
}

func arrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "data", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

func TestCreateTable(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, err := os.MkdirTemp("", "iceberg")
	require.NoError(t, err)
	ident := append(ns, randName("tbl_"))
	tbl, err := cat.CreateTable(ctx, ident, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, ident) })
	require.True(t, tbl.Schema().Equals(tableSchema()))

	_, err = cat.CreateTable(ctx, ident, tableSchema())
	require.ErrorIs(t, err, catpkg.ErrTableAlreadyExists)
}

func TestListTables(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc1, _ := os.MkdirTemp("", "iceberg")
	loc2, _ := os.MkdirTemp("", "iceberg")
	id1 := append(ns, randName("tbl_"))
	id2 := append(ns, randName("tbl_"))
	require.NoError(t, func() error {
		_, err := cat.CreateTable(ctx, id1, tableSchema(), catpkg.WithLocation("file://"+loc1))
		return err
	}())
	require.NoError(t, func() error {
		_, err := cat.CreateTable(ctx, id2, tableSchema(), catpkg.WithLocation("file://"+loc2))
		return err
	}())
	t.Cleanup(func() {
		_ = cat.DropTable(ctx, id1)
		_ = cat.DropTable(ctx, id2)
	})

	tbls := cat.ListTables(ctx, ns)
	found := []table.Identifier{}
	for tblIdent, err := range tbls {
		require.NoError(t, err)
		found = append(found, tblIdent)
	}
	require.Contains(t, found, id1)
	require.Contains(t, found, id2)

	it := cat.ListTables(ctx, table.Identifier{"missing"})
	var err error
	it(func(_ table.Identifier, e error) bool {
		err = e
		return false
	})
	require.Error(t, err)
}

func TestCheckTableExists(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, _ := os.MkdirTemp("", "iceberg")
	id := append(ns, randName("tbl_"))
	_, err := cat.CreateTable(ctx, id, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, id) })

	exists, err := cat.CheckTableExists(ctx, id)
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = cat.CheckTableExists(ctx, append(ns, "missing"))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestDropTable(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, _ := os.MkdirTemp("", "iceberg")
	id := append(ns, randName("tbl_"))
	_, err := cat.CreateTable(ctx, id, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)

	require.NoError(t, cat.DropTable(ctx, id))
	exists, err := cat.CheckTableExists(ctx, id)
	require.NoError(t, err)
	require.False(t, exists)

	err = cat.DropTable(ctx, id)
	require.ErrorIs(t, err, catpkg.ErrNoSuchTable)
}

func TestRenameTable(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, _ := os.MkdirTemp("", "iceberg")
	from := append(ns, randName("tbl_"))
	to := append(ns, randName("tbl_"))
	_, err := cat.CreateTable(ctx, from, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, to) })

	_, err = cat.RenameTable(ctx, from, to)
	require.NoError(t, err)

	exists, err := cat.CheckTableExists(ctx, from)
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = cat.CheckTableExists(ctx, to)
	require.NoError(t, err)
	require.True(t, exists)

	_, err = cat.RenameTable(ctx, from, append(ns, "missing"))
	require.Error(t, err)
}

func TestLoadTable(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, _ := os.MkdirTemp("", "iceberg")
	id := append(ns, randName("tbl_"))
	_, err := cat.CreateTable(ctx, id, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, id) })

	tbl, err := cat.LoadTable(ctx, id, nil)
	require.NoError(t, err)
	require.True(t, tbl.Schema().Equals(tableSchema()))

	_, err = cat.LoadTable(ctx, append(ns, "missing"), nil)
	require.ErrorIs(t, err, catpkg.ErrNoSuchTable)
}

func TestAppendAndReadData(t *testing.T) {
	cat := setupHiveCatalog(t)
	ctx := context.Background()
	ns := table.Identifier{randName("ns_")}
	require.NoError(t, cat.CreateNamespace(ctx, ns, nil))
	t.Cleanup(func() { _ = cat.DropNamespace(ctx, ns) })

	loc, _ := os.MkdirTemp("", "iceberg")
	id := append(ns, randName("tbl_"))
	tbl, err := cat.CreateTable(ctx, id, tableSchema(), catpkg.WithLocation("file://"+loc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.DropTable(ctx, id) })

	arrSchema := arrowSchema()
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, arrSchema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	rec := bldr.NewRecord()
	defer rec.Release()
	arrTbl := array.NewTableFromRecords(arrSchema, []arrow.Record{rec})
	defer arrTbl.Release()

	tbl, err = tbl.AppendTable(ctx, arrTbl, arrTbl.NumRows(), nil)
	require.NoError(t, err)

	result, err := tbl.Scan().ToArrowTable(ctx)
	require.NoError(t, err)
	defer result.Release()
	require.Equal(t, int64(2), result.NumRows())
}
