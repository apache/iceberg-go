package hive

import (
	"context"
	"errors"
	"path"
	"strings"
	"testing"

	"iter"

	"github.com/stretchr/testify/assert"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	cataloginternal "github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive"
	hms "github.com/beltran/gohive/hive_metastore"
)

type mockMetastore struct {
	databases map[string]struct{}
	tables    map[string]*hms.Table // key db.table
	failOnce  map[string]bool
}

func newMockMetastore() *mockMetastore {
	return &mockMetastore{
		databases: map[string]struct{}{},
		tables:    map[string]*hms.Table{},
		failOnce:  map[string]bool{},
	}
}

func (m *mockMetastore) Close() {}

func (m *mockMetastore) GetAllTables(ctx context.Context, db string) ([]string, error) {
	if m.failOnce["get_all_tables"] {
		m.failOnce["get_all_tables"] = false
		return nil, errors.New("connection error")
	}
	var out []string
	for k := range m.tables {
		if strings.HasPrefix(k, db+".") {
			out = append(out, strings.TrimPrefix(k, db+"."))
		}
	}
	return out, nil
}

func (m *mockMetastore) GetTable(ctx context.Context, db, tbl string) (*hms.Table, error) {
	if m.failOnce["get_table"] {
		m.failOnce["get_table"] = false
		return nil, errors.New("connection error")
	}
	return m.tables[db+"."+tbl], nil
}

func (m *mockMetastore) CreateTable(ctx context.Context, tbl *hms.Table) error {
	if m.failOnce["create_table"] {
		m.failOnce["create_table"] = false
		return errors.New("connection error")
	}
	m.tables[tbl.DbName+"."+tbl.TableName] = tbl
	return nil
}

func (m *mockMetastore) DropTable(ctx context.Context, db, tbl string, _ bool) error {
	if m.failOnce["drop_table"] {
		m.failOnce["drop_table"] = false
		return errors.New("connection error")
	}
	delete(m.tables, db+"."+tbl)
	return nil
}

func (m *mockMetastore) AlterTable(ctx context.Context, db, tbl string, newTable *hms.Table) error {
	m.tables[newTable.DbName+"."+newTable.TableName] = newTable
	delete(m.tables, db+"."+tbl)
	return nil
}

func (m *mockMetastore) GetAllDatabases(ctx context.Context) ([]string, error) {
	if m.failOnce["get_all_databases"] {
		m.failOnce["get_all_databases"] = false
		return nil, errors.New("connection error")
	}
	out := make([]string, 0, len(m.databases))
	for k := range m.databases {
		out = append(out, k)
	}
	return out, nil
}

func (m *mockMetastore) CreateDatabase(ctx context.Context, db *hms.Database) error {
	if m.failOnce["create_database"] {
		m.failOnce["create_database"] = false
		return errors.New("connection error")
	}
	m.databases[db.Name] = struct{}{}
	return nil
}

func (m *mockMetastore) DropDatabase(ctx context.Context, name string, _, _ bool) error {
	if m.failOnce["drop_database"] {
		m.failOnce["drop_database"] = false
		return errors.New("connection error")
	}
	delete(m.databases, name)
	for k := range m.tables {
		if strings.HasPrefix(k, name+".") {
			delete(m.tables, k)
		}
	}
	return nil
}

// helper to prepare metadata file
func writeMetadata(t *testing.T, dir string, sc *iceberg.Schema) string {
	metadata, err := table.NewMetadata(sc, nil, table.UnsortedSortOrder, dir, nil)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	loc := path.Join(dir, "metadata", "00000-00000000000000000000000000000000.metadata.json")
	if err := cataloginternal.WriteTableMetadata(metadata, io.LocalFS{}, loc); err != nil {
		t.Fatalf("write metadata: %v", err)
	}
	return loc
}

func TestNewHiveCatalogSuccess(t *testing.T) {
	mt := newMockMetastore()
	orig := connectToMetastore
	connectToMetastore = func(host string, port int, auth string, cfg *gohive.MetastoreConnectConfiguration) (metastoreClient, error) {
		return mt, nil
	}
	defer func() { connectToMetastore = orig }()

	cfg := Config{Host: "localhost", Port: 1, Auth: "NONE"}
	cat, err := NewHiveCatalog(cfg)
	if err != nil {
		t.Fatalf("new catalog: %v", err)
	}
	if cat.client == nil {
		t.Fatalf("expected client")
	}
}

func TestHiveCatalogListTables(t *testing.T) {
	mt := newMockMetastore()
	mt.databases["db"] = struct{}{}
	mt.tables["db.t1"] = &hms.Table{DbName: "db", TableName: "t1", Parameters: map[string]string{"metadata_location": "loc"}}
	mt.tables["db.t2"] = &hms.Table{DbName: "db", TableName: "t2", Parameters: map[string]string{"metadata_location": "loc"}}
	cat := &HiveCatalog{client: mt}

	next, stop := iter.Pull2(cat.ListTables(context.Background(), table.Identifier{"db"}))
	defer stop()
	var names []string
	for {
		id, err, ok := next()
		if !ok {
			break
		}
		if err != nil {
			t.Fatalf("list tables: %v", err)
		}
		names = append(names, id[1])
	}
	assert.ElementsMatch(t, []string{"t1", "t2"}, names)
}

func TestHiveCatalogListTablesInvalid(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	next, stop := iter.Pull2(cat.ListTables(context.Background(), table.Identifier{"a", "b"}))
	defer stop()
	_, err, ok := next()
	if !ok || err == nil {
		t.Fatalf("expected error for invalid namespace")
	}
}

func TestHiveCatalogCreateTable(t *testing.T) {
	mt := newMockMetastore()
	mt.databases["db"] = struct{}{}
	cat := &HiveCatalog{client: mt}
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	loc := t.TempDir()
	id := table.Identifier{"db", "tbl"}
	tbl, err := cat.CreateTable(context.Background(), id, sc, catalog.WithLocation(loc))
	if err != nil {
		t.Fatalf("create table: %v", err)
	}
	assert.Equal(t, id, tbl.Identifier())
}

func TestHiveCatalogCreateTableRetry(t *testing.T) {
	fail := newMockMetastore()
	fail.failOnce["create_table"] = true
	good := newMockMetastore()
	cat := &HiveCatalog{client: fail, host: "h", port: 1, auth: "NONE", options: gohive.NewMetastoreConnectConfiguration()}
	orig := connectToMetastore
	connectToMetastore = func(host string, port int, auth string, cfg *gohive.MetastoreConnectConfiguration) (metastoreClient, error) {
		return good, nil
	}
	defer func() { connectToMetastore = orig }()
	good.databases["db"] = struct{}{}
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	loc := t.TempDir()
	id := table.Identifier{"db", "tbl"}
	if _, err := cat.CreateTable(context.Background(), id, sc, catalog.WithLocation(loc)); err != nil {
		t.Fatalf("create table retry: %v", err)
	}
}

func TestHiveCatalogLoadTable(t *testing.T) {
	mt := newMockMetastore()
	sc := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int32, Required: true})
	dir := t.TempDir()
	loc := writeMetadata(t, dir, sc)
	mt.tables["db.tbl"] = &hms.Table{DbName: "db", TableName: "tbl", Parameters: map[string]string{"metadata_location": loc}, Sd: &hms.StorageDescriptor{Location: dir}}
	cat := &HiveCatalog{client: mt}
	tbl, err := cat.LoadTable(context.Background(), table.Identifier{"db", "tbl"}, nil)
	if err != nil {
		t.Fatalf("load table: %v", err)
	}
	assert.Equal(t, table.Identifier{"db", "tbl"}, tbl.Identifier())
}

func TestHiveCatalogLoadTableInvalid(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	if _, err := cat.LoadTable(context.Background(), table.Identifier{"db"}, nil); err == nil {
		t.Fatalf("expected error")
	}
}

func TestHiveCatalogDropTable(t *testing.T) {
	mt := newMockMetastore()
	mt.tables["db.tbl"] = &hms.Table{DbName: "db", TableName: "tbl", Parameters: map[string]string{"metadata_location": "loc"}}
	cat := &HiveCatalog{client: mt}
	if err := cat.DropTable(context.Background(), table.Identifier{"db", "tbl"}); err != nil {
		t.Fatalf("drop table: %v", err)
	}
	if _, ok := mt.tables["db.tbl"]; ok {
		t.Fatalf("table still exists")
	}
}

func TestHiveCatalogListNamespaces(t *testing.T) {
	mt := newMockMetastore()
	mt.databases["db1"] = struct{}{}
	mt.databases["db2"] = struct{}{}
	cat := &HiveCatalog{client: mt}
	namespaces, err := cat.ListNamespaces(context.Background(), nil)
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	var names []string
	for _, ns := range namespaces {
		names = append(names, ns[0])
	}
	assert.ElementsMatch(t, []string{"db1", "db2"}, names)
}

func TestHiveCatalogListNamespacesError(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	if _, err := cat.ListNamespaces(context.Background(), table.Identifier{"a"}); err == nil {
		t.Fatalf("expected error")
	}
}

func TestHiveCatalogCreateNamespace(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	if err := cat.CreateNamespace(context.Background(), table.Identifier{"db"}, nil); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, ok := mt.databases["db"]; !ok {
		t.Fatalf("namespace not created")
	}
}

func TestHiveCatalogCreateNamespaceInvalid(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	if err := cat.CreateNamespace(context.Background(), table.Identifier{"a", "b"}, nil); err == nil {
		t.Fatalf("expected error")
	}
}

func TestHiveCatalogDropNamespace(t *testing.T) {
	mt := newMockMetastore()
	mt.databases["db"] = struct{}{}
	mt.tables["db.tbl"] = &hms.Table{DbName: "db", TableName: "tbl"}
	cat := &HiveCatalog{client: mt}
	if err := cat.DropNamespace(context.Background(), table.Identifier{"db"}); err != nil {
		t.Fatalf("drop namespace: %v", err)
	}
	if _, ok := mt.databases["db"]; ok {
		t.Fatalf("namespace still exists")
	}
	if _, ok := mt.tables["db.tbl"]; ok {
		t.Fatalf("table still exists")
	}
}

func TestHiveCatalogDropNamespaceInvalid(t *testing.T) {
	mt := newMockMetastore()
	cat := &HiveCatalog{client: mt}
	if err := cat.DropNamespace(context.Background(), table.Identifier{"a", "b"}); err == nil {
		t.Fatalf("expected error")
	}
}
