package catalog

import (
	"bufio"
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/thanos-io/objstore"
)

var (
	ErrorTableNotFound = fmt.Errorf("table not found")
)

const (
	hdfsTableMetadataDir = "metadata"
	hdfsVersionHintFile  = "version-hint.text"

	namespaceSeparator = "\x1F"
)

func hdfsMetadataFileName(version int) string {
	return fmt.Sprintf("v%d.metadata.json", version)
}

type hdfs struct {
	bucket objstore.Bucket
}

func NewHDFS(uri string, bucket objstore.Bucket) Catalog {
	return &hdfs{bucket: NewIcebucket(uri, bucket)}
}

func (h *hdfs) CatalogType() CatalogType {
	return Hadoop
}

func (h *hdfs) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if len(namespace) != 1 {
		return nil, fmt.Errorf("hdfs catalog only supports listing tables in a single namespace")
	}

	ns := namespace[0]
	tables := []table.Identifier{}
	h.bucket.Iter(ctx, ns, func(name string) error {
		tables = append(tables, table.Identifier{filepath.Base(name)})
		return nil
	})

	return tables, nil
}

func (h *hdfs) DropTable(ctx context.Context, identifier table.Identifier) error {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return err
	}

	// Drop the table's directory.
	return h.bucket.Delete(ctx, filepath.Join(ns, tbl))
}

func (h *hdfs) RenameTable(ctx context.Context, from, to table.Identifier) (table.Table, error) {
	return nil, fmt.Errorf("hdfs catalog does not support renaming tables")
}

func (h *hdfs) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	namespaces := []table.Identifier{}
	return namespaces, h.bucket.Iter(ctx, filepath.Join(parent...), func(name string) error {
		namespaces = append(namespaces, table.Identifier{name})
		return nil
	})
}

func (h *hdfs) CreateNamespace(ctx context.Context, namespace table.Identifier, _ iceberg.Properties) error {
	return fmt.Errorf("hdfs catalog does not support creating namespaces")
}

func (h *hdfs) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return fmt.Errorf("hdfs catalog does not support dropping namespaces")
}

func (h *hdfs) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, fmt.Errorf("hdfs catalog does not support loading namespace properties")
}

func (h *hdfs) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error) {
	return PropertiesUpdateSummary{}, fmt.Errorf("hdfs catalog does not support updating namespace properties")
}

func (h *hdfs) LoadTable(ctx context.Context, identifier table.Identifier, _ iceberg.Properties) (table.Table, error) {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return nil, err
	}

	// Load the latest version of the table.
	t, err := h.loadLatestTable(ctx, identifier, ns, tbl)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (h *hdfs) CreateTable(ctx context.Context, location string, schema *iceberg.Schema, props iceberg.Properties) (table.Table, error) {
	// TODO: upload the metadata file to the bucket?
	metadata := table.NewMetadataV1Builder(
		location,
		schema,
		time.Now().UnixMilli(),
		schema.NumFields(),
	).
		WithTableUUID(uuid.New()).
		WithCurrentSchemaID(schema.ID).
		Build()

	return table.NewHDFSTable(0, table.Identifier{location}, metadata, filepath.Join(location, hdfsTableMetadataDir, hdfsMetadataFileName(0)), h.bucket), nil
}

func (h *hdfs) loadLatestTable(ctx context.Context, identifier table.Identifier, ns, tbl string) (table.Table, error) {
	v, err := getTableVersion(ctx, h.bucket, ns, tbl)
	if err != nil {
		return nil, err
	}

	md, err := getTableMetadata(ctx, h.bucket, ns, tbl, v)
	if err != nil {
		return nil, err
	}

	return table.NewHDFSTable(v, identifier, md, filepath.Join(ns, tbl, hdfsTableMetadataDir, hdfsMetadataFileName(v)), h.bucket), nil
}

// getTableMetadata returns the metadata of the table at the specified version.
func getTableMetadata(ctx context.Context, bucket objstore.Bucket, ns, tbl string, version int) (table.Metadata, error) {
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hdfsTableMetadataDir, hdfsMetadataFileName(version)))
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata file: %w", err)
	}
	defer r.Close()

	md, err := table.ParseMetadata(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return md, nil
}

// getTableVersion returns the latest version of the table.
// FIXME: this could fallback to a version file scan instead of returning an error
func getTableVersion(ctx context.Context, bucket objstore.Bucket, ns, tbl string) (int, error) {
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hdfsTableMetadataDir, hdfsVersionHintFile))
	if err != nil {
		if bucket.IsObjNotFoundErr(err) { // Table does not exist.
			return -1, ErrorTableNotFound
		}
		return -1, fmt.Errorf("failed to get version hint file: %w", err)
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)
	scanner.Scan()
	b := scanner.Text()

	v, err := strconv.Atoi(b)
	if err != nil {
		return -1, fmt.Errorf("failed to parse version hint: %w", err)
	}

	return v, nil
}

func splitIdentForPath(ident table.Identifier) (string, string, error) {
	if len(ident) < 1 {
		return "", "", fmt.Errorf("%w: missing namespace or invalid identifier %v",
			ErrNoSuchTable, strings.Join(ident, "."))
	}

	return strings.Join(NamespaceFromIdent(ident), namespaceSeparator), TableNameFromIdent(ident), nil
}
