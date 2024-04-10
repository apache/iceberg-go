package catalog

import (
	"bufio"
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/thanos-io/objstore"
)

const (
	hiveTableMetadataDir = "metadata"
	hiveVersionHintFile  = "version-hint.text"
)

func hiveMetadataFileName(version int) string {
	return fmt.Sprintf("v%d.metadata.json", version)
}

type hive struct {
	bucket objstore.Bucket
}

func NewHive(bucket objstore.Bucket) Catalog {
	return &hive{bucket: bucket}
}

func (h *hive) CatalogType() CatalogType {
	return Hive
}

func (h *hive) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if len(namespace) != 1 {
		return nil, fmt.Errorf("hive catalog only supports listing tables in a single namespace")
	}

	ns := namespace[0]
	tables := []table.Identifier{}
	h.bucket.Iter(ctx, ns, func(name string) error {
		tables = append(tables, table.Identifier{filepath.Base(name)})
		return nil
	})

	return tables, nil
}

func (h *hive) DropTable(ctx context.Context, identifier table.Identifier) error {
	ns, tbl, err := splitIdentForPath(identifier)
	if err != nil {
		return err
	}

	// Drop the table's directory.
	return h.bucket.Delete(ctx, filepath.Join(ns, tbl))
}

func (h *hive) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return nil, fmt.Errorf("hive catalog does not support renaming tables")
}

func (h *hive) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	namespaces := []table.Identifier{}
	return namespaces, h.bucket.Iter(ctx, filepath.Join(parent...), func(name string) error {
		namespaces = append(namespaces, table.Identifier{name})
		return nil
	})
}

func (h *hive) CreateNamespace(ctx context.Context, namespace table.Identifier, _ iceberg.Properties) error {
	return fmt.Errorf("hive catalog does not support creating namespaces")
}

func (h *hive) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return fmt.Errorf("hive catalog does not support dropping namespaces")
}

func (h *hive) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return nil, fmt.Errorf("hive catalog does not support loading namespace properties")
}

func (h *hive) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error) {
	return PropertiesUpdateSummary{}, fmt.Errorf("hive catalog does not support updating namespace properties")
}

func (h *hive) LoadTable(ctx context.Context, identifier table.Identifier, _ iceberg.Properties) (*table.Table, error) {
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

func (h *hive) loadLatestTable(ctx context.Context, identifier table.Identifier, ns, tbl string) (*table.Table, error) {
	v, err := getTableVersion(ctx, h.bucket, ns, tbl)
	if err != nil {
		return nil, err
	}

	md, err := getTableMetadata(ctx, h.bucket, ns, tbl, v)
	if err != nil {
		return nil, err
	}

	return table.New(identifier, md, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveMetadataFileName(md.Version())), h.bucket), nil
}

// getTableMetadata returns the metadata of the table at the specified version.
func getTableMetadata(ctx context.Context, bucket objstore.Bucket, ns, tbl string, version int) (table.Metadata, error) {
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveMetadataFileName(version)))
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
	r, err := bucket.Get(ctx, filepath.Join(ns, tbl, hiveTableMetadataDir, hiveVersionHintFile))
	if err != nil {
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
