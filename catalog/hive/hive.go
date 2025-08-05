package hive

import (
	"context"
	"fmt"
	"iter"
	"net/url"
	"path"
	"strconv"
	"sync"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	cataloginternal "github.com/apache/iceberg-go/catalog/internal"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/beltran/gohive"
	hms "github.com/beltran/gohive/hive_metastore"
)

type metastoreClient interface {
	Close()
	GetAllTables(ctx context.Context, db string) ([]string, error)
	GetTable(ctx context.Context, db, tbl string) (*hms.Table, error)
	CreateTable(ctx context.Context, tbl *hms.Table) error
	DropTable(ctx context.Context, db, tbl string, deleteData bool) error
	AlterTable(ctx context.Context, db, tbl string, newTable *hms.Table) error
	GetAllDatabases(ctx context.Context) ([]string, error)
	CreateDatabase(ctx context.Context, db *hms.Database) error
	DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error
}

type gohiveClient struct{ *gohive.HiveMetastoreClient }

func (g gohiveClient) Close() { g.HiveMetastoreClient.Close() }
func (g gohiveClient) GetAllTables(ctx context.Context, db string) ([]string, error) {
	return g.Client.GetAllTables(ctx, db)
}
func (g gohiveClient) GetTable(ctx context.Context, db, tbl string) (*hms.Table, error) {
	return g.Client.GetTable(ctx, db, tbl)
}
func (g gohiveClient) CreateTable(ctx context.Context, tbl *hms.Table) error {
	return g.Client.CreateTable(ctx, tbl)
}
func (g gohiveClient) DropTable(ctx context.Context, db, tbl string, deleteData bool) error {
	return g.Client.DropTable(ctx, db, tbl, deleteData)
}
func (g gohiveClient) AlterTable(ctx context.Context, db, tbl string, newTable *hms.Table) error {
	return g.Client.AlterTable(ctx, db, tbl, newTable)
}
func (g gohiveClient) GetAllDatabases(ctx context.Context) ([]string, error) {
	return g.Client.GetAllDatabases(ctx)
}
func (g gohiveClient) CreateDatabase(ctx context.Context, db *hms.Database) error {
	return g.Client.CreateDatabase(ctx, db)
}
func (g gohiveClient) DropDatabase(ctx context.Context, name string, deleteData, cascade bool) error {
	return g.Client.DropDatabase(ctx, name, deleteData, cascade)
}

var connectToMetastore = func(host string, port int, auth string, cfg *gohive.MetastoreConnectConfiguration) (metastoreClient, error) {
	client, err := gohive.ConnectToMetastore(host, port, auth, cfg)
	if err != nil {
		return nil, err
	}
	return gohiveClient{client}, nil
}

// HiveCatalog implements the catalog.Catalog interface for the Hive Metastore.
// The catalog maintains an active Hive metastore client and automatically
// attempts to reconnect when operations fail due to a lost connection.
type HiveCatalog struct {
	host string
	port int
	auth string

	options *gohive.MetastoreConnectConfiguration
	client  metastoreClient

	mu sync.Mutex
}

// Config contains parameters used to establish a connection to the Hive
// metastore.
type Config struct {
	Host          string
	Port          int
	Auth          string
	Username      string
	Password      string
	TransportMode string
}

func init() {
	catalog.Register(string(catalog.Hive), catalog.RegistrarFunc(func(ctx context.Context, name string, p iceberg.Properties) (catalog.Catalog, error) {
		cfg := Config{
			Host:          p.Get("host", ""),
			Port:          p.GetInt("port", 0),
			Auth:          p.Get("auth", "NONE"),
			Username:      p.Get("username", ""),
			Password:      p.Get("password", ""),
			TransportMode: p.Get("transport-mode", ""),
		}

		if uri := p.Get("uri", ""); uri != "" {
			u, err := url.Parse(uri)
			if err != nil {
				return nil, fmt.Errorf("parse catalog URI: %w", err)
			}
			if host := u.Hostname(); host != "" {
				cfg.Host = host
			}
			if port := u.Port(); port != "" {
				v, err := strconv.Atoi(port)
				if err != nil {
					return nil, fmt.Errorf("parse port: %w", err)
				}
				cfg.Port = v
			}
			if auth := u.Query().Get("auth"); auth != "" {
				cfg.Auth = auth
			}
			if user := u.User; user != nil {
				cfg.Username = user.Username()
				cfg.Password, _ = user.Password()
			}
		}

		if cfg.Port == 0 {
			cfg.Port = 9083
		}

		return NewHiveCatalog(cfg)
	}))
}

// NewHiveCatalog initializes a HiveCatalog using the provided configuration. If
// the connection to the metastore cannot be established, an error is returned.
func NewHiveCatalog(cfg Config) (*HiveCatalog, error) {
	opts := gohive.NewMetastoreConnectConfiguration()
	if cfg.TransportMode != "" {
		opts.TransportMode = cfg.TransportMode
	}
	opts.Username = cfg.Username
	opts.Password = cfg.Password

	client, err := connectToMetastore(cfg.Host, cfg.Port, cfg.Auth, opts)
	if err != nil {
		return nil, fmt.Errorf("connect to metastore: %w", err)
	}

	return &HiveCatalog{
		host:    cfg.Host,
		port:    cfg.Port,
		auth:    cfg.Auth,
		options: opts,
		client:  client,
	}, nil
}

// reconnect attempts to re-establish the connection to the metastore.
func (c *HiveCatalog) reconnect() error {
	client, err := connectToMetastore(c.host, c.port, c.auth, c.options)
	if err != nil {
		return fmt.Errorf("reconnect to metastore: %w", err)
	}

	if c.client != nil {
		c.client.Close()
	}
	c.client = client
	return nil
}

// withRetry executes fn using the current metastore client. If fn returns an
// error, the catalog will attempt to reconnect and invoke fn again once.
func (c *HiveCatalog) withRetry(fn func(metastoreClient) error) error {
	c.mu.Lock()
	client := c.client
	c.mu.Unlock()

	if client == nil {
		if err := c.reconnect(); err != nil {
			return err
		}
		c.mu.Lock()
		client = c.client
		c.mu.Unlock()
	}

	if err := fn(client); err != nil {
		if rerr := c.reconnect(); rerr != nil {
			return err
		}

		c.mu.Lock()
		client = c.client
		c.mu.Unlock()
		return fn(client)
	}

	return nil
}

var _ catalog.Catalog = (*HiveCatalog)(nil)

// CatalogType returns the catalog type for this implementation.
func (c *HiveCatalog) CatalogType() catalog.Type {
	return catalog.Hive
}

// CreateTable creates a new Iceberg table.
func (c *HiveCatalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	// Stage metadata for the new table. This generates a metadata file on the
	// provided or derived table location using the helper from the catalog
	// internal package. The staged table is returned with a metadata file
	// location but the file still needs to be written.
	staged, err := cataloginternal.CreateStagedTable(ctx, nil,
		func(context.Context, table.Identifier) (iceberg.Properties, error) {
			// Namespace properties are not used by the Hive catalog at the
			// moment, so return an empty set.
			return iceberg.Properties{}, nil
		}, identifier, schema, opts...)
	if err != nil {
		return nil, err
	}

	// Write the metadata file to the file system specified by the table
	// location. The staged table already knows which file system to use.
	fs, err := staged.FS(ctx)
	if err != nil {
		return nil, err
	}
	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("loaded filesystem IO does not support writing")
	}
	if err := cataloginternal.WriteTableMetadata(staged.Metadata(), wfs, staged.MetadataLocation()); err != nil {
		return nil, err
	}

	// Build the Hive table definition using the staged metadata. Only the
	// fields required by the metastore are populated. The metadata location
	// is stored as a table property so that it can be retrieved later when
	// loading the table.
	database := identifier[0]
	tableName := identifier[1]

	metadataLocation := staged.MetadataLocation()
	sdLocation := path.Dir(metadataLocation)

	input := &hms.Table{
		DbName:    database,
		TableName: tableName,
		TableType: "ICEBERG",
		Parameters: map[string]string{
			"metadata_location": metadataLocation,
		},
		Sd: &hms.StorageDescriptor{
			Location: sdLocation,
		},
	}

	// Create the table using the metastore client. The withRetry helper
	// ensures that the operation is retried once if the connection was
	// dropped and then re-established.
	if err := c.withRetry(func(cl metastoreClient) error {
		return cl.CreateTable(ctx, input)
	}); err != nil {
		return nil, fmt.Errorf("create table %s.%s: %w", database, tableName, err)
	}

	// Finally load the table from the catalog to return a fully initialised
	// Iceberg table instance to the caller.
	return c.LoadTable(ctx, identifier, staged.Properties())
}

// CommitTable commits table metadata to the catalog.
func (c *HiveCatalog) CommitTable(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	panic("not implemented")
}

// ListTables returns identifiers of tables in the provided namespace.
func (c *HiveCatalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		if len(namespace) != 1 {
			yield(table.Identifier{}, fmt.Errorf("invalid namespace: %v", namespace))

			return
		}
		db := namespace[0]

		var tables []string
		if err := c.withRetry(func(cl metastoreClient) error {
			var err error
			tables, err = cl.GetAllTables(ctx, db)
			return err
		}); err != nil {
			yield(table.Identifier{}, err)
			return
		}

		for _, t := range tables {
			if !yield(table.Identifier{db, t}, nil) {
				return
			}
		}
	}
}

// LoadTable loads a table and returns its representation.
func (c *HiveCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	if len(identifier) != 2 {
		return nil, fmt.Errorf("invalid identifier: %v", identifier)
	}
	db := identifier[0]
	tbl := identifier[1]

	var hTable *hms.Table
	if err := c.withRetry(func(cl metastoreClient) error {
		var err error
		hTable, err = cl.GetTable(ctx, db, tbl)
		return err
	}); err != nil {
		return nil, fmt.Errorf("load table %s.%s: %w", db, tbl, err)
	}

	if hTable == nil {
		return nil, fmt.Errorf("load table %s.%s: no table returned", db, tbl)
	}
	metadataLocation, ok := hTable.Parameters["metadata_location"]
	if !ok || metadataLocation == "" {
		return nil, fmt.Errorf("not an Iceberg table")
	}

	if props == nil {
		props = iceberg.Properties{}
	}

	return table.NewFromLocation(
		ctx,
		identifier,
		metadataLocation,
		io.LoadFSFunc(props, metadataLocation),
		c,
	)
}

// DropTable removes a table from the catalog.
func (c *HiveCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	if len(identifier) != 2 {
		return fmt.Errorf("invalid identifier: %v", identifier)
	}
	db := identifier[0]
	tbl := identifier[1]

	return c.withRetry(func(cl metastoreClient) error {
		return cl.DropTable(ctx, db, tbl, true)
	})
}

// RenameTable renames a table in the catalog.
func (c *HiveCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	if len(from) != 2 || len(to) != 2 {
		return nil, fmt.Errorf("invalid identifiers: %v -> %v", from, to)
	}

	var tblObj *hms.Table
	if err := c.withRetry(func(cl metastoreClient) error {
		var err error
		tblObj, err = cl.GetTable(ctx, from[0], from[1])
		if err != nil {
			return err
		}
		tblObj.DbName = to[0]
		tblObj.TableName = to[1]
		return cl.AlterTable(ctx, from[0], from[1], tblObj)
	}); err != nil {
		return nil, fmt.Errorf("rename table %v to %v: %w", from, to, err)
	}

	return c.LoadTable(ctx, to, nil)
}

// CheckTableExists checks whether a table exists.
func (c *HiveCatalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	panic("not implemented")
}

// ListNamespaces lists available namespaces, optionally filtering by parent.
func (c *HiveCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	if len(parent) > 0 {
		return nil, fmt.Errorf("hive catalog does not support nested namespaces: %v", parent)
	}

	var dbs []string
	if err := c.withRetry(func(cl metastoreClient) error {
		var err error
		dbs, err = cl.GetAllDatabases(ctx)
		return err
	}); err != nil {
		return nil, err
	}

	namespaces := make([]table.Identifier, 0, len(dbs))
	for _, db := range dbs {
		namespaces = append(namespaces, table.Identifier{db})
	}

	return namespaces, nil
}

// CreateNamespace creates a namespace with optional properties.
func (c *HiveCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if len(namespace) != 1 {
		return fmt.Errorf("invalid namespace: %v", namespace)
	}

	db := &hms.Database{
		Name:       namespace[0],
		Parameters: map[string]string{},
	}
	if props != nil {
		db.Parameters = props
		if loc := props["location"]; loc != "" {
			db.LocationUri = loc
		}
	}

	return c.withRetry(func(cl metastoreClient) error {
		return cl.CreateDatabase(ctx, db)
	})
}

// DropNamespace drops the specified namespace and its tables.
func (c *HiveCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if len(namespace) != 1 {
		return fmt.Errorf("invalid namespace: %v", namespace)
	}
	db := namespace[0]

	return c.withRetry(func(cl metastoreClient) error {
		// deleteData=true ensures underlying data is deleted, cascade=true
		// removes all tables contained in the namespace.
		return cl.DropDatabase(ctx, db, true, true)
	})
}

// CheckNamespaceExists checks whether the namespace exists.
func (c *HiveCatalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	panic("not implemented")
}

// LoadNamespaceProperties loads properties for the given namespace.
func (c *HiveCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	panic("not implemented")
}

// UpdateNamespaceProperties updates properties on the namespace.
func (c *HiveCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	panic("not implemented")
}
