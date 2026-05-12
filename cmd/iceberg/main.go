// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/alexflint/go-arg"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/catalog/hive"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/config"
	_ "github.com/apache/iceberg-go/io/gocloud"
	"github.com/apache/iceberg-go/table"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
)

// Subcommand structs

type ListCmd struct {
	Parent string `arg:"positional" help:"catalog parent namespace"`
}

type DescribeCmd struct {
	Identifier string `arg:"positional,required" help:"fully qualified identifier, or 'namespace'/'table' followed by an identifier"`
	Target     string `arg:"positional" help:"fully qualified namespace or table (when first arg is 'namespace' or 'table')"`
}

type SchemaCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type SpecCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type UuidCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type LocationCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
}

type FilesCmd struct {
	TableID string `arg:"positional,required" help:"full path to a table"`
	History bool   `arg:"--history" help:"show all snapshots"`
}

type RenameCmd struct {
	From string `arg:"positional,required" help:"source table identifier"`
	To   string `arg:"positional,required" help:"destination table identifier"`
}

// Create subcommands

type CreateNamespaceCmd struct {
	Identifier  string `arg:"positional,required" help:"fully qualified namespace"`
	Description string `arg:"--description" help:"description for the namespace"`
	LocationURI string `arg:"--location-uri" help:"location URI for the namespace"`
}

type CreateTableCmd struct {
	Identifier    string `arg:"positional,required" help:"fully qualified table"`
	Schema        string `arg:"--schema" help:"table schema in JSON"`
	InferSchema   string `arg:"--infer-schema" help:"infer schema from a local data file (parquet)"`
	Properties    string `arg:"--properties" help:"table properties as key=value pairs"`
	PartitionSpec string `arg:"--partition-spec" help:"partition spec as comma-separated field names"`
	SortOrder     string `arg:"--sort-order" help:"sort order as field:direction[:null-order]"`
	LocationURI   string `arg:"--location-uri" help:"location URI for the table"`
}

type CreateCmd struct {
	Namespace *CreateNamespaceCmd `arg:"subcommand:namespace" help:"create a namespace"`
	Table     *CreateTableCmd     `arg:"subcommand:table" help:"create a table"`
}

// Drop subcommands

type DropNamespaceCmd struct {
	Identifier string `arg:"positional,required" help:"fully qualified namespace"`
}

type DropTableCmd struct {
	Identifier string `arg:"positional,required" help:"fully qualified table"`
}

type DropCmd struct {
	Namespace *DropNamespaceCmd `arg:"subcommand:namespace" help:"drop a namespace"`
	Table     *DropTableCmd     `arg:"subcommand:table" help:"drop a table"`
}

// Properties subcommands

type PropsGetCmd struct {
	Type       string `arg:"positional,required" help:"'namespace' or 'table'"`
	Identifier string `arg:"positional,required" help:"fully qualified identifier"`
	PropName   string `arg:"positional" help:"property name (omit to list all)"`
}

type PropsSetCmd struct {
	Type       string `arg:"positional,required" help:"'namespace' or 'table'"`
	Identifier string `arg:"positional,required" help:"fully qualified identifier"`
	PropName   string `arg:"positional,required" help:"property name"`
	Value      string `arg:"positional,required" help:"property value"`
}

type PropsRemoveCmd struct {
	Type       string `arg:"positional,required" help:"'namespace' or 'table'"`
	Identifier string `arg:"positional,required" help:"fully qualified identifier"`
	PropName   string `arg:"positional,required" help:"property name"`
}

type PropertiesCmd struct {
	Get    *PropsGetCmd    `arg:"subcommand:get" help:"get properties"`
	Set    *PropsSetCmd    `arg:"subcommand:set" help:"set a property"`
	Remove *PropsRemoveCmd `arg:"subcommand:remove" help:"remove a property"`
}

// Compact subcommands

type CompactAnalyzeCmd struct {
	TableID        string `arg:"positional,required" help:"full path to a table"`
	TargetFileSize int64  `arg:"--target-file-size" default:"0" help:"target output file size in bytes"`
}

type CompactRunCmd struct {
	TableID                     string `arg:"positional,required" help:"full path to a table"`
	TargetFileSize              int64  `arg:"--target-file-size" default:"0" help:"target output file size in bytes"`
	PartialProgress             bool   `arg:"--partial-progress" help:"stage each group as a separate snapshot"`
	PreserveDeadEqualityDeletes bool   `arg:"--preserve-dead-equality-deletes" help:"keep equality-delete files that are provably dead after the rewrite (default: drop them — recommended for sustained CDC workloads)"`
}

type CompactCmd struct {
	Analyze *CompactAnalyzeCmd `arg:"subcommand:analyze" help:"analyze compaction plan"`
	Run     *CompactRunCmd     `arg:"subcommand:run" help:"run bin-pack compaction"`
}

// Top-level args

type Args struct {
	List       *ListCmd       `arg:"subcommand:list" help:"list tables or namespaces"`
	Describe   *DescribeCmd   `arg:"subcommand:describe" help:"describe a namespace or table"`
	Schema     *SchemaCmd     `arg:"subcommand:schema" help:"get the schema of a table"`
	Spec       *SpecCmd       `arg:"subcommand:spec" help:"return the partition spec of a table"`
	Uuid       *UuidCmd       `arg:"subcommand:uuid" help:"return the UUID of a table"`
	Location   *LocationCmd   `arg:"subcommand:location" help:"return the location of a table"`
	Create     *CreateCmd     `arg:"subcommand:create" help:"create a namespace or table"`
	Drop       *DropCmd       `arg:"subcommand:drop" help:"drop a namespace or table"`
	Files      *FilesCmd      `arg:"subcommand:files" help:"list all files of a table"`
	Rename     *RenameCmd     `arg:"subcommand:rename" help:"rename a table"`
	Properties *PropertiesCmd `arg:"subcommand:properties" help:"manage properties on tables/namespaces"`
	Compact    *CompactCmd    `arg:"subcommand:compact" help:"analyze or run bin-pack compaction"`
	Info       *InfoCmd       `arg:"subcommand:info" help:"show single-screen table summary"`

	Catalog     string `arg:"--catalog" default:"rest" help:"catalog type"`
	CatalogName string `arg:"--catalog-name" default:"default" help:"catalog name from config"`
	URI         string `arg:"--uri" help:"catalog URI"`
	Output      string `arg:"--output" default:"text" help:"output type (json/text)"`
	Credential  string `arg:"--credential" help:"credentials for the catalog"`
	Token       string `arg:"--token" help:"OAuth token (skip OAuth flow)"`
	Warehouse   string `arg:"--warehouse" help:"warehouse to use"`
	Scope       string `arg:"--scope" default:"catalog" help:"OAuth scope"`
	Config      string `arg:"--config" help:"path to configuration file"`

	RestOptions *config.RestOptions `arg:"-"`
}

func (Args) Description() string {
	return "Apache Iceberg CLI"
}

func (Args) Version() string {
	return "iceberg " + iceberg.Version()
}

func main() {
	ctx := context.Background()

	var args Args
	parser := arg.MustParse(&args)

	if parser.Subcommand() == nil {
		parser.WriteHelp(os.Stderr)
		os.Exit(1)
	}

	// Determine which flags were explicitly supplied on the command line so
	// that mergeConf can apply file-config values only for flags that were
	// not explicitly provided (i.e. still at their default).
	explicitFlags := make(map[string]bool)
	for _, a := range os.Args[1:] {
		if strings.HasPrefix(a, "--") {
			name := strings.SplitN(strings.TrimPrefix(a, "--"), "=", 2)[0]
			explicitFlags[name] = true
		}
	}

	fileCfg := config.ParseConfig(config.LoadConfig(args.Config), args.CatalogName)
	if fileCfg != nil {
		mergeConf(fileCfg, &args, explicitFlags)
	}

	// Validate nested subcommands before catalog init.
	switch {
	case args.Create != nil && args.Create.Namespace == nil && args.Create.Table == nil:
		_ = parser.WriteHelpForSubcommand(os.Stderr, "create")
		os.Exit(1)
	case args.Drop != nil && args.Drop.Namespace == nil && args.Drop.Table == nil:
		_ = parser.WriteHelpForSubcommand(os.Stderr, "drop")
		os.Exit(1)
	case args.Properties != nil && args.Properties.Get == nil && args.Properties.Set == nil && args.Properties.Remove == nil:
		_ = parser.WriteHelpForSubcommand(os.Stderr, "properties")
		os.Exit(1)
	case args.Compact != nil && args.Compact.Analyze == nil && args.Compact.Run == nil:
		_ = parser.WriteHelpForSubcommand(os.Stderr, "compact")
		os.Exit(1)
	}

	var output Output
	switch strings.ToLower(args.Output) {
	case "text":
		output = textOutput{}
	case "json":
		output = jsonOutput{}
	default:
		log.Fatal("unimplemented output type")
	}

	cat := initCatalog(ctx, args)

	switch {
	case args.List != nil:
		list(ctx, output, cat, args.List.Parent)
	case args.Describe != nil:
		runDescribe(ctx, output, cat, args.Describe)
	case args.Schema != nil:
		tbl := loadTable(ctx, output, cat, args.Schema.TableID)
		output.Schema(tbl.Schema())
	case args.Spec != nil:
		tbl := loadTable(ctx, output, cat, args.Spec.TableID)
		output.Spec(tbl.Spec())
	case args.Location != nil:
		tbl := loadTable(ctx, output, cat, args.Location.TableID)
		output.Text(tbl.Location())
	case args.Uuid != nil:
		tbl := loadTable(ctx, output, cat, args.Uuid.TableID)
		output.Uuid(tbl.Metadata().TableUUID())
	case args.Create != nil:
		runCreate(ctx, output, cat, args.Create)
	case args.Drop != nil:
		runDrop(ctx, output, cat, args.Drop)
	case args.Files != nil:
		tbl := loadTable(ctx, output, cat, args.Files.TableID)
		output.Files(tbl, args.Files.History)
	case args.Rename != nil:
		runRename(ctx, output, cat, args.Rename)
	case args.Properties != nil:
		runProperties(ctx, output, cat, args.Properties)
	case args.Compact != nil:
		runCompact(ctx, output, cat, args.Compact)
	case args.Info != nil:
		tbl := loadTable(ctx, output, cat, args.Info.TableID)
		output.Info(tbl)
	}
}

func initCatalog(ctx context.Context, args Args) catalog.Catalog {
	var (
		cat catalog.Catalog
		err error
	)

	switch catalog.Type(args.Catalog) {
	case catalog.REST:
		opts := []rest.Option{}
		if len(args.Token) > 0 {
			opts = append(opts, rest.WithOAuthToken(args.Token))
		} else if len(args.Credential) > 0 {
			opts = append(opts, rest.WithCredential(args.Credential))
		}

		if len(args.Warehouse) > 0 {
			opts = append(opts, rest.WithWarehouseLocation(args.Warehouse))
		}

		if len(args.Scope) > 0 {
			opts = append(opts, rest.WithScope(args.Scope))
		}

		if args.RestOptions != nil {
			if args.RestOptions.SigV4Enabled {
				opts = append(opts, rest.WithSigV4())
			}

			if args.RestOptions.SigningName != "" || args.RestOptions.SigningRegion != "" {
				opts = append(opts, rest.WithSigV4RegionSvc(args.RestOptions.SigningRegion, args.RestOptions.SigningName))
			}
		}

		if cat, err = rest.NewCatalog(ctx, "rest", args.URI, opts...); err != nil {
			log.Fatal(err)
		}
	case catalog.Glue:
		awscfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			log.Fatal(err)
		}

		opts := []glue.Option{
			glue.WithAwsConfig(awscfg),
		}
		cat = glue.NewCatalog(opts...)
	case catalog.Hive:
		props := iceberg.Properties{
			hive.URI: args.URI,
		}
		if len(args.Warehouse) > 0 {
			props[hive.Warehouse] = args.Warehouse
		}

		if cat, err = hive.NewCatalog(props); err != nil {
			log.Fatal(err)
		}
	case catalog.Hadoop:
		if cat, err = hadoop.NewCatalog("hadoop", args.Warehouse, iceberg.Properties{
			"warehouse": args.Warehouse,
		}); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unrecognized catalog type")
	}

	return cat
}

func runDescribe(ctx context.Context, output Output, cat catalog.Catalog, cmd *DescribeCmd) {
	// Support both:
	//   iceberg describe namespace my.ns
	//   iceberg describe my.ns.table
	entityType := "any"
	ident := cmd.Identifier

	switch cmd.Identifier {
	case "namespace":
		entityType = "ns"
		ident = cmd.Target

		if ident == "" {
			log.Fatal("missing IDENTIFIER for describe namespace")
		}
	case "table":
		entityType = "tbl"
		ident = cmd.Target

		if ident == "" {
			log.Fatal("missing IDENTIFIER for describe table")
		}
	}

	describe(ctx, output, cat, ident, entityType)
}

func runCreate(ctx context.Context, output Output, cat catalog.Catalog, cmd *CreateCmd) {
	switch {
	case cmd.Namespace != nil:
		ns := cmd.Namespace
		props := iceberg.Properties{}
		if ns.Description != "" {
			props["Description"] = ns.Description
		}

		if ns.LocationURI != "" {
			props["Location"] = ns.LocationURI
		}

		err := cat.CreateNamespace(ctx, catalog.ToIdentifier(ns.Identifier), props)
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}

		output.Text("Namespace " + ns.Identifier + " created successfully")
	case cmd.Table != nil:
		tbl := cmd.Table
		if tbl.Schema != "" && tbl.InferSchema != "" {
			output.Error(errors.New("--schema and --infer-schema are mutually exclusive"))
			os.Exit(1)
		}

		var schema *iceberg.Schema

		switch {
		case tbl.Schema != "":
			var err error

			schema, err = iceberg.NewSchemaFromJsonFields(0, tbl.Schema)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case tbl.InferSchema != "":
			var err error

			schema, err = schemaFromFile(tbl.InferSchema)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			output.Text("Inferred schema from " + tbl.InferSchema + ":")
			output.Schema(schema)
		default:
			output.Error(errors.New("missing --schema or --infer-schema for table creation"))
			os.Exit(1)
		}

		var opts []catalog.CreateTableOpt
		if tbl.LocationURI != "" {
			opts = append(opts, catalog.WithLocation(tbl.LocationURI))
		}

		if tbl.Properties != "" {
			props, err := parseProperties(tbl.Properties)
			if err != nil {
				output.Error(fmt.Errorf("failed to parse properties: %w", err))
				os.Exit(1)
			}

			opts = append(opts, catalog.WithProperties(props))
		}

		if tbl.PartitionSpec != "" {
			spec, err := parsePartitionSpec(tbl.PartitionSpec)
			if err != nil {
				output.Error(fmt.Errorf("failed to parse partition spec: %w", err))
				os.Exit(1)
			}

			opts = append(opts, catalog.WithPartitionSpec(spec))
		}

		if tbl.SortOrder != "" {
			sortOrder, err := parseSortOrder(tbl.SortOrder)
			if err != nil {
				output.Error(fmt.Errorf("failed to parse sort order: %w", err))
				os.Exit(1)
			}

			opts = append(opts, catalog.WithSortOrder(sortOrder))
		}

		ident := catalog.ToIdentifier(tbl.Identifier)
		_, err := cat.CreateTable(ctx, ident, schema, opts...)
		if err != nil {
			output.Error(fmt.Errorf("failed to create table: %w", err))
			os.Exit(1)
		}

		output.Text("Table " + tbl.Identifier + " created successfully")
	}
}

func runDrop(ctx context.Context, output Output, cat catalog.Catalog, cmd *DropCmd) {
	switch {
	case cmd.Namespace != nil:
		err := cat.DropNamespace(ctx, catalog.ToIdentifier(cmd.Namespace.Identifier))
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}
	case cmd.Table != nil:
		err := cat.DropTable(ctx, catalog.ToIdentifier(cmd.Table.Identifier))
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}
	}
}

func runRename(ctx context.Context, output Output, cat catalog.Catalog, cmd *RenameCmd) {
	_, err := cat.RenameTable(ctx,
		catalog.ToIdentifier(cmd.From), catalog.ToIdentifier(cmd.To))
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	output.Text("Renamed table from " + cmd.From + " to " + cmd.To)
}

func validateEntityType(t string) {
	if t != "namespace" && t != "table" {
		log.Fatalf("expected 'namespace' or 'table', got %q", t)
	}
}

func runProperties(ctx context.Context, output Output, cat catalog.Catalog, cmd *PropertiesCmd) {
	switch {
	case cmd.Get != nil:
		get := cmd.Get
		validateEntityType(get.Type)

		isNs := get.Type == "namespace"
		ident := catalog.ToIdentifier(get.Identifier)

		var props iceberg.Properties
		if isNs {
			var err error

			props, err = cat.LoadNamespaceProperties(ctx, ident)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			tbl := loadTable(ctx, output, cat, get.Identifier)
			props = tbl.Metadata().Properties()
		}

		if get.PropName == "" {
			output.DescribeProperties(props)

			return
		}

		if val, ok := props[get.PropName]; ok {
			output.Text(val)
		} else {
			output.Error(errors.New("could not find property " + get.PropName + " on namespace " + get.Identifier))
			os.Exit(1)
		}
	case cmd.Set != nil:
		set := cmd.Set
		validateEntityType(set.Type)

		isNs := set.Type == "namespace"
		ident := catalog.ToIdentifier(set.Identifier)

		output.Text("Setting " + set.PropName + "=" + set.Value + " on " + set.Identifier)
		if isNs {
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				nil, iceberg.Properties{set.PropName: set.Value})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			tbl := loadTable(ctx, output, cat, set.Identifier)
			_, _, err := cat.CommitTable(ctx, tbl.Identifier(), nil,
				[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{set.PropName: set.Value})})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}

		output.Text("Updated " + set.PropName + " on " + set.Identifier)
	case cmd.Remove != nil:
		rm := cmd.Remove
		validateEntityType(rm.Type)

		isNs := rm.Type == "namespace"
		ident := catalog.ToIdentifier(rm.Identifier)

		output.Text("Removing " + rm.PropName + " on " + rm.Identifier)
		if isNs {
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				[]string{rm.PropName}, nil)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			tbl := loadTable(ctx, output, cat, rm.Identifier)
			_, _, err := cat.CommitTable(ctx, tbl.Identifier(), nil,
				[]table.Update{table.NewRemovePropertiesUpdate([]string{rm.PropName})})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}

		output.Text("Removed " + rm.PropName + " from " + rm.Identifier)
	}
}

func runCompact(ctx context.Context, output Output, cat catalog.Catalog, cmd *CompactCmd) {
	switch {
	case cmd.Analyze != nil:
		compact(ctx, output, cat, compactConfig{
			tableID:        cmd.Analyze.TableID,
			targetFileSize: cmd.Analyze.TargetFileSize,
			analyzeOnly:    true,
		})
	case cmd.Run != nil:
		compact(ctx, output, cat, compactConfig{
			tableID:                     cmd.Run.TableID,
			targetFileSize:              cmd.Run.TargetFileSize,
			partialProgress:             cmd.Run.PartialProgress,
			preserveDeadEqualityDeletes: cmd.Run.PreserveDeadEqualityDeletes,
		})
	}
}

func list(ctx context.Context, output Output, cat catalog.Catalog, parent string) {
	prnt := catalog.ToIdentifier(parent)

	var ids []table.Identifier

	if parent != "" {
		iter := cat.ListTables(ctx, prnt)
		for id, err := range iter {
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			ids = append(ids, id)
		}
	}

	if len(ids) == 0 {
		ns, err := cat.ListNamespaces(ctx, prnt)
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}

		ids = ns
	}

	output.Identifiers(ids)
}

func describe(ctx context.Context, output Output, cat catalog.Catalog, id string, entityType string) {
	ident := catalog.ToIdentifier(id)

	isNS, isTbl := false, false
	if (entityType == "any" || entityType == "ns") && len(ident) > 0 {
		nsprops, err := cat.LoadNamespaceProperties(ctx, ident)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchNamespace) {
				if entityType != "any" || len(ident) == 1 {
					output.Error(err)
					os.Exit(1)
				}
			} else {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			isNS = true
			output.DescribeProperties(nsprops)
		}
	}

	if (entityType == "any" || entityType == "tbl") && len(ident) > 1 {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			if !errors.Is(err, catalog.ErrNoSuchTable) || entityType != "any" {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			isTbl = true
			output.DescribeTable(tbl)
		}
	}

	if !isNS && !isTbl {
		output.Error(fmt.Errorf("%w: table or namespace does not exist: %s",
			catalog.ErrNoSuchNamespace, ident))
		os.Exit(1)
	}
}

func loadTable(ctx context.Context, output Output, cat catalog.Catalog, id string) *table.Table {
	tbl, err := cat.LoadTable(ctx, catalog.ToIdentifier(id))
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	return tbl
}

// mergeConf applies values from the file config into args for any option
// that was not explicitly provided on the command line. explicitFlags is a set
// of flag names (without the "--" prefix) that appeared in os.Args so that
// CLI-provided values always take precedence over the file config.
func mergeConf(fileConf *config.CatalogConfig, args *Args, explicitFlags map[string]bool) {
	if !explicitFlags["catalog"] && len(fileConf.CatalogType) > 0 {
		args.Catalog = fileConf.CatalogType
	}

	if !explicitFlags["uri"] && len(fileConf.URI) > 0 {
		args.URI = fileConf.URI
	}

	if !explicitFlags["output"] && len(fileConf.Output) > 0 {
		args.Output = fileConf.Output
	}

	if !explicitFlags["credential"] && len(fileConf.Credential) > 0 {
		args.Credential = fileConf.Credential
	}

	if !explicitFlags["warehouse"] && len(fileConf.Warehouse) > 0 {
		args.Warehouse = fileConf.Warehouse
	}

	if fileConf.RestOptions != nil {
		args.RestOptions = fileConf.RestOptions
	}
}

func schemaFromFile(path string) (*iceberg.Schema, error) {
	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".parquet", ".parq":
		return schemaFromParquetFile(path)
	default:
		return nil, fmt.Errorf("unsupported file format %s for %s: only .parquet and .parq files are supported", ext, path)
	}
}

func schemaFromParquetFile(path string) (*iceberg.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	pqReader, err := file.NewParquetReader(f)
	if err != nil {
		f.Close()

		return nil, fmt.Errorf("failed to read parquet file: %w", err)
	}
	defer pqReader.Close() // also closes underlying file

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet schema: %w", err)
	}

	arrowSchema, err := arrowReader.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to get arrow schema: %w", err)
	}

	// Prefer existing field IDs from the Parquet file (written by Iceberg-aware
	// tools like Spark or PyIceberg). Fall back to fresh sequential IDs only
	// when the error is specifically about missing field IDs.
	schema, err := table.ArrowSchemaToIceberg(arrowSchema, true, nil)
	if err != nil {
		if errors.Is(err, iceberg.ErrInvalidSchema) && strings.Contains(err.Error(), "field-id") {
			return table.ArrowSchemaToIcebergWithFreshIDs(arrowSchema, true)
		}

		return nil, fmt.Errorf("failed to convert parquet schema to iceberg: %w", err)
	}

	return schema, nil
}
