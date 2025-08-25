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
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/glue"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/config"
	"github.com/apache/iceberg-go/table"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/docopt/docopt-go"
)

const usage = `iceberg.

Usage:
  iceberg list [options] [PARENT]
  iceberg describe [options] [namespace | table] IDENTIFIER
  iceberg (schema | spec | uuid | location) [options] TABLE_ID
  iceberg create [options] (namespace | table) IDENTIFIER
  iceberg drop [options] (namespace | table) IDENTIFIER
  iceberg files [options] TABLE_ID [--history]
  iceberg rename [options] <from> <to>
  iceberg properties [options] get (namespace | table) IDENTIFIER [PROPNAME]
  iceberg properties [options] set (namespace | table) IDENTIFIER PROPNAME VALUE
  iceberg properties [options] remove (namespace | table) IDENTIFIER PROPNAME
  iceberg -h | --help | --version

Commands:
  describe    Describe a namespace or a table.
  list        List tables or namespaces.
  schema      Get the schema of the table.
  create      Create a namespace or a table.
  spec        Return the partition spec of the table.
  uuid        Return the UUID of the table.
  location    Return the location of the table.
  drop        Operations to drop a namespace or table.
  files       List all the files of the table.
  rename      Rename a table.
  properties  Properties on tables/namespaces.

Arguments:
  PARENT         Catalog parent namespace
  IDENTIFIER     fully qualified namespace or table
  TABLE_ID       full path to a table
  PROPNAME       name of a property
  VALUE          value to set

Options:
  -h --help          	show this help messages and exit
  --catalog TEXT     	specify the catalog type [default: rest]
  --uri TEXT         	specify the catalog URI
  --output TYPE      	output type (json/text) [default: text]
  --credential TEXT  	specify credentials for the catalog
  --warehouse TEXT   	specify the warehouse to use
  --scope TEXT       	specify the OAuth scope for authentication [default: catalog]
  --config TEXT      	specify the path to the configuration file
  --description TEXT 	specify a description for the namespace
  --location-uri TEXT  	specify a location URI for the namespace
  --schema JSON        	specify table schema in json (for create table use only)
                       	Ex: [{"name":"id","type":"int","required":false,"doc":"unique id"}]
  --properties TEXT 	specify table properties in key=value format (for create table use only)
						Ex:"format-version=2,write.format.default=parquet"
  --partition-spec TEXT specify partition spec as comma-separated field names(for create table use only)
						Ex:"field1,field2"
  --sort-order TEXT 	specify sort order as field:direction[:null-order] format(for create table use only)
						Ex:"field1:asc,field2:desc:nulls-first,field3:asc:nulls-last"`

type Config struct {
	List     bool `docopt:"list"`
	Describe bool `docopt:"describe"`
	Schema   bool `docopt:"schema"`
	Spec     bool `docopt:"spec"`
	Uuid     bool `docopt:"uuid"`
	Location bool `docopt:"location"`
	Props    bool `docopt:"properties"`
	Create   bool `docopt:"create"`
	Drop     bool `docopt:"drop"`
	Files    bool `docopt:"files"`
	Rename   bool `docopt:"rename"`

	Get    bool `docopt:"get"`
	Set    bool `docopt:"set"`
	Remove bool `docopt:"remove"`

	Namespace bool `docopt:"namespace"`
	Table     bool `docopt:"table"`

	RenameFrom string `docopt:"<from>"`
	RenameTo   string `docopt:"<to>"`

	Parent   string `docopt:"PARENT"`
	Ident    string `docopt:"IDENTIFIER"`
	TableID  string `docopt:"TABLE_ID"`
	PropName string `docopt:"PROPNAME"`
	Value    string `docopt:"VALUE"`

	Catalog       string `docopt:"--catalog"`
	URI           string `docopt:"--uri"`
	Output        string `docopt:"--output"`
	History       bool   `docopt:"--history"`
	Cred          string `docopt:"--credential"`
	Warehouse     string `docopt:"--warehouse"`
	Config        string `docopt:"--config"`
	Scope         string `docopt:"--scope"`
	Description   string `docopt:"--description"`
	LocationURI   string `docopt:"--location-uri"`
	SchemaStr     string `docopt:"--schema"`
	TableProps    string `docopt:"--properties"`
	PartitionSpec string `docopt:"--partition-spec"`
	SortOrder     string `docopt:"--sort-order"`
}

func main() {
	ctx := context.Background()
	args, err := docopt.ParseArgs(usage, os.Args[1:], iceberg.Version())
	if err != nil {
		log.Fatal(err)
	}

	cfg := Config{}

	if err := args.Bind(&cfg); err != nil {
		log.Fatal(err)
	}

	fileCfg := config.ParseConfig(config.LoadConfig(cfg.Config), "default")
	if fileCfg != nil {
		mergeConf(fileCfg, &cfg)
	}

	var output Output
	switch strings.ToLower(cfg.Output) {
	case "text":
		output = textOutput{}
	case "json":
		output = jsonOutput{}
	default:
		log.Fatal("unimplemented output type")
	}

	var cat catalog.Catalog
	switch catalog.Type(cfg.Catalog) {
	case catalog.REST:
		opts := []rest.Option{}
		if len(cfg.Cred) > 0 {
			opts = append(opts, rest.WithCredential(cfg.Cred))
		}

		if len(cfg.Warehouse) > 0 {
			opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
		}

		if len(cfg.Scope) > 0 {
			opts = append(opts, rest.WithScope(cfg.Scope))
		}

		if cat, err = rest.NewCatalog(ctx, "rest", cfg.URI, opts...); err != nil {
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
	default:
		log.Fatal("unrecognized catalog type")
	}

	switch {
	case cfg.List:
		list(ctx, output, cat, cfg.Parent)
	case cfg.Describe:
		entityType := "any"
		if cfg.Namespace {
			entityType = "ns"
		} else if cfg.Table {
			entityType = "tbl"
		}

		describe(ctx, output, cat, cfg.Ident, entityType)
	case cfg.Schema:
		tbl := loadTable(ctx, output, cat, cfg.TableID)
		output.Schema(tbl.Schema())
	case cfg.Spec:
		tbl := loadTable(ctx, output, cat, cfg.TableID)
		output.Spec(tbl.Spec())
	case cfg.Location:
		tbl := loadTable(ctx, output, cat, cfg.TableID)
		output.Text(tbl.Location())
	case cfg.Uuid:
		tbl := loadTable(ctx, output, cat, cfg.TableID)
		output.Uuid(tbl.Metadata().TableUUID())
	case cfg.Props:
		properties(ctx, output, cat, propCmd{
			get: cfg.Get, set: cfg.Set, remove: cfg.Remove,
			namespace: cfg.Namespace, table: cfg.Table,
			identifier: cfg.Ident,
			propname:   cfg.PropName,
			value:      cfg.Value,
		})
	case cfg.Rename:
		_, err := cat.RenameTable(ctx,
			catalog.ToIdentifier(cfg.RenameFrom), catalog.ToIdentifier(cfg.RenameTo))
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}

		output.Text("Renamed table from " + cfg.RenameFrom + " to " + cfg.RenameTo)
	case cfg.Drop:
		switch {
		case cfg.Namespace:
			err := cat.DropNamespace(ctx, catalog.ToIdentifier(cfg.Ident))
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case cfg.Table:
			err := cat.DropTable(ctx, catalog.ToIdentifier(cfg.Ident))
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}

	case cfg.Create:
		switch {
		case cfg.Namespace:
			props := iceberg.Properties{}
			if cfg.Description != "" {
				props["Description"] = cfg.Description
			}

			if cfg.LocationURI != "" {
				props["Location"] = cfg.LocationURI
			}

			err := cat.CreateNamespace(ctx, catalog.ToIdentifier(cfg.Ident), props)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
			output.Text("Namespace " + cfg.Ident + " created successfully")
		case cfg.Table:
			if cfg.SchemaStr == "" {
				output.Error(errors.New("missing --schema for table creation"))
				os.Exit(1)
			}

			schema, err := iceberg.NewSchemaFromJsonFields(0, cfg.SchemaStr)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			var opts []catalog.CreateTableOpt
			if cfg.LocationURI != "" {
				opts = append(opts, catalog.WithLocation(cfg.LocationURI))
			}
			if cfg.TableProps != "" {
				props, err := parseProperties(cfg.TableProps)
				if err != nil {
					output.Error(fmt.Errorf("failed to parse properties: %w", err))
					os.Exit(1)
				}
				opts = append(opts, catalog.WithProperties(props))
			}
			if cfg.PartitionSpec != "" {
				spec, err := parsePartitionSpec(cfg.PartitionSpec)
				if err != nil {
					output.Error(fmt.Errorf("failed to parse partition spec: %w", err))
					os.Exit(1)
				}
				opts = append(opts, catalog.WithPartitionSpec(spec))
			}

			if cfg.SortOrder != "" {
				sortOrder, err := parseSortOrder(cfg.SortOrder)
				if err != nil {
					output.Error(fmt.Errorf("failed to parse sort order: %w", err))
					os.Exit(1)
				}
				opts = append(opts, catalog.WithSortOrder(sortOrder))
			}

			ident := catalog.ToIdentifier(cfg.Ident)
			_, err = cat.CreateTable(ctx, ident, schema, opts...)
			if err != nil {
				output.Error(fmt.Errorf("failed to create table: %w", err))
				os.Exit(1)
			}
			output.Text("Table " + cfg.Ident + " created successfully")
		default:
			output.Error(errors.New("not implemented"))
			os.Exit(1)
		}
	case cfg.Files:
		tbl := loadTable(ctx, output, cat, cfg.TableID)
		output.Files(tbl, cfg.History)
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

type propCmd struct {
	get, set, remove bool
	namespace, table bool

	identifier, propname, value string
}

func properties(ctx context.Context, output Output, cat catalog.Catalog, args propCmd) {
	ident := catalog.ToIdentifier(args.identifier)

	switch {
	case args.get:
		var props iceberg.Properties
		switch {
		case args.namespace:
			var err error
			props, err = cat.LoadNamespaceProperties(ctx, ident)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case args.table:
			tbl := loadTable(ctx, output, cat, args.identifier)
			props = tbl.Metadata().Properties()
		}

		if args.propname == "" {
			output.DescribeProperties(props)

			return
		}

		if val, ok := props[args.propname]; ok {
			output.Text(val)
		} else {
			output.Error(errors.New("could not find property " + args.propname + " on namespace " + args.identifier))
			os.Exit(1)
		}
	case args.set:
		output.Text("Setting " + args.propname + "=" + args.value + " on " + args.identifier)
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				nil, iceberg.Properties{args.propname: args.value})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case args.table:
			tbl := loadTable(ctx, output, cat, args.identifier)
			_, _, err := cat.CommitTable(ctx, tbl.Identifier(), nil,
				[]table.Update{table.NewSetPropertiesUpdate(iceberg.Properties{args.propname: args.value})})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}
		output.Text("Updated " + args.propname + " on " + args.identifier)
	case args.remove:
		output.Text("Removing " + args.propname + " on " + args.identifier)
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				[]string{args.propname}, nil)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case args.table:
			tbl := loadTable(ctx, output, cat, args.identifier)

			_, _, err := cat.CommitTable(ctx, tbl.Identifier(), nil,
				[]table.Update{table.NewRemovePropertiesUpdate([]string{args.propname})})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}
		output.Text("Removed " + args.propname + " from " + args.identifier)
	}
}

func mergeConf(fileConf *config.CatalogConfig, resConfig *Config) {
	if len(resConfig.Catalog) == 0 {
		resConfig.Catalog = fileConf.CatalogType
	}
	if len(resConfig.URI) == 0 {
		resConfig.URI = fileConf.URI
	}
	if len(resConfig.Output) == 0 {
		resConfig.Output = fileConf.Output
	}
	if len(resConfig.Cred) == 0 {
		resConfig.Cred = fileConf.Credential
	}
	if len(resConfig.Warehouse) == 0 {
		resConfig.Warehouse = fileConf.Warehouse
	}
}
