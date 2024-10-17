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
	"github.com/apache/iceberg-go/table"
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

Arguments:
  PARENT         Catalog parent namespace
  IDENTIFIER     fully qualified namespace or table
  TABLE_ID       full path to a table
  PROPNAME       name of a property
  VALUE          value to set

Options:
  -h --help          	show this helpe messages and exit
  --catalog TEXT     	specify the catalog type [default: rest]
  --uri TEXT         	specify the catalog URI
  --output TYPE      	output type (json/text) [default: text]
  --credential TEXT  	specify credentials for the catalog
  --warehouse TEXT   	specify the warehouse to use
  --description TEXT 	specify a description for the namespace
  --location-uri TEXT  	specify a location URI for the namespace`

func main() {
	args, err := docopt.ParseArgs(usage, os.Args[1:], iceberg.Version())
	if err != nil {
		log.Fatal(err)
	}

	cfg := struct {
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

		Catalog     string `docopt:"--catalog"`
		URI         string `docopt:"--uri"`
		Output      string `docopt:"--output"`
		History     bool   `docopt:"--history"`
		Cred        string `docopt:"--credential"`
		Warehouse   string `docopt:"--warehouse"`
		Description string `docopt:"--description"`
		LocationURI string `docopt:"--location-uri"`
	}{}

	if err := args.Bind(&cfg); err != nil {
		log.Fatal(err)
	}

	var output Output
	switch strings.ToLower(cfg.Output) {
	case "text":
		output = text{}
	case "json":
		fallthrough
	default:
		log.Fatal("unimplemented output type")
	}

	var cat catalog.Catalog
	switch catalog.CatalogType(cfg.Catalog) {
	case catalog.REST:
		opts := []catalog.Option[catalog.RestCatalog]{}
		if len(cfg.Cred) > 0 {
			opts = append(opts, catalog.WithCredential(cfg.Cred))
		}

		if len(cfg.Warehouse) > 0 {
			opts = append(opts, catalog.WithWarehouseLocation(cfg.Warehouse))
		}

		if cat, err = catalog.NewRestCatalog("rest", cfg.URI, opts...); err != nil {
			log.Fatal(err)
		}
	case catalog.Glue:
		opts := []catalog.Option[catalog.GlueCatalog]{}
		cat = catalog.NewGlueCatalog(opts...)
	default:
		log.Fatal("unrecognized catalog type")
	}

	switch {
	case cfg.List:
		list(output, cat, cfg.Parent)
	case cfg.Describe:
		entityType := "any"
		if cfg.Namespace {
			entityType = "ns"
		} else if cfg.Table {
			entityType = "tbl"
		}

		describe(output, cat, cfg.Ident, entityType)
	case cfg.Schema:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Schema(tbl.Schema())
	case cfg.Spec:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Spec(tbl.Spec())
	case cfg.Location:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Text(tbl.Location())
	case cfg.Uuid:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Uuid(tbl.Metadata().TableUUID())
	case cfg.Props:
		properties(output, cat, propCmd{
			get: cfg.Get, set: cfg.Set, remove: cfg.Remove,
			namespace: cfg.Namespace, table: cfg.Table,
			identifier: cfg.Ident,
			propname:   cfg.PropName,
			value:      cfg.Value,
		})
	case cfg.Rename:
		_, err := cat.RenameTable(context.Background(),
			catalog.ToRestIdentifier(cfg.RenameFrom), catalog.ToRestIdentifier(cfg.RenameTo))
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}

		output.Text("Renamed table from " + cfg.RenameFrom + " to " + cfg.RenameTo)
	case cfg.Drop:
		switch {
		case cfg.Namespace:
			err := cat.DropNamespace(context.Background(), catalog.ToRestIdentifier(cfg.Ident))
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case cfg.Table:
			err := cat.DropTable(context.Background(), catalog.ToRestIdentifier(cfg.Ident))
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

			err := cat.CreateNamespace(context.Background(), catalog.ToRestIdentifier(cfg.Ident), props)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		default:
			output.Error(errors.New("not implemented"))
			os.Exit(1)
		}
	case cfg.Files:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Files(tbl, cfg.History)
	}
}

func list(output Output, cat catalog.Catalog, parent string) {
	prnt := catalog.ToRestIdentifier(parent)

	ids, err := cat.ListNamespaces(context.Background(), prnt)
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	if len(ids) == 0 && parent != "" {
		ids, err = cat.ListTables(context.Background(), prnt)
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}
	}
	output.Identifiers(ids)
}

func describe(output Output, cat catalog.Catalog, id string, entityType string) {
	ctx := context.Background()

	ident := catalog.ToRestIdentifier(id)

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
		tbl, err := cat.LoadTable(ctx, ident, nil)
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

func loadTable(output Output, cat catalog.Catalog, id string) *table.Table {
	tbl, err := cat.LoadTable(context.Background(), catalog.ToRestIdentifier(id), nil)
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

func properties(output Output, cat catalog.Catalog, args propCmd) {
	ctx, ident := context.Background(), catalog.ToRestIdentifier(args.identifier)

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
			tbl := loadTable(output, cat, args.identifier)
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
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				nil, iceberg.Properties{args.propname: args.value})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			output.Text("updated " + args.propname + " on " + args.identifier)
		case args.table:
			loadTable(output, cat, args.identifier)
			output.Text("Setting " + args.propname + "=" + args.value + " on " + args.identifier)
			output.Error(errors.New("not implemented: Writing is WIP"))
		}
	case args.remove:
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(ctx, ident,
				[]string{args.propname}, nil)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			output.Text("removing " + args.propname + " from " + args.identifier)
		case args.table:
			loadTable(output, cat, args.identifier)
			output.Text("Setting " + args.propname + "=" + args.value + " on " + args.identifier)
			output.Error(errors.New("not implemented: Writing is WIP"))
		}
	}
}
