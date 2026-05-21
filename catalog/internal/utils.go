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

package internal

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdfs "io/fs"
	"maps"
	"net/url"
	"os"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/internal"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"
)

func WriteTableMetadata(metadata table.Metadata, fs icebergio.WriteFileIO, loc string, compression string) (err error) {
	switch compression {
	case table.MetadataCompressionCodecNone, table.MetadataCompressionCodecGzip, table.MetadataCompressionCodecZstd:
		// supported codecs
	default:
		return fmt.Errorf("unsupported write metadata compression codec: %s", compression)
	}

	out, err := fs.Create(loc)
	if err != nil {
		return err
	}
	defer internal.CheckedClose(out, &err)

	var writer io.Writer = out
	switch compression {
	case table.MetadataCompressionCodecGzip:
		gzw := gzip.NewWriter(out)
		writer = gzw
		defer internal.CheckedClose(gzw, &err)
	case table.MetadataCompressionCodecZstd:
		enc, zErr := zstd.NewWriter(out)
		if zErr != nil {
			return zErr
		}
		writer = enc
		defer internal.CheckedClose(enc, &err)
	}

	err = json.NewEncoder(writer).Encode(metadata)

	return
}

func WriteMetadata(ctx context.Context, metadata table.Metadata, loc string, props iceberg.Properties) error {
	fs, err := icebergio.LoadFS(ctx, props, loc)
	if err != nil {
		return err
	}

	wfs, ok := fs.(icebergio.WriteFileIO)
	if !ok {
		return errors.New("filesystem IO does not support writing")
	}

	compression := props.Get(table.MetadataCompressionKey, table.MetadataCompressionDefault)

	return WriteTableMetadata(metadata, wfs, loc, compression)
}

func UpdateTableMetadata(base table.Metadata, updates []table.Update, metadataLoc string) (table.Metadata, error) {
	return table.UpdateTableMetadata(base, updates, metadataLoc)
}

func CreateStagedTable(ctx context.Context, catprops iceberg.Properties, nspropsFn GetNamespacePropsFn, ident table.Identifier, sc *iceberg.Schema, opts ...catalog.CreateTableOpt) (table.StagedTable, error) {
	var cfg catalog.CreateTableCfg
	for _, opt := range opts {
		opt(&cfg)
	}

	dbIdent := catalog.NamespaceFromIdent(ident)
	tblname := catalog.TableNameFromIdent(ident)
	dbname := strings.Join(dbIdent, ".")

	loc, err := ResolveTableLocation(ctx, cfg.Location, dbname, tblname, catprops, nspropsFn)
	if err != nil {
		return table.StagedTable{}, err
	}

	provider, err := table.LoadLocationProvider(loc, cfg.Properties)
	if err != nil {
		return table.StagedTable{}, err
	}

	metadataLoc, err := provider.NewTableMetadataFileLocation(0)
	if err != nil {
		return table.StagedTable{}, err
	}

	metadata, err := table.NewMetadata(sc, cfg.PartitionSpec, cfg.SortOrder, loc, cfg.Properties)
	if err != nil {
		return table.StagedTable{}, err
	}

	ioProps := maps.Clone(catprops)
	maps.Copy(ioProps, cfg.Properties)

	return table.StagedTable{
		Table: table.New(
			ident,
			metadata,
			metadataLoc,
			icebergio.LoadFSFunc(ioProps, metadataLoc),
			nil,
		),
	}, nil
}

type GetNamespacePropsFn func(context.Context, table.Identifier) (iceberg.Properties, error)

func ResolveTableLocation(ctx context.Context, loc, dbname, tablename string, catprops iceberg.Properties, nsprops GetNamespacePropsFn) (string, error) {
	if len(loc) == 0 {
		dbprops, err := nsprops(ctx, strings.Split(dbname, "."))
		if err != nil {
			return "", err
		}

		return getDefaultWarehouseLocation(dbname, tablename, dbprops, catprops)
	}

	return strings.TrimSuffix(loc, "/"), nil
}

func getDefaultWarehouseLocation(dbname, tablename string, nsprops, catprops iceberg.Properties) (string, error) {
	if dblocation := nsprops.Get("location", ""); dblocation != "" {
		return url.JoinPath(dblocation, tablename)
	}

	if warehousepath := catprops.Get("warehouse", ""); warehousepath != "" {
		return url.JoinPath(warehousepath, dbname+".db", tablename)
	}

	return "", errors.New("no default path set, please specify a location when creating a table")
}

// (\d+)            -> version number
// -                -> separator
// ([\w-]{36})      -> UUID (36 characters, including hyphens)
// (?:\.\w+)?       -> optional codec name
// \.metadata\.json -> file extension
// var tableMetadataFileNameRegex = regexp.MustCompile(`^(\d+)-([\w-]{36})(?:\.\w+)?\.metadata\.json`)
var tableMetadataFileNameRegex = regexp.MustCompile(`^(\d+)-([\w-]{36})(?:\.\w+)?\.metadata\.json`)

func ParseMetadataVersion(location string) int {
	fileName := path.Base(location)
	matches := tableMetadataFileNameRegex.FindStringSubmatch(fileName)

	if len(matches) != 3 {
		return -1
	}

	if _, err := uuid.Parse(matches[2]); err != nil {
		return -1
	}

	v, err := strconv.Atoi(matches[1])
	if err != nil {
		return -1
	}

	return v
}

func UpdateAndStageTable(ctx context.Context, current *table.Table, ident table.Identifier, reqs []table.Requirement, updates []table.Update, cat table.CatalogIO) (*table.StagedTable, error) {
	var (
		baseMeta    table.Metadata
		metadataLoc string
	)

	if current != nil {
		for _, r := range reqs {
			if err := r.Validate(current.Metadata()); err != nil {
				return nil, err
			}
		}

		baseMeta = current.Metadata()
		metadataLoc = current.MetadataLocation()
	} else {
		var err error
		baseMeta, err = table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "", nil)
		if err != nil {
			return nil, err
		}
	}

	updated, err := UpdateTableMetadata(baseMeta, updates, metadataLoc)
	if err != nil {
		return nil, err
	}

	provider, err := table.LoadLocationProvider(updated.Location(), updated.Properties())
	if err != nil {
		return nil, err
	}

	newVersion := ParseMetadataVersion(metadataLoc) + 1
	newLocation, err := provider.NewTableMetadataFileLocation(newVersion)
	if err != nil {
		return nil, err
	}

	return &table.StagedTable{
		Table: table.New(
			ident,
			updated,
			newLocation,
			icebergio.LoadFSFunc(updated.Properties(), newLocation),
			cat,
		),
	}, nil
}

func normalizeURI(uri string) string {
	if strings.HasPrefix(uri, "file:") {
		// Clean "file:/", "file://", "file:///" to all consistently have "file:///" prefix
		cleaned := strings.TrimPrefix(uri, "file:")
		cleaned = strings.TrimPrefix(cleaned, "//")
		cleaned = strings.TrimPrefix(cleaned, "/")

		return "file:///" + cleaned
	}

	return uri
}

// PurgeTableFiles physically deletes all files under the table's warehouse location
// and any referenced files written outside the location root (e.g., via write.data.path
// or write.metadata.path properties).
// It is best-effort — if interrupted, orphaned files may remain.
func PurgeTableFiles(ctx context.Context, tbl *table.Table) error {
	fs, err := tbl.FS(ctx)
	if err != nil {
		return err
	}
	location := tbl.Metadata().Location()

	var errs []error
	fileSet := make(map[string]bool)

	// 1. Walk the table location directory tree to capture all local files
	if listable, ok := fs.(icebergio.ListableIO); ok {
		walkErr := listable.WalkDir(location, func(path string, d stdfs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() {
				fileSet[normalizeURI(path)] = true
			}

			return nil
		})
		if walkErr != nil {
			errs = append(errs, fmt.Errorf("failed walking directory %s: %w", location, walkErr))
		}
	}

	// 2. Union in manifest-referenced and metadata files (which might be outside the table location)
	if referencedFiles, refErr := tbl.GetReferencedFiles(fs); refErr == nil {
		for path := range referencedFiles {
			fileSet[normalizeURI(path)] = true
		}
	} else {
		errs = append(errs, fmt.Errorf("failed to get referenced files from metadata: %w", refErr))
	}

	// Convert to slice and sort for deterministic behavior
	files := make([]string, 0, len(fileSet))
	for f := range fileSet {
		files = append(files, f)
	}
	slices.Sort(files)

	if bulk, ok := fs.(icebergio.BulkRemovableIO); ok && len(files) > 0 {
		_, bulkErr := bulk.DeleteFiles(ctx, files)
		if bulkErr != nil {
			errs = append(errs, fmt.Errorf("bulk deletion failed: %w", bulkErr))
		}
	} else {
		for _, file := range files {
			if rmErr := fs.Remove(file); rmErr != nil && !os.IsNotExist(rmErr) {
				errs = append(errs, fmt.Errorf("failed to remove %s: %w", file, rmErr))
			}
		}
	}

	return errors.Join(errs...)
}
