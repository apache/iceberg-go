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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"
)

func GetMetadataLoc(location string, newVersion uint) string {
	return fmt.Sprintf("%s/metadata/%05d-%s.metadata.json",
		location, newVersion, uuid.New().String())
}

func WriteTableMetadata(metadata table.Metadata, fs io.WriteFileIO, loc string) error {
	out, err := fs.Create(loc)
	if err != nil {
		return nil
	}

	return errors.Join(
		json.NewEncoder(out).Encode(metadata),
		out.Close(),
	)
}

func WriteMetadata(ctx context.Context, metadata table.Metadata, loc string, props iceberg.Properties) error {
	fs, err := io.LoadFS(ctx, props, loc)
	if err != nil {
		return err
	}

	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return errors.New("filesystem IO does not support writing")
	}

	out, err := wfs.Create(loc)
	if err != nil {
		return nil
	}

	return errors.Join(
		json.NewEncoder(out).Encode(metadata),
		out.Close(),
	)
}

func UpdateTableMetadata(base table.Metadata, updates []table.Update, metadataLoc string) (table.Metadata, error) {
	bldr, err := table.MetadataBuilderFromBase(base)
	if err != nil {
		return nil, err
	}

	for _, update := range updates {
		if err := update.Apply(bldr); err != nil {
			return nil, err
		}
	}

	if bldr.HasChanges() {
		if metadataLoc != "" {
			maxMetadataLogEntries := max(1,
				base.Properties().GetInt(
					table.MetadataPreviousVersionsMaxKey, table.MetadataPreviousVersionsMaxDefault))

			bldr.TrimMetadataLogs(maxMetadataLogEntries + 1).
				AppendMetadataLog(table.MetadataLogEntry{
					MetadataFile: metadataLoc,
					TimestampMs:  base.LastUpdatedMillis(),
				})
		}
		if base.LastUpdatedMillis() == bldr.LastUpdatedMS() {
			bldr.SetLastUpdatedMS()
		}
	}

	return bldr.Build()
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
			io.LoadFSFunc(ioProps, metadataLoc),
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
			io.LoadFSFunc(updated.Properties(), newLocation),
			cat,
		),
	}, nil
}

func CreateViewMetadata(
	ctx context.Context,
	catalogName string,
	nsIdent []string,
	schema *iceberg.Schema,
	viewSQL string,
	loc string,
	props iceberg.Properties,
) (metadataLocation string, err error) {
	versionId := int64(1)
	timestampMs := time.Now().UnixMilli()

	viewVersion := struct {
		VersionID       int64             `json:"version-id"`
		TimestampMs     int64             `json:"timestamp-ms"`
		SchemaID        int               `json:"schema-id"`
		Summary         map[string]string `json:"summary"`
		Operation       string            `json:"operation"`
		Representations []struct {
			Type    string `json:"type"`
			SQL     string `json:"sql"`
			Dialect string `json:"dialect"`
		} `json:"representations"`
		DefaultCatalog   string   `json:"default-catalog"`
		DefaultNamespace []string `json:"default-namespace"`
	}{
		VersionID:   versionId,
		TimestampMs: timestampMs,
		SchemaID:    schema.ID,
		Summary:     map[string]string{"sql": viewSQL},
		Operation:   "create",
		Representations: []struct {
			Type    string `json:"type"`
			SQL     string `json:"sql"`
			Dialect string `json:"dialect"`
		}{
			{Type: "sql", SQL: viewSQL, Dialect: "default"},
		},
		DefaultCatalog:   catalogName,
		DefaultNamespace: nsIdent,
	}

	viewVersionBytes, err := json.Marshal(viewVersion)
	if err != nil {
		return "", fmt.Errorf("failed to marshal view version: %w", err)
	}

	if props == nil {
		props = iceberg.Properties{}
	}
	props["view-version"] = string(viewVersionBytes)
	props["view-format"] = "iceberg"
	props["view-sql"] = viewSQL

	metadataLocation = loc + "/metadata/view-" + uuid.New().String() + ".metadata.json"

	viewUUID := uuid.New().String()
	props["view-uuid"] = viewUUID

	viewMetadata := map[string]interface{}{
		"view-uuid":          viewUUID,
		"format-version":     1,
		"location":           loc,
		"schema":             schema,
		"current-version-id": versionId,
		"versions": map[string]interface{}{
			"1": viewVersion,
		},
		"properties": props,
		"version-log": []map[string]interface{}{
			{
				"timestamp-ms": timestampMs,
				"version-id":   versionId,
			},
		},
	}

	viewMetadataBytes, err := json.Marshal(viewMetadata)
	if err != nil {
		return "", fmt.Errorf("failed to marshal view metadata: %w", err)
	}

	fs, err := io.LoadFS(ctx, props, metadataLocation)
	if err != nil {
		return "", fmt.Errorf("failed to load filesystem for view metadata: %w", err)
	}

	wfs, ok := fs.(io.WriteFileIO)
	if !ok {
		return "", errors.New("filesystem IO does not support writing")
	}

	out, err := wfs.Create(metadataLocation)
	if err != nil {
		return "", fmt.Errorf("failed to create view metadata file: %w", err)
	}
	defer out.Close()

	if _, err := out.Write(viewMetadataBytes); err != nil {
		return "", fmt.Errorf("failed to write view metadata: %w", err)
	}

	return metadataLocation, nil
}

func LoadViewMetadata(ctx context.Context,
	props iceberg.Properties,
	metadataLocation string,
	viewName string,
	namespace string,
) (map[string]interface{}, error) {
	// Initial metadata with basic information
	viewMetadata := map[string]interface{}{
		"name":              viewName,
		"namespace":         namespace,
		"metadata-location": metadataLocation,
	}

	// Load the filesystem
	fs, err := io.LoadFS(ctx, props, metadataLocation)
	if err != nil {
		return nil, fmt.Errorf("error loading view metadata: %w", err)
	}

	// Open the metadata file
	inputFile, err := fs.Open(metadataLocation)
	if err != nil {
		return viewMetadata, fmt.Errorf("error encountered loading view metadata: %w", err)
	}
	defer inputFile.Close()

	// Decode the complete metadata
	var fullViewMetadata map[string]interface{}
	if err := json.NewDecoder(inputFile).Decode(&fullViewMetadata); err != nil {
		return viewMetadata, fmt.Errorf("error encountered decoding view metadata: %w", err)
	}

	// Update the metadata with name, namespace and location
	fullViewMetadata["name"] = viewName
	fullViewMetadata["namespace"] = namespace
	fullViewMetadata["metadata-location"] = metadataLocation

	if props, ok := fullViewMetadata["properties"].(map[string]interface{}); ok {
		strProps := make(map[string]string)
		for k, v := range props {
			if str, ok := v.(string); ok {
				strProps[k] = str
			} else if vJson, err := json.Marshal(v); err == nil {
				strProps[k] = string(vJson)
			}
		}
		fullViewMetadata["properties"] = strProps
	}

	return fullViewMetadata, nil
}
