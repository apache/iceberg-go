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

package resttest

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/internal"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

func (s *server) catalogProps() iceberg.Properties {
	props := iceberg.Properties{}
	if s.cfg.warehouse != "" {
		props["warehouse"] = s.cfg.warehouse
	}

	return props
}

// namespacePropsLocked reads namespace properties for the location helpers.
// It runs inside a handler that already holds s.mu.
func (s *server) namespacePropsLocked(_ context.Context, ns table.Identifier) (iceberg.Properties, error) {
	props, ok := s.namespaces[nsKey(ns)]
	if !ok {
		return nil, catalog.ErrNoSuchNamespace
	}

	return props, nil
}

func (s *server) lookupTableLocked(ns []string, name string) *tableEntry {
	return s.tables[tableKey(ns, name)]
}

func ident(ns []string, name string) table.Identifier {
	return append(slices.Clone(ns), name)
}

func displayName(ns []string, name string) string {
	return strings.Join(ident(ns, name), ".")
}

func (e *tableEntry) loadResponse() loadTableResponse {
	return loadTableResponse{
		MetadataLocation: e.metadataLoc,
		Metadata:         e.metadata,
		Config:           iceberg.Properties{},
	}
}

func (s *server) listTables(w http.ResponseWriter, r *http.Request, ns []string) {
	if !s.namespaceExistsLocked(ns) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}

	out := []identifier{}
	for _, t := range s.tables {
		if slices.Equal(t.namespace, ns) {
			out = append(out, identifier{Namespace: slices.Clone(t.namespace), Name: t.name})
		}
	}
	slices.SortFunc(out, func(a, b identifier) int { return strings.Compare(a.Name, b.Name) })

	writeJSON(w, http.StatusOK, listTablesResponse{Identifiers: out})
}

func (s *server) createTable(w http.ResponseWriter, r *http.Request, ns []string) {
	var req createTableRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" || req.Schema == nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "create table requires a name and a schema")

		return
	}
	if !s.namespaceExistsLocked(ns) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}
	if s.lookupTableLocked(ns, req.Name) != nil {
		writeError(w, r, http.StatusConflict, typeAlreadyExists, "table already exists: "+displayName(ns, req.Name))

		return
	}

	opts := []catalog.CreateTableOpt{catalog.WithProperties(req.Properties)}
	if req.Location != "" {
		opts = append(opts, catalog.WithLocation(req.Location))
	}
	if req.PartitionSpec != nil {
		opts = append(opts, catalog.WithPartitionSpec(&req.PartitionSpec.PartitionSpec))
	}
	if req.WriteOrder != nil {
		opts = append(opts, catalog.WithSortOrder(req.WriteOrder.SortOrder))
	}

	staged, err := internal.CreateStagedTable(r.Context(), s.catalogProps(), s.namespacePropsLocked,
		ident(ns, req.Name), req.Schema, opts...)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, err.Error())

		return
	}

	if req.StageCreate {
		// A staged create returns metadata without registering the table.
		// The client finishes it with an assert-create commit.
		writeJSON(w, http.StatusOK, loadTableResponse{Metadata: staged.Metadata(), Config: iceberg.Properties{}})

		return
	}

	if err := internal.WriteMetadata(r.Context(), staged.Table); err != nil {
		writeError(w, r, http.StatusInternalServerError, typeServerError, "write table metadata: "+err.Error())

		return
	}

	entry := &tableEntry{
		namespace:   slices.Clone(ns),
		name:        req.Name,
		metadataLoc: staged.MetadataLocation(),
		metadata:    staged.Metadata(),
	}
	s.tables[tableKey(ns, req.Name)] = entry

	writeJSON(w, http.StatusOK, entry.loadResponse())
}

func (s *server) loadTable(w http.ResponseWriter, r *http.Request, ns []string, name string) {
	entry := s.lookupTableLocked(ns, name)
	if entry == nil {
		writeError(w, r, http.StatusNotFound, typeNoSuchTable, "table does not exist: "+displayName(ns, name))

		return
	}

	writeJSON(w, http.StatusOK, entry.loadResponse())
}

func (s *server) tableExists(w http.ResponseWriter, r *http.Request, ns []string, name string) {
	if s.lookupTableLocked(ns, name) == nil {
		writeError(w, r, http.StatusNotFound, typeNoSuchTable, "")

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// dropTable removes the table pointer. purgeRequested is accepted but files
// are left in place.
func (s *server) dropTable(w http.ResponseWriter, r *http.Request, ns []string, name string) {
	key := tableKey(ns, name)
	if _, ok := s.tables[key]; !ok {
		writeError(w, r, http.StatusNotFound, typeNoSuchTable, "table does not exist: "+displayName(ns, name))

		return
	}

	delete(s.tables, key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) renameTable(w http.ResponseWriter, r *http.Request) {
	var req renameTableRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	src, dst := req.Source, req.Destination
	if src.Name == "" || dst.Name == "" || len(src.Namespace) == 0 || len(dst.Namespace) == 0 {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "rename requires a source and a destination")

		return
	}

	entry := s.lookupTableLocked(src.Namespace, src.Name)
	if entry == nil {
		writeError(w, r, http.StatusNotFound, typeNoSuchTable,
			"table does not exist: "+displayName(src.Namespace, src.Name))

		return
	}
	if !s.namespaceExistsLocked(dst.Namespace) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace,
			"namespace does not exist: "+strings.Join(dst.Namespace, "."))

		return
	}

	srcKey, dstKey := tableKey(src.Namespace, src.Name), tableKey(dst.Namespace, dst.Name)
	if srcKey == dstKey {
		w.WriteHeader(http.StatusNoContent)

		return
	}
	if _, exists := s.tables[dstKey]; exists {
		writeError(w, r, http.StatusConflict, typeAlreadyExists,
			"table already exists: "+displayName(dst.Namespace, dst.Name))

		return
	}

	delete(s.tables, srcKey)
	entry.namespace, entry.name = slices.Clone(dst.Namespace), dst.Name
	s.tables[dstKey] = entry
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) registerTable(w http.ResponseWriter, r *http.Request, ns []string) {
	var req registerTableRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" || req.MetadataLocation == "" {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "register requires a name and a metadata-location")

		return
	}
	if !s.namespaceExistsLocked(ns) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}
	if s.lookupTableLocked(ns, req.Name) != nil {
		writeError(w, r, http.StatusConflict, typeAlreadyExists, "table already exists: "+displayName(ns, req.Name))

		return
	}

	md, err := readMetadata(r.Context(), s.catalogProps(), req.MetadataLocation)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "read table metadata: "+err.Error())

		return
	}

	entry := &tableEntry{
		namespace:   slices.Clone(ns),
		name:        req.Name,
		metadataLoc: req.MetadataLocation,
		metadata:    md,
	}
	s.tables[tableKey(ns, req.Name)] = entry

	writeJSON(w, http.StatusOK, entry.loadResponse())
}

func readMetadata(ctx context.Context, props iceberg.Properties, loc string) (md table.Metadata, err error) {
	fs, err := icebergio.LoadFS(ctx, props, loc)
	if err != nil {
		return nil, err
	}
	f, err := fs.Open(ctx, loc)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return table.ParseMetadataBytes(data)
}

// commitTable applies a commit under the server lock: it validates the
// requirements against the current metadata, writes the next metadata file
// and swaps the pointer. A requirement that no longer holds is reported as
// 409, so the client returns table.ErrCommitFailed.
func (s *server) commitTable(w http.ResponseWriter, r *http.Request, ns []string, name string) {
	var req commitTableRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Identifier != nil && (req.Identifier.Name != name || !slices.Equal(req.Identifier.Namespace, ns)) {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "commit identifier does not match the request path")

		return
	}

	id := ident(ns, name)
	entry := s.lookupTableLocked(ns, name)

	var current *table.Table
	if entry != nil {
		current = table.New(id, entry.metadata, entry.metadataLoc,
			icebergio.LoadFSFunc(s.catalogProps(), entry.metadataLoc), nil)
	} else {
		// Only a staged create may commit to a table that does not exist.
		// Anything else would silently create the table.
		if !slices.ContainsFunc(req.Requirements, isAssertCreate) {
			writeError(w, r, http.StatusNotFound, typeNoSuchTable, "table does not exist: "+displayName(ns, name))

			return
		}
		if !s.namespaceExistsLocked(ns) {
			writeError(w, r, http.StatusNotFound, typeNoSuchNamespace,
				"namespace does not exist: "+strings.Join(ns, "."))

			return
		}
	}

	staged, err := internal.UpdateAndStageTable(r.Context(), s.catalogProps(), current, id,
		req.Requirements, req.Updates, nil)
	switch {
	case errors.Is(err, table.ErrCommitFailed):
		writeError(w, r, http.StatusConflict, typeCommitFailed, err.Error())

		return
	case errors.Is(err, catalog.ErrNoSuchTable):
		writeError(w, r, http.StatusNotFound, typeNoSuchTable, err.Error())

		return
	case err != nil:
		writeError(w, r, http.StatusBadRequest, typeBadRequest, err.Error())

		return
	}

	if entry != nil && staged.Metadata().Equals(entry.metadata) {
		writeJSON(w, http.StatusOK, commitTableResponse{MetadataLocation: entry.metadataLoc, Metadata: entry.metadata})

		return
	}

	if err := internal.WriteMetadata(r.Context(), staged.Table); err != nil {
		writeError(w, r, http.StatusInternalServerError, typeServerError, "write table metadata: "+err.Error())

		return
	}

	if entry == nil {
		entry = &tableEntry{namespace: slices.Clone(ns), name: name}
		s.tables[tableKey(ns, name)] = entry
	}
	entry.metadataLoc, entry.metadata = staged.MetadataLocation(), staged.Metadata()

	writeJSON(w, http.StatusOK, commitTableResponse{MetadataLocation: entry.metadataLoc, Metadata: entry.metadata})
}

func isAssertCreate(r table.Requirement) bool { return r.GetType() == "assert-create" }
