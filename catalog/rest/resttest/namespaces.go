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
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog/internal"
)

func (s *server) namespaceExistsLocked(ns []string) bool {
	_, ok := s.namespaces[nsKey(ns)]

	return ok
}

func (s *server) listNamespaces(w http.ResponseWriter, r *http.Request) {
	var parent []string
	if p := r.URL.Query().Get("parent"); p != "" {
		parent = splitNamespace(p)
		if !s.namespaceExistsLocked(parent) {
			writeError(w, r, http.StatusNotFound, typeNoSuchNamespace,
				"namespace does not exist: "+strings.Join(parent, "."))

			return
		}
	}

	out := [][]string{}
	for key := range s.namespaces {
		ns := splitNamespace(key)
		if len(ns) == len(parent)+1 && slices.Equal(ns[:len(parent)], parent) {
			out = append(out, ns)
		}
	}
	slices.SortFunc(out, func(a, b []string) int { return strings.Compare(nsKey(a), nsKey(b)) })

	writeJSON(w, http.StatusOK, listNamespacesResponse{Namespaces: out})
}

func (s *server) createNamespace(w http.ResponseWriter, r *http.Request) {
	var req namespaceRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Namespace) == 0 || slices.Contains(req.Namespace, "") {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, "namespace must have non-empty levels")

		return
	}
	if len(req.Namespace) > 1 && !s.namespaceExistsLocked(req.Namespace[:len(req.Namespace)-1]) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace,
			"parent namespace does not exist: "+strings.Join(req.Namespace[:len(req.Namespace)-1], "."))

		return
	}
	if s.namespaceExistsLocked(req.Namespace) {
		writeError(w, r, http.StatusConflict, typeAlreadyExists,
			"namespace already exists: "+strings.Join(req.Namespace, "."))

		return
	}

	props := maps.Clone(req.Properties)
	if props == nil {
		props = iceberg.Properties{}
	}
	s.namespaces[nsKey(req.Namespace)] = props

	writeJSON(w, http.StatusOK, namespaceResponse{Namespace: req.Namespace, Properties: maps.Clone(props)})
}

func (s *server) loadNamespace(w http.ResponseWriter, r *http.Request, ns []string) {
	props, ok := s.namespaces[nsKey(ns)]
	if !ok {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}

	writeJSON(w, http.StatusOK, namespaceResponse{Namespace: ns, Properties: maps.Clone(props)})
}

func (s *server) namespaceExists(w http.ResponseWriter, r *http.Request, ns []string) {
	if !s.namespaceExistsLocked(ns) {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "")

		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *server) dropNamespace(w http.ResponseWriter, r *http.Request, ns []string) {
	key := nsKey(ns)
	if _, ok := s.namespaces[key]; !ok {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}

	for other := range s.namespaces {
		if strings.HasPrefix(other, key+nsSep) {
			writeError(w, r, http.StatusConflict, typeNamespaceNotEmpty,
				"namespace has child namespaces: "+strings.Join(ns, "."))

			return
		}
	}
	for _, t := range s.tables {
		if slices.Equal(t.namespace, ns) {
			writeError(w, r, http.StatusConflict, typeNamespaceNotEmpty,
				"namespace contains tables: "+strings.Join(ns, "."))

			return
		}
	}

	delete(s.namespaces, key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) updateNamespaceProperties(w http.ResponseWriter, r *http.Request, ns []string) {
	var req updatePropertiesRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	key := nsKey(ns)
	current, ok := s.namespaces[key]
	if !ok {
		writeError(w, r, http.StatusNotFound, typeNoSuchNamespace, "namespace does not exist: "+strings.Join(ns, "."))

		return
	}

	updated, summary, err := internal.GetUpdatedPropsAndUpdateSummary(current, req.Removals, req.Updates)
	if err != nil {
		writeError(w, r, http.StatusBadRequest, typeBadRequest, err.Error())

		return
	}
	s.namespaces[key] = updated

	writeJSON(w, http.StatusOK, summary)
}
