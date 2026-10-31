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
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Wire types for the REST catalog spec. The catalog/rest client keeps its own
// copies unexported, so the server defines the shapes it decodes and encodes.

type errorEnvelope struct {
	Error errorModel `json:"error"`
}

type errorModel struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

type configResponse struct {
	Defaults  iceberg.Properties `json:"defaults"`
	Overrides iceberg.Properties `json:"overrides"`
	Endpoints []string           `json:"endpoints"`
}

type identifier struct {
	Namespace []string `json:"namespace"`
	Name      string   `json:"name"`
}

type namespaceRequest struct {
	Namespace  []string           `json:"namespace"`
	Properties iceberg.Properties `json:"properties"`
}

type namespaceResponse struct {
	Namespace  []string           `json:"namespace"`
	Properties iceberg.Properties `json:"properties"`
}

type listNamespacesResponse struct {
	Namespaces [][]string `json:"namespaces"`
}

type updatePropertiesRequest struct {
	Removals []string           `json:"removals"`
	Updates  iceberg.Properties `json:"updates"`
}

type listTablesResponse struct {
	Identifiers []identifier `json:"identifiers"`
}

// createTableRequest decodes the partition spec and write order unbound: their
// source IDs refer to the schema in the same request and are bound when the
// metadata is built.
type createTableRequest struct {
	Name          string                        `json:"name"`
	Schema        *iceberg.Schema               `json:"schema"`
	Location      string                        `json:"location"`
	PartitionSpec *iceberg.UnboundPartitionSpec `json:"partition-spec"`
	WriteOrder    *table.UnboundSortOrder       `json:"write-order"`
	StageCreate   bool                          `json:"stage-create"`
	Properties    iceberg.Properties            `json:"properties"`
}

type loadTableResponse struct {
	MetadataLocation string             `json:"metadata-location,omitempty"`
	Metadata         table.Metadata     `json:"metadata"`
	Config           iceberg.Properties `json:"config"`
}

type commitTableRequest struct {
	Identifier   *identifier        `json:"identifier"`
	Requirements table.Requirements `json:"requirements"`
	Updates      table.Updates      `json:"updates"`
}

type commitTableResponse struct {
	MetadataLocation string         `json:"metadata-location"`
	Metadata         table.Metadata `json:"metadata"`
}

type renameTableRequest struct {
	Source      identifier `json:"source"`
	Destination identifier `json:"destination"`
}

type registerTableRequest struct {
	Name             string `json:"name"`
	MetadataLocation string `json:"metadata-location"`
}
