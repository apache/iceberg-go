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
	"maps"
	"strings"

	"github.com/apache/iceberg-go"
)

// ResolveParquetBloomColumnPaths maps logical names by field ID, including
// sanitized names and list/map wrappers. Keep existing physical-path settings
// as a fallback; an explicit logical setting takes precedence.
func ResolveParquetBloomColumnPaths(props iceberg.Properties, tableSchema, fileSchema *iceberg.Schema, pathToID map[string]int, content iceberg.ManifestEntryContent) iceberg.Properties {
	prefixes := []string{ParquetBloomFilterColumnEnabledKeyPrefix + ".", ParquetBloomFilterColumnFPPKeyPrefix + ".", ParquetBloomFilterColumnNDVKeyPrefix + "."}
	configured := make(map[string]map[string]string)
	var mapped iceberg.Properties
	for key, value := range props {
		for _, prefix := range prefixes {
			if name, ok := strings.CutPrefix(key, prefix); ok {
				if mapped == nil {
					mapped = maps.Clone(props)
				}
				delete(mapped, key)
				if configured[prefix] == nil {
					configured[prefix] = make(map[string]string)
				}
				configured[prefix][name] = value

				break
			}
		}
	}
	if mapped == nil {
		return props
	}
	if content != iceberg.EntryContentData {
		return mapped
	}
	idToPath := make(map[int]string, len(pathToID))
	for path, id := range pathToID {
		idToPath[id] = path
	}
	logicalPath := func(name string) (string, bool) {
		var field iceberg.NestedField
		var found bool
		schema := tableSchema
		if schema != nil {
			field, found = schema.FindFieldByName(name)
		}
		if !found && fileSchema != nil {
			schema = fileSchema
			field, found = schema.FindFieldByName(name)
		}
		if !found {
			return "", false
		}
		canonical, _ := schema.FindColumnName(field.ID)
		if canonical != name {
			return "", false
		}

		return idToPath[field.ID], true
	}
	for prefix, settings := range configured {
		for name, value := range settings {
			if _, logical := logicalPath(name); logical {
				continue
			}
			if _, found := pathToID[name]; found {
				mapped[prefix+name] = value
			}
		}
		for name, value := range settings {
			if path, logical := logicalPath(name); logical && path != "" {
				mapped[prefix+path] = value
			}
		}
	}

	return mapped
}
