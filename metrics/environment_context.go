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

package metrics

import (
	"maps"
	"sync"

	"github.com/apache/iceberg-go"
)

const icebergVersionKey = "iceberg-version"

var environmentContext = struct {
	sync.RWMutex
	properties map[string]string
}{
	properties: make(map[string]string),
}

// EnvironmentContext returns a snapshot of the process-wide metadata attached
// to newly emitted reports. Iceberg's own version is always present and cannot
// be removed by caller-supplied properties. The returned map is independent of
// the stored context and may be modified by the caller.
func EnvironmentContext() map[string]string {
	environmentContext.RLock()
	defer environmentContext.RUnlock()

	result := map[string]string{icebergVersionKey: iceberg.Version()}
	maps.Copy(result, environmentContext.properties)
	result[icebergVersionKey] = iceberg.Version()

	return result
}

// SetEnvironmentContext replaces the caller-supplied report metadata. A copy
// is retained, so later changes to properties do not affect future reports.
// The built-in Iceberg version metadata is restored automatically.
func SetEnvironmentContext(properties map[string]string) {
	environmentContext.Lock()
	defer environmentContext.Unlock()

	environmentContext.properties = maps.Clone(properties)
	delete(environmentContext.properties, icebergVersionKey)
}

// SetEnvironmentProperty sets one process-wide report metadata entry.
func SetEnvironmentProperty(key, value string) {
	if key == icebergVersionKey {
		return
	}

	environmentContext.Lock()
	defer environmentContext.Unlock()

	if environmentContext.properties == nil {
		environmentContext.properties = make(map[string]string)
	}
	environmentContext.properties[key] = value
}

// RemoveEnvironmentProperty removes one process-wide report metadata entry.
func RemoveEnvironmentProperty(key string) {
	if key == icebergVersionKey {
		return
	}

	environmentContext.Lock()
	defer environmentContext.Unlock()

	delete(environmentContext.properties, key)
}
