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

package iceberg

import (
	"maps"
	"sync"
)

const (
	// EnvironmentContextEngineNameKey identifies the engine name property.
	EnvironmentContextEngineNameKey = "engine-name"
	// EnvironmentContextEngineVersionKey identifies the engine version property.
	EnvironmentContextEngineVersionKey = "engine-version"

	environmentContextIcebergVersionKey = "iceberg-version"
)

var environmentContext = struct {
	sync.RWMutex
	properties map[string]string
}{
	properties: make(map[string]string),
}

var environmentContextInit sync.Once

func initializeEnvironmentContext() {
	environmentContextInit.Do(func() {
		environmentContext.Lock()
		environmentContext.properties[environmentContextIcebergVersionKey] = Version()
		environmentContext.Unlock()
	})
}

// EnvironmentContext returns an independent snapshot of the process-wide
// context used to populate report metadata. The returned map may be modified
// by the caller without changing the stored context.
func EnvironmentContext() map[string]string {
	initializeEnvironmentContext()

	environmentContext.RLock()
	defer environmentContext.RUnlock()

	return maps.Clone(environmentContext.properties)
}

// SetEnvironmentProperty sets one process-wide environment context property.
func SetEnvironmentProperty(key, value string) {
	initializeEnvironmentContext()

	environmentContext.Lock()
	defer environmentContext.Unlock()

	environmentContext.properties[key] = value
}

// RemoveEnvironmentProperty removes one process-wide environment context
// property.
func RemoveEnvironmentProperty(key string) {
	initializeEnvironmentContext()

	environmentContext.Lock()
	defer environmentContext.Unlock()

	delete(environmentContext.properties, key)
}
