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

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// LoadFSWithRegistry uses the global FileIO registry when registry is nil.
// A non-nil (including empty) registry is strictly isolated.
func LoadFSWithRegistry(ctx context.Context, registry *icebergio.Registry, props map[string]string, location string) (icebergio.IO, error) {
	if registry == nil {
		return icebergio.LoadFS(ctx, props, location)
	}

	return registry.LoadFS(ctx, props, location)
}

// LoadFSFuncWithRegistry creates a lazy FileIO loader bound to the selected registry.
// Choosing the loader here prevents later table reads from reverting to the global registry.
func LoadFSFuncWithRegistry(registry *icebergio.Registry, props map[string]string, location string) table.FSysF {
	if registry == nil {
		return icebergio.LoadFSFunc(props, location)
	}

	return registry.LoadFSFunc(props, location)
}
