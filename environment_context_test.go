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
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func preserveEnvironmentProperties(t *testing.T, keys ...string) {
	t.Helper()
	previous := EnvironmentContext()
	t.Cleanup(func() {
		for _, key := range keys {
			if value, ok := previous[key]; ok {
				SetEnvironmentProperty(key, value)
			} else {
				RemoveEnvironmentProperty(key)
			}
		}
	})
}

func TestEnvironmentContextCopiesValues(t *testing.T) {
	const key = "test-engine"
	preserveEnvironmentProperties(t, key)

	SetEnvironmentProperty(key, "go")
	context := EnvironmentContext()
	context[key] = "changed"

	assert.Equal(t, "go", EnvironmentContext()[key])
	assert.Equal(t, Version(), EnvironmentContext()[environmentContextIcebergVersionKey])

	RemoveEnvironmentProperty(key)
	assert.NotContains(t, EnvironmentContext(), key)
}

func TestEnvironmentContextAllowsIcebergVersionMutation(t *testing.T) {
	preserveEnvironmentProperties(t, environmentContextIcebergVersionKey)

	SetEnvironmentProperty(environmentContextIcebergVersionKey, "custom")
	assert.Equal(t, "custom", EnvironmentContext()[environmentContextIcebergVersionKey])

	RemoveEnvironmentProperty(environmentContextIcebergVersionKey)
	assert.NotContains(t, EnvironmentContext(), environmentContextIcebergVersionKey)
}

func TestEnvironmentContextConcurrentAccess(t *testing.T) {
	const workers = 8
	const iterations = 100

	keys := make([]string, 0, workers)
	for worker := range workers {
		keys = append(keys, fmt.Sprintf("test-concurrent-%d", worker))
	}
	preserveEnvironmentProperties(t, keys...)

	var wg sync.WaitGroup
	for worker, key := range keys {
		wg.Add(1)
		go func(worker int, key string) {
			defer wg.Done()
			for iteration := range iterations {
				SetEnvironmentProperty(key, strconv.Itoa(iteration))
				_ = EnvironmentContext()[key]
				if (worker+iteration)%2 == 0 {
					RemoveEnvironmentProperty(key)
				}
			}
		}(worker, key)
	}

	wg.Wait()
}
