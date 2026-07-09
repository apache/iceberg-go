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

package gocloud

import (
	"testing"

	"github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
)

func TestParseGCSConfigUseJSONAPI(t *testing.T) {
	t.Run("defaults to disabled", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{})
		assert.Len(t, cfg.ClientOptions, 0)
	})

	t.Run("enables reads on true", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "true"})
		assert.Len(t, cfg.ClientOptions, 1)
	})

	t.Run("does not enable on false", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "false"})
		assert.Len(t, cfg.ClientOptions, 0)
	})

	t.Run("does not enable on invalid value", func(t *testing.T) {
		cfg := ParseGCSConfig(map[string]string{io.GCSUseJSONAPI: "not-a-bool"})
		assert.Len(t, cfg.ClientOptions, 0)
	})
}
