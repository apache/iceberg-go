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

package maintenance

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/require"
)

func TestDeleteOrphanFilesRejectsNegativeAgeBeforeScan(t *testing.T) {
	_, err := DeleteOrphanFiles(context.Background(), &table.Table{}, WithCleanupFilesOlderThan(-time.Nanosecond))
	require.EqualError(t, err, "orphan cleanup age must be non-negative")
}

func TestRetentionOptionsAcceptZeroAge(t *testing.T) {
	cfg := &orphanCleanupConfig{}
	WithCleanupFilesOlderThan(0)(cfg)
	require.NoError(t, cfg.validationErr)
}
