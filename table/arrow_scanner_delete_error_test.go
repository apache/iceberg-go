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

package table

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go/table/internal"
	"github.com/stretchr/testify/require"
)

func TestCreateIteratorReleasesQueuedBatchesAfterDeleteError(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)

	expectedErr := errors.New("delete load failed")
	records := make(chan enumeratedRecord, 2)
	records <- enumeratedRecord{
		Record: internal.Enumerated[arrow.RecordBatch]{
			Value: checkedInt64RecordBatch(mem, 1),
			Index: 0,
			Last:  true,
		},
		Task: internal.Enumerated[FileScanTask]{Index: 1, Last: true},
	}
	records <- enumeratedRecord{
		Task: internal.Enumerated[FileScanTask]{Index: 0},
		Err:  expectedErr,
	}
	close(records)

	ctx, cancel := context.WithCancelCause(context.Background())
	var gotErr error
	for _, err := range createIterator(ctx, 2, records, nil, cancel, 0) {
		gotErr = err
	}

	require.ErrorIs(t, gotErr, expectedErr)
}
