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
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type failingWriteFileWriter struct {
	writeErr error
	abortErr error

	abortCalled bool
	closeCalled bool
}

func (w *failingWriteFileWriter) Write(arrow.RecordBatch) error {
	return w.writeErr
}

func (*failingWriteFileWriter) BytesWritten() int64 {
	return 0
}

func (w *failingWriteFileWriter) Close() (iceberg.DataFile, error) {
	w.closeCalled = true

	return nil, nil
}

func (w *failingWriteFileWriter) Abort() error {
	w.abortCalled = true

	return w.abortErr
}

func TestWriteDataFileBatchesAbortsOnWriteError(t *testing.T) {
	tests := []struct {
		name     string
		abortErr error
	}{
		{name: "abort succeeds"},
		{name: "abort fails", abortErr: errors.New("abort failed")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeErr := errors.New("write failed")
			writer := &failingWriteFileWriter{
				writeErr: writeErr,
				abortErr: tt.abortErr,
			}

			// The fake writer ignores the batch, so nil keeps this focused on cleanup.
			df, err := writeDataFileBatches(writer, []arrow.RecordBatch{nil})
			require.Nil(t, df)
			require.Error(t, err)

			assert.True(t, errors.Is(err, writeErr))
			if tt.abortErr != nil {
				assert.True(t, errors.Is(err, tt.abortErr))
			} else {
				assert.True(t, err == writeErr, "abort success should return the raw write error")
			}
			assert.True(t, writer.abortCalled)
			assert.False(t, writer.closeCalled)
		})
	}
}
