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
	"math"
	"testing"
)

func TestIsRowLineageColumn(t *testing.T) {
	tests := []struct {
		name    string
		fieldID int
		want    bool
	}{
		{name: "row ID", fieldID: RowIDFieldID, want: true},
		{name: "last updated sequence", fieldID: LastUpdatedSequenceNumberFieldID, want: true},
		{name: "position delete file path", fieldID: DeleteFilePathFieldID},
		{name: "position delete position", fieldID: DeleteFilePosFieldID},
		{name: "file metadata column", fieldID: math.MaxInt32 - 1},
		{name: "other reserved ID", fieldID: MaxStructFieldID + 1},
		{name: "user column", fieldID: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRowLineageColumn(tt.fieldID); got != tt.want {
				t.Errorf("IsRowLineageColumn(%d) = %t, want %t", tt.fieldID, got, tt.want)
			}
		})
	}
}
