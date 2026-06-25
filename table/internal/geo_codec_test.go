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

package internal_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/geoarrow/geoarrow-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// wktToWKB is a helper which converts Well Known Text (WKT) to Well Known Bytes (WKB).
// Note that return bytes are little endian.
func wktToWKB(s string) (geoarrow.WKBBytes, error) {
	geometry, err := wkt.Unmarshal(s)
	if err != nil {
		return nil, fmt.Errorf("parse WKT: %w", err)
	}

	wkbBytes, err := wkb.Marshal(geometry, wkb.NDR) // little endian
	if err != nil {
		return nil, fmt.Errorf("marshal WKB: %w", err)
	}

	return geoarrow.WKBBytes(wkbBytes), nil
}

func TestWKTToWKB(t *testing.T) {
	tests := []struct {
		name    string
		wkt     string
		wantWKB string
		wantErr error
	}{
		{
			name:    "point",
			wkt:     "POINT (30 10)",
			wantWKB: "01010000000000000000003e400000000000002440",
		},
		{
			name:    "linestring",
			wkt:     "LINESTRING (30 10, 10 30, 40 40)",
			wantWKB: "0102000000030000000000000000003e40000000000000244000000000000024400000000000003e4000000000000044400000000000004440",
		},
		{
			name:    "polygon",
			wkt:     "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))",
			wantWKB: "010300000001000000050000000000000000003e4000000000000024400000000000004440000000000000444000000000000034400000000000004440000000000000244000000000000034400000000000003e400000000000002440",
		},
		{
			name:    "multipoint",
			wkt:     "MULTIPOINT ((10 40), (40 30), (20 20), (30 20))",
			wantWKB: "010400000004000000010100000000000000000024400000000000004440010100000000000000000044400000000000003e4001010000000000000000003440000000000000344001010000000000000000003e400000000000003440",
		},
		{
			name:    "geometry_collection",
			wkt:     "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))",
			wantWKB: "010700000002000000010100000000000000000010400000000000001840010200000002000000000000000000104000000000000018400000000000001c400000000000002440",
		},
		{
			name:    "unknown wkt",
			wkt:     "POINT (30 10",
			wantErr: errors.New("parse WKT: syntax error: unexpected $end, expecting ')'"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := wktToWKB(tt.wkt)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.wantErr.Error())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantWKB, got.String())
		})
	}
}
