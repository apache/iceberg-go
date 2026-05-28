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
	"github.com/geoarrow/geoarrow-go"

	"github.com/twpayne/go-geom/encoding/wkb"
	"github.com/twpayne/go-geom/encoding/wkt"
)

// WKTToWKB is a helper which converts Well Known Text (WKT) to Well Known Bytes (WKB).
// Note that return bytes are little endian.
func WKTToWKB(s string) (geoarrow.WKBBytes, error) {
	geometry, err := wkt.Unmarshal(s)
	if err != nil {
		return nil, err
	}

	wkbBytes, err := wkb.Marshal(geometry, wkb.NDR) // little endian
	if err != nil {
		return nil, err
	}

	return geoarrow.WKBBytes(wkbBytes), nil
}
