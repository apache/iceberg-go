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
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

type (
	binaryPartitionKey  string
	float32PartitionKey uint32
	float64PartitionKey uint64
)

const (
	// Canonical NaN payloads make all NaNs of the same width compare equal.
	// The width-specific key types preserve float width, while using raw bits
	// intentionally keeps -0.0 and +0.0 distinct.
	canonicalFloat32NaNBits uint32 = 0x7fc00000
	canonicalFloat64NaNBits uint64 = 0x7ff8000000000000
)

func ComparablePartitionKey(value any) any {
	switch value := value.(type) {
	case []byte:
		return binaryPartitionKey(value)
	case float32:
		rawBits := math.Float32bits(value)
		if math.IsNaN(float64(value)) {
			rawBits = canonicalFloat32NaNBits
		}

		return float32PartitionKey(rawBits)
	case float64:
		rawBits := math.Float64bits(value)
		if math.IsNaN(value) {
			rawBits = canonicalFloat64NaNBits
		}

		return float64PartitionKey(rawBits)
	}

	return value
}

// CanonicalPartitionKey identifies a partition by spec ID and post-transform
// values. Both the compaction planner and delete matching use this encoding.
// The key is opaque. The spec prefix is retained for compaction logs.
func CanonicalPartitionKey(specID int32, partition map[int]any) (string, error) {
	var storage [256]byte
	key := strconv.AppendInt(storage[:0], int64(specID), 10)
	key = append(key, ':')
	if len(partition) == 0 {
		return string(append(key, '_')), nil
	}

	key, err := AppendPartitionTuple(key, partition)
	if err != nil {
		return "", err
	}

	return string(key), nil
}

// AppendPartitionTuple appends a deterministic key for the partition values
// sorted by field ID. The caller can prefix the tuple with a spec ID.
func AppendPartitionTuple(dst []byte, partition map[int]any) ([]byte, error) {
	var idStorage [8]int
	ids := idStorage[:0]
	if len(partition) > len(idStorage) {
		ids = make([]int, 0, len(partition))
	}
	for id := range partition {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for _, id := range ids {
		dst = strconv.AppendInt(dst, int64(id), 10)
		dst = append(dst, ':')
		var err error
		dst, err = AppendCanonicalPartitionValue(dst, partition[id])
		if err != nil {
			return nil, fmt.Errorf("partition field %d: %w", id, err)
		}
		dst = append(dst, ';')
	}

	return dst, nil
}

// AppendCanonicalPartitionValue encodes one partition value into dst by
// semantic class, so different Go representations of the same logical
// value produce the same bytes:
//
//   - integer-backed types (int/int32/int64, Date, Time, Timestamp,
//     TimestampNano) collapse into one class — within a single
//     (specID, fieldID) the logical type is fixed, so this only
//     tolerates decoder representation drift, never conflates two
//     logical values of one field;
//   - uuid.UUID and []byte collapse into raw-bytes hex;
//   - Decimal and DecimalLiteral collapse into (scale, unscaled).
//
// String values are length-prefixed so keys stay injective for inputs
// containing the field separators. Unknown types return an error.
func AppendCanonicalPartitionValue(dst []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(dst, 'z'), nil
	case bool:
		return strconv.AppendBool(append(dst, 'b', ':'), v), nil
	case int:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case int32:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case int64:
		return strconv.AppendInt(append(dst, 'i', ':'), v, 10), nil
	case iceberg.Date:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case iceberg.Time:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case iceberg.Timestamp:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case iceberg.TimestampNano:
		return strconv.AppendInt(append(dst, 'i', ':'), int64(v), 10), nil
	case float32:
		return strconv.AppendFloat(append(dst, 'f', ':'), float64(v), 'x', -1, 64), nil
	case float64:
		return strconv.AppendFloat(append(dst, 'f', ':'), v, 'x', -1, 64), nil
	case string:
		dst = strconv.AppendInt(append(dst, 's', ':'), int64(len(v)), 10)

		return append(append(dst, ':'), v...), nil
	case []byte:
		return hex.AppendEncode(append(dst, 'B', ':'), v), nil
	case uuid.UUID:
		return hex.AppendEncode(append(dst, 'B', ':'), v[:]), nil
	case iceberg.Decimal:
		return appendCanonicalPartitionDecimal(dst, v), nil
	case iceberg.DecimalLiteral:
		return appendCanonicalPartitionDecimal(dst, iceberg.Decimal(v)), nil
	default:
		return nil, fmt.Errorf("unsupported partition value type %T in canonical key", v)
	}
}

func appendCanonicalPartitionDecimal(dst []byte, v iceberg.Decimal) []byte {
	dst = strconv.AppendInt(append(dst, 'd', ':'), int64(v.Scale), 10)

	return append(append(dst, ':'), v.Val.BigInt().String()...)
}
