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
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
)

// BuildPartitionMatchPredicate constructs a BooleanExpression matching all rows
// that belong to any of the given partitions. It is the core of dynamic
// partition overwrite (see https://github.com/apache/iceberg-go/issues/1216):
// the returned expression selects the existing data to delete before the new
// data is appended.
//
// partitions holds partition tuples as returned by DataFile.Partition(), each
// keyed by partition-field ID and carrying the field's (transformed) value. The
// result is an OR across distinct partitions, each clause an AND across the
// spec's fields:
//
//	transform(source) == value when the partition value is present
//	IsNaN(transform(source)) when the value is a floating-point NaN (x == NaN is never true)
//	IsNull(transform(source)) when the partition value is absent or nil
//
// Duplicate tuples collapse to a single clause, and an empty input yields
// AlwaysFalse (matching nothing). Callers are expected to pass a partitioned
// spec; dynamic partition overwrite rejects unpartitioned tables upstream.
//
// The transform is kept in the row predicate so the match is evaluated against
// the partition value rather than comparing the post-transform value directly
// with the source column. This is phase 1 of issue #1216. The current overwrite
// path cannot execute non-identity predicates for partial-file rewrites because
// source-column metrics are conservative and the Substrait row-filter converter
// rejects transformed terms; partition-level strict matching is still needed
// before this helper can drive those rewrites (tracked in issue #1215).
func BuildPartitionMatchPredicate(spec iceberg.PartitionSpec, schema *iceberg.Schema, partitions []map[int]any) (iceberg.BooleanExpression, error) {
	type fieldRef struct {
		id         int
		name       string
		transform  iceberg.Transform
		resultType iceberg.Type
	}

	var fields []fieldRef
	for _, f := range spec.Fields() {
		// Partition transforms currently have exactly one source column.
		if len(f.SourceIDs) != 1 {
			return nil, fmt.Errorf("%w: partition field %q must have exactly one source id, got %d",
				iceberg.ErrInvalidArgument, f.Name, len(f.SourceIDs))
		}

		sourceName, ok := schema.FindColumnName(f.SourceIDs[0])
		if !ok {
			return nil, fmt.Errorf("%w: partition field %q references unknown source id %d",
				iceberg.ErrInvalidArgument, f.Name, f.SourceIDs[0])
		}
		bound, err := iceberg.NewUnboundTransform(f.Transform, iceberg.Reference(sourceName)).Bind(schema, true)
		if err != nil {
			return nil, fmt.Errorf("partition field %q: %w", f.Name, err)
		}
		if _, err := f.Transform.MarshalText(); err != nil {
			return nil, fmt.Errorf("partition field %q: %w", f.Name, err)
		}

		fields = append(fields, fieldRef{
			id: f.FieldID, name: sourceName, transform: f.Transform, resultType: bound.Type(),
		})
	}

	// NewAnd/NewOr fold away the AlwaysTrue/AlwaysFalse seeds, so a single-field,
	// single-partition input returns the bare leaf predicate rather than a
	// wrapped tree.
	var result iceberg.BooleanExpression = iceberg.AlwaysFalse{}
	seen := make(map[string]struct{}, len(partitions))

	for _, part := range partitions {
		var clause iceberg.BooleanExpression = iceberg.AlwaysTrue{}

		// sigParts encodes (field id, canonical value bytes) per field so that
		// distinct tuples can never collide into one dedup key, regardless of
		// the value's content (e.g. strings containing the separators).
		sigParts := make([]string, 0, len(fields))

		for _, fr := range fields {
			term := partitionTerm(fr.transform, fr.name)

			// DataFile.Partition() stores a null partition value as a nil-valued
			// entry. A missing field ID is malformed and must fail closed rather
			// than silently under-deleting.
			val, ok := part[fr.id]
			if !ok {
				return nil, fmt.Errorf("%w: partition field %q (field id %d) is missing from partition tuple",
					iceberg.ErrInvalidArgument, fr.name, fr.id)
			}
			if val == nil {
				clause = iceberg.NewAnd(clause, iceberg.IsNull(term))
				sigParts = append(sigParts, strconv.Itoa(fr.id)+":null")

				continue
			}

			lit, err := LiteralForPartitionValue(val)
			if err != nil {
				return nil, fmt.Errorf("partition field %q: %w", fr.name, err)
			}
			lit, err = validatePartitionValue(fr.transform, fr.resultType, lit)
			if err != nil {
				return nil, fmt.Errorf("partition field %q: %w", fr.name, err)
			}

			if isNaN(lit.Any()) {
				// x == NaN is never true (IEEE 754), so a NaN partition value must
				// match via IsNaN. This intentionally diverges from PyIceberg's
				// _build_partition_predicate, which emits EqualTo for every value:
				// IsNaN is both correct as a row filter and lets the strict-metrics
				// evaluator delete NaN-only files outright instead of rewriting them.
				//
				// NaN has many valid bit patterns, so the dedup signature uses a
				// fixed sentinel (not MarshalBinary) to keep duplicate NaN
				// partitions collapsing to a single clause.
				clause = iceberg.NewAnd(clause, iceberg.IsNaN(term))
				sigParts = append(sigParts, strconv.Itoa(fr.id)+":nan")

				continue
			}

			clause = iceberg.NewAnd(clause, iceberg.LiteralPredicate(iceberg.OpEQ, term, lit))

			enc, err := lit.MarshalBinary()
			if err != nil {
				return nil, fmt.Errorf("partition field %q: encoding value for dedup: %w", fr.name, err)
			}
			sigParts = append(sigParts, strconv.Itoa(fr.id)+":v"+hex.EncodeToString(enc))
		}

		sig := strings.Join(sigParts, "|")
		if _, dup := seen[sig]; dup {
			continue
		}
		seen[sig] = struct{}{}

		result = iceberg.NewOr(result, clause)
	}

	return result, nil
}

func partitionTerm(transform iceberg.Transform, name string) iceberg.UnboundTerm {
	ref := iceberg.Reference(name)
	if isIdentityTransform(transform) {
		return ref
	}
	if isVoidTransform(transform) {
		// Use the value form so binding the null predicate can fold it to
		// AlwaysTrue. Pointer forms are accepted by the Transform interface too.
		return iceberg.NewUnboundTransform(iceberg.VoidTransform{}, ref)
	}

	return iceberg.NewUnboundTransform(transform, ref)
}

func isIdentityTransform(transform iceberg.Transform) bool {
	switch t := transform.(type) {
	case iceberg.IdentityTransform:
		return true
	case *iceberg.IdentityTransform:
		return t != nil
	default:
		return false
	}
}

func isVoidTransform(transform iceberg.Transform) bool {
	switch t := transform.(type) {
	case iceberg.VoidTransform:
		return true
	case *iceberg.VoidTransform:
		return t != nil
	default:
		return false
	}
}

func isTruncateTransform(transform iceberg.Transform) bool {
	switch t := transform.(type) {
	case iceberg.TruncateTransform:
		return true
	case *iceberg.TruncateTransform:
		return t != nil
	default:
		return false
	}
}

func validatePartitionValue(transform iceberg.Transform, resultType iceberg.Type, lit iceberg.Literal) (iceberg.Literal, error) {
	normalized, err := lit.To(resultType)
	if err != nil {
		return nil, fmt.Errorf("%w: partition value type %s cannot be converted to transform result type %s: %v",
			iceberg.ErrInvalidArgument, lit.Type(), resultType, err)
	}

	switch normalized.(type) {
	case iceberg.AboveMaxLiteral, iceberg.BelowMinLiteral:
		return nil, fmt.Errorf("%w: partition value %s is outside transform result type %s",
			iceberg.ErrInvalidArgument, normalized, resultType)
	}

	switch t := transform.(type) {
	case iceberg.BucketTransform:
		if err := validateBucketPartitionValue(t.NumBuckets, normalized); err != nil {
			return nil, err
		}
	case *iceberg.BucketTransform:
		if t == nil {
			return nil, fmt.Errorf("%w: bucket transform cannot be nil", iceberg.ErrInvalidArgument)
		}
		if err := validateBucketPartitionValue(t.NumBuckets, normalized); err != nil {
			return nil, err
		}
	case iceberg.VoidTransform, *iceberg.VoidTransform:
		return nil, fmt.Errorf("%w: void transform only accepts a nil partition value", iceberg.ErrInvalidArgument)
	}

	if (isIdentityTransform(transform) || isTruncateTransform(transform)) && !isNaN(normalized.Any()) {
		applied := transform.Apply(iceberg.Optional[iceberg.Literal]{Valid: true, Val: normalized})
		if !applied.Valid || !applied.Val.Equals(normalized) {
			return nil, fmt.Errorf("%w: partition value %s is not in the range of transform %s",
				iceberg.ErrInvalidArgument, normalized, transform)
		}
	}

	return normalized, nil
}

func validateBucketPartitionValue(numBuckets int, lit iceberg.Literal) error {
	value, ok := lit.(iceberg.Int32Literal)
	if !ok {
		return fmt.Errorf("%w: bucket partition value must be int32, got %s", iceberg.ErrInvalidArgument, lit.Type())
	}
	if numBuckets <= 0 || numBuckets > math.MaxInt32 {
		return fmt.Errorf("%w: bucket transform requires numBuckets in [1, %d], got %d",
			iceberg.ErrInvalidArgument, math.MaxInt32, numBuckets)
	}
	if value.Value() < 0 || int64(value.Value()) >= int64(numBuckets) {
		return fmt.Errorf("%w: bucket partition value %d is outside [0, %d)",
			iceberg.ErrInvalidArgument, value.Value(), numBuckets)
	}

	return nil
}

// isNaN reports whether v is a floating-point NaN.
func isNaN(v any) bool {
	switch f := v.(type) {
	case float32:
		return math.IsNaN(float64(f))
	case float64:
		return math.IsNaN(f)
	default:
		return false
	}
}

// LiteralForPartitionValue converts a partition value (as stored on a DataFile)
// into a typed Literal so the resulting predicate binds against the source
// field with the correct type, rather than relying on a string rendering.
//
// DataFile.Partition() yields either a Literal (e.g. DecimalLiteral for decimal
// fields, decoded in manifest.go) or a raw Go value; both are handled.
func LiteralForPartitionValue(v any) (iceberg.Literal, error) {
	// Decoded partition values are sometimes already Literals (decimal fields).
	if lit, ok := v.(iceberg.Literal); ok {
		return lit, nil
	}

	// The remaining cases cover the raw Go values manifest decoding emits
	// (int32/int64/float/string/[]byte and the Date/Time/Timestamp aliases).
	// The int and iceberg.Decimal cases are accepted defensively for
	// hand-constructed inputs; the decoder itself yields int32/int64 and, for
	// decimals, a DecimalLiteral caught by the passthrough above.
	switch val := v.(type) {
	case bool:
		return iceberg.NewLiteral(val), nil
	case int32:
		return iceberg.NewLiteral(val), nil
	case int64:
		return iceberg.NewLiteral(val), nil
	case int:
		return iceberg.NewLiteral(int64(val)), nil
	case float32:
		return iceberg.NewLiteral(val), nil
	case float64:
		return iceberg.NewLiteral(val), nil
	case string:
		return iceberg.NewLiteral(val), nil
	case []byte:
		return iceberg.NewLiteral(val), nil
	case iceberg.Date:
		return iceberg.NewLiteral(val), nil
	case iceberg.Time:
		return iceberg.NewLiteral(val), nil
	case iceberg.Timestamp:
		return iceberg.NewLiteral(val), nil
	case iceberg.TimestampNano:
		return iceberg.NewLiteral(val), nil
	case uuid.UUID:
		return iceberg.NewLiteral(val), nil
	case iceberg.Decimal:
		return iceberg.NewLiteral(val), nil
	default:
		return nil, fmt.Errorf("%w: unsupported partition value type %T", iceberg.ErrInvalidArgument, v)
	}
}
