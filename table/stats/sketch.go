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

// Package stats computes Apache Iceberg table statistics.
//
// This file provides the number-of-distinct-values (NDV) primitive: a thin
// wrapper over an Apache DataSketches Theta sketch that produces
// apache-datasketches-theta-v1 statistics blobs mutually readable and mergeable
// with the ones written by Iceberg-Java's Spark ComputeTableStats action.
// Interoperability rests on settings this package pins to match Iceberg-Java:
// the hash seed (9001), the log2 nominal entries (lgK = 12, i.e. 4096 entries),
// and the serialized form (an uncompressed, ordered compact sketch).
//
// Iceberg-Java builds one ALPHA-family update sketch per Spark partition and
// merges the partials through a Theta union, so the sketch it stores is always a
// union result trimmed to 4096 entries. datasketches-go provides only the
// QuickSelect update family, so this package mirrors the merge instead: Sketch
// and Union both produce their estimate and serialization through a union with
// the same settings. The decoded sketch (theta and retained hashes) is therefore
// identical to Iceberg-Java's whenever each per-partition sketch holds at most
// 4096 distinct values, including when the merged result holds more and is
// trimmed. Once a single sketch goes past 4096 distinct values, the ALPHA and
// QuickSelect families keep different samples, so the estimates agree only
// within the sketch's error bounds.
//
// Callers must feed values using Iceberg's single-value serialization (the same
// byte encoding used for manifest bounds: little-endian integers and floats,
// UTF-8 strings with no length prefix, etc.); the sketch hashes those exact
// bytes, so matching the encoding is what makes NDV estimates agree across
// engines. Null values must not be added.
package stats

import (
	"fmt"

	"github.com/apache/datasketches-go/theta"
)

const (
	// defaultSeed is the hash seed embedded in every serialized sketch. It must
	// match Iceberg-Java's ThetaUtil.DEFAULT_UPDATE_SEED (and the DataSketches
	// default) so sketches can be merged and read across implementations.
	defaultSeed = theta.DefaultSeed

	// defaultLgK is the log2 of the nominal number of entries. Iceberg-Java's
	// NDV action uses DataSketches' default of 4096 nominal entries (2^12) for
	// both its update sketches and its union, so we pin lgK = 12.
	defaultLgK = uint8(12)
)

// Sketch accumulates values into a Theta sketch to estimate the number of
// distinct values (NDV) of a column. It is not safe for concurrent use.
type Sketch struct {
	us *theta.QuickSelectUpdateSketch
}

// NewSketch returns an empty Sketch configured to match Iceberg-Java's NDV
// sketches (seed 9001, lgK 12).
func NewSketch() *Sketch {
	us, err := theta.NewQuickSelectUpdateSketch(
		theta.WithUpdateSketchLgK(defaultLgK),
		theta.WithUpdateSketchSeed(defaultSeed),
	)
	mustNotFail(err)

	return &Sketch{us: us}
}

// Add feeds one non-null value into the sketch. value must be the Iceberg
// single-value serialization of the column value; the sketch hashes these
// bytes directly.
//
// An empty value (such as the empty string or a zero-length binary) is
// ignored and not counted as a distinct value. This matches Iceberg-Java,
// whose DataSketches UpdateSketch.update(ByteBuffer) rejects empty input.
func (s *Sketch) Add(value []byte) {
	if len(value) == 0 {
		return
	}

	// The only errors UpdateBytes reports are a duplicate value, a hash at or
	// above the current sampling threshold, and a zero hash. Each is a normal
	// outcome that leaves the sketch unchanged, and DataSketches-Java skips the
	// same cases silently.
	_ = s.us.UpdateBytes(value)
}

// Estimate returns the estimated number of distinct values added so far.
func (s *Sketch) Estimate() float64 {
	return s.result().Estimate()
}

// NDV returns Estimate truncated to an int64, matching how Iceberg-Java records
// the "ndv" blob property (String.valueOf((long) sketch.getEstimate())).
func (s *Sketch) NDV() int64 {
	return int64(s.Estimate())
}

// Serialize returns the compact, ordered, uncompressed serialization of the
// sketch: the byte form stored inside an apache-datasketches-theta-v1 Puffin
// blob (before any Puffin-level compression). The bytes are not identical to
// Iceberg-Java's (DataSketches-Java sets a few preamble fields differently),
// but each implementation decodes the other's to the same sketch.
func (s *Sketch) Serialize() []byte {
	return marshal(s.result())
}

// result passes the sketch through a union, as Iceberg-Java does with every
// sketch it stores, so the result is trimmed to the nominal entries.
func (s *Sketch) result() *theta.CompactSketch {
	u := NewUnion()
	u.Add(s)

	return u.result()
}

// Union merges Theta sketches into a single sketch, used to combine per-file or
// per-partition sketches into one column-level NDV estimate. Merging requires
// the same seed on every input, which this package guarantees. Union is not
// safe for concurrent use.
type Union struct {
	u *theta.Union
}

// NewUnion returns an empty Union configured to match Iceberg-Java's NDV union
// (seed 9001, lgK 12).
func NewUnion() *Union {
	u, err := theta.NewUnion(
		theta.WithUnionLgK(defaultLgK),
		theta.WithUnionSeed(defaultSeed),
	)
	mustNotFail(err)

	return &Union{u: u}
}

// Add merges s into the union.
func (u *Union) Add(s *Sketch) {
	// Update fails only on a seed mismatch, which cannot happen between
	// sketches this package configures.
	mustNotFail(u.u.Update(s.us))
}

// AddBytes decodes a serialized sketch (as produced by Sketch.Serialize or by
// another Iceberg implementation) and merges it into the union.
func (u *Union) AddBytes(serialized []byte) error {
	cs, err := theta.Decode(serialized, defaultSeed)
	if err != nil {
		return fmt.Errorf("stats: decode theta sketch: %w", err)
	}
	if err := u.u.Update(cs); err != nil {
		return fmt.Errorf("stats: union theta sketch: %w", err)
	}

	return nil
}

// Estimate returns the estimated number of distinct values across all merged
// sketches.
func (u *Union) Estimate() float64 {
	return u.result().Estimate()
}

// NDV returns Estimate truncated to an int64.
func (u *Union) NDV() int64 {
	return int64(u.Estimate())
}

// Serialize returns the compact, ordered, uncompressed serialization of the
// merged sketch, in the same byte form as Sketch.Serialize.
func (u *Union) Serialize() []byte {
	return marshal(u.result())
}

func (u *Union) result() *theta.CompactSketch {
	// Result fails only if the seed hash cannot be computed, which cannot
	// happen for the fixed seed this package uses.
	cs, err := u.u.Result(true)
	mustNotFail(err)

	return cs
}

// EstimateFromBytes decodes a serialized Theta sketch and returns its estimated
// number of distinct values. It reads sketches written by any Iceberg
// implementation, including the compressed serialization versions produced by
// newer DataSketches releases.
func EstimateFromBytes(serialized []byte) (float64, error) {
	cs, err := theta.Decode(serialized, defaultSeed)
	if err != nil {
		return 0, fmt.Errorf("stats: decode theta sketch: %w", err)
	}

	return cs.Estimate(), nil
}

// NDVFromBytes decodes a serialized Theta sketch and returns its estimate
// truncated to an int64.
func NDVFromBytes(serialized []byte) (int64, error) {
	est, err := EstimateFromBytes(serialized)
	if err != nil {
		return 0, err
	}

	return int64(est), nil
}

func marshal(cs *theta.CompactSketch) []byte {
	// Uncompressed encoding into memory fails only if the writer does, and a
	// bytes.Buffer never does.
	b, err := cs.MarshalBinary()
	mustNotFail(err)

	return b
}

// mustNotFail panics on errors that the pinned settings rule out. Reaching it
// means datasketches-go changed behavior, not that the caller did anything
// wrong.
func mustNotFail(err error) {
	if err != nil {
		panic(fmt.Sprintf("stats: unexpected theta sketch error: %v", err))
	}
}
