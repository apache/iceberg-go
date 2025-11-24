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
	"cmp"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
)

// Helper function to find the difference between two slices (a - b).
func Difference(a, b []string) []string {
	m := make(map[string]bool)
	for _, item := range b {
		m[item] = true
	}

	diff := make([]string, 0)
	for _, item := range a {
		if !m[item] {
			diff = append(diff, item)
		}
	}

	return diff
}

type Bin[T any] struct {
	binWeight    int64
	targetWeight int64
	items        []T
}

func (b *Bin[T]) Weight() int64            { return b.binWeight }
func (b *Bin[T]) CanAdd(weight int64) bool { return b.binWeight+weight <= b.targetWeight }
func (b *Bin[T]) Add(item T, weight int64) {
	b.binWeight += weight
	b.items = append(b.items, item)
}

func PackingIterator[T any](itr iter.Seq[T], targetWeight int64, lookback int, weightFunc func(T) int64, largestBinFirst bool) iter.Seq[[]T] {
	bins := make([]Bin[T], 0)
	findBin := func(weight int64) *Bin[T] {
		for i := range bins {
			if bins[i].CanAdd(weight) {
				return &bins[i]
			}
		}

		return nil
	}

	removeBin := func() Bin[T] {
		if largestBinFirst {
			maxBin := slices.MaxFunc(bins, func(a, b Bin[T]) int {
				return cmp.Compare(a.Weight(), b.Weight())
			})
			i := slices.IndexFunc(bins, func(e Bin[T]) bool {
				return e.Weight() == maxBin.Weight()
			})

			bins = slices.Delete(bins, i, i+1)

			return maxBin
		}

		var out Bin[T]
		out, bins = bins[0], bins[1:]

		return out
	}

	return func(yield func([]T) bool) {
		for item := range itr {
			w := weightFunc(item)
			bin := findBin(w)
			if bin != nil {
				bin.Add(item, w)
			} else {
				bin := Bin[T]{targetWeight: targetWeight}
				bin.Add(item, w)
				bins = append(bins, bin)

				if len(bins) > lookback {
					if !yield(removeBin().items) {
						return
					}
				}
			}
		}

		for len(bins) > 0 {
			if !yield(removeBin().items) {
				return
			}
		}
	}
}

type SlicePacker[T any] struct {
	TargetWeight    int64
	Lookback        int
	LargestBinFirst bool
}

func (s *SlicePacker[T]) Pack(items []T, weightFunc func(T) int64) [][]T {
	return slices.Collect(PackingIterator(slices.Values(items), s.TargetWeight,
		s.Lookback, weightFunc, s.LargestBinFirst))
}

func (s *SlicePacker[T]) PackEnd(items []T, weightFunc func(T) int64) [][]T {
	slices.Reverse(items)
	packed := s.Pack(items, weightFunc)
	slices.Reverse(packed)

	result := make([][]T, 0, len(packed))
	for _, items := range packed {
		slices.Reverse(items)
		result = append(result, items)
	}

	return result
}

type CountingWriter struct {
	Count int64
	W     io.Writer
}

func (w *CountingWriter) Write(p []byte) (int, error) {
	n, err := w.W.Write(p)
	w.Count += int64(n)

	return n, err
}

func RecoverError(err *error) {
	if r := recover(); r != nil {
		switch e := r.(type) {
		case string:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %s", e)
		case error:
			*err = fmt.Errorf("error encountered during arrow schema visitor: %w", e)
		}
	}
}

func Counter(start int) iter.Seq[int] {
	return func(yield func(int) bool) {
		for {
			if !yield(start) {
				return
			}
			start++
		}
	}
}

// CheckedClose is a helper function to close a resource and return an error if it fails.
// It is intended to be used in a defer statement.
func CheckedClose(c io.Closer, err *error) {
	*err = errors.Join(*err, c.Close())
}

// SliceEqualHelper compares the equality of two slices whose elements have an Equals method
func SliceEqualHelper[T interface{ Equals(T) bool }](s1, s2 []T) bool {
	return slices.EqualFunc(s1, s2, func(t1, t2 T) bool {
		return t1.Equals(t2)
	})
}
