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

package collation_test

import (
	"testing"

	"github.com/apache/iceberg-go/collation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}

func TestParseAndCanonical(t *testing.T) {
	tests := []struct {
		spec      string
		canonical string
		binary    bool
	}{
		{"utf8", "utf8", true},
		{"UTF8", "utf8", true},
		{"en_US", "en_US", false},
		{"en_US-cs", "en_US", false},    // cs is the default, dropped
		{"en_US-cs-as", "en_US", false}, // both defaults, dropped
		{"en_US-ci", "en_US-ci", false},
		{"en_US-ci-ai", "en_US-ci-ai", false},
		{"de_DE-ci", "de_DE-ci", false},
		{"sv", "sv", false},
		{"utf8-lower", "utf8-lower", false}, // transform => not byte-order-safe
		{"utf8-rtrim", "utf8-rtrim", false},
	}
	for _, tt := range tests {
		t.Run(tt.spec, func(t *testing.T) {
			s, err := collation.Parse(tt.spec)
			require.NoError(t, err)
			assert.Equal(t, tt.canonical, s.String())
			assert.Equal(t, tt.binary, s.IsBinary())
		})
	}
}

func TestParseErrors(t *testing.T) {
	bad := []string{
		"",            // empty
		"en_US-ci-cs", // conflicting case
		"en_US-ai-as", // conflicting accent
		"utf8-upper-lower",
		"utf8-ci",     // ci needs a real locale
		"utf8-ai",     // ai needs a real locale
		"en_US-lower", // casing cannot combine with a locale
		"en_US-bogus", // unknown modifier
		"not a locale",
	}
	for _, spec := range bad {
		t.Run(spec, func(t *testing.T) {
			_, err := collation.Parse(spec)
			require.Error(t, err)
			require.ErrorIs(t, err, collation.ErrInvalidCollation)
		})
	}
}

func TestCaseInsensitiveEquality(t *testing.T) {
	cmp := collation.MustParse("en_US-ci").Comparator()
	assert.Equal(t, 0, cmp("APPLE", "apple"), "ci should treat APPLE == apple")
	assert.NotEqual(t, 0, cmp("apple", "banana"))

	// Binary comparison must distinguish case.
	bin := collation.MustParse("utf8").Comparator()
	assert.NotEqual(t, 0, bin("APPLE", "apple"))
}

func TestAccentInsensitiveEquality(t *testing.T) {
	cmp := collation.MustParse("en_US-ci-ai").Comparator()
	assert.Equal(t, 0, cmp("résumé", "resume"), "ai should treat résumé == resume")
}

func TestLocaleOrdering(t *testing.T) {
	// In Swedish, 'ö' sorts after 'z'.
	sv := collation.MustParse("sv").Comparator()
	assert.Equal(t, 1, sign(sv("ö", "z")), "sv: ö sorts after z")

	// In German (phonebook-style root behavior here), 'ö' sorts near 'o',
	// i.e. before 'z'. This is the crux of why UTF-8 byte order is wrong for
	// collated columns.
	de := collation.MustParse("de_DE").Comparator()
	assert.Equal(t, -1, sign(de("ö", "z")), "de: ö sorts before z")
}

func TestBinaryVsCollationOrderDiffer(t *testing.T) {
	// 'a' (0x61) > 'B' (0x42) in UTF-8 byte order, but 'a' < 'B' under a
	// case-insensitive collation. This divergence is exactly why pruning with
	// UTF-8 bounds is unsafe for collated columns.
	bin := collation.MustParse("utf8").Comparator()
	assert.Equal(t, 1, sign(bin("a", "B")), "byte order: a > B")

	ci := collation.MustParse("en_US-ci").Comparator()
	assert.Equal(t, -1, sign(ci("a", "B")), "case-insensitive: a < B")
}

func TestTrimming(t *testing.T) {
	rtrim := collation.MustParse("utf8-rtrim").Comparator()
	assert.Equal(t, 0, rtrim("abc   ", "abc"))

	notrim := collation.MustParse("utf8").Comparator()
	assert.NotEqual(t, 0, notrim("abc   ", "abc"))
}

func TestCasing(t *testing.T) {
	lower := collation.MustParse("utf8-lower").Comparator()
	assert.Equal(t, 0, lower("HELLO", "hello"))
}

func TestKeyEqualityUnderCollation(t *testing.T) {
	s := collation.MustParse("en_US-ci")
	// Strings equal under the collation produce identical sort keys.
	assert.Equal(t, s.Key("APPLE"), s.Key("apple"))
	assert.NotEqual(t, s.Key("apple"), s.Key("banana"))
}

func TestEqualAndVersion(t *testing.T) {
	assert.True(t, collation.Equal(nil, collation.MustParse("utf8")))
	assert.True(t, collation.Equal(collation.MustParse("en_US"), collation.MustParse("en_US-cs")))
	assert.False(t, collation.Equal(collation.MustParse("en_US"), collation.MustParse("en_US-ci")))

	v1 := collation.MustParse("en_US-ci").WithVersion("153.88")
	v2 := collation.MustParse("en_US-ci").WithVersion("154.0")
	assert.False(t, collation.Equal(v1, v2), "different pinned versions are not equal")
	assert.Equal(t, "153.88", v1.Version())
}

func TestComparatorConcurrentSafe(t *testing.T) {
	cmp := collation.MustParse("en_US-ci").Comparator()
	done := make(chan int, 8)
	for range 8 {
		go func() {
			r := 0
			for range 1000 {
				r += cmp("Apple", "apple")
			}
			done <- r
		}()
	}
	for range 8 {
		assert.Equal(t, 0, <-done)
	}
}
