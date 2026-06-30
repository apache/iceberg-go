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

// Package collation is a prototype implementation of collation support for
// iceberg-go. A collation is an annotation on a string field that changes how
// values are compared and ordered (for example case-insensitive, accent-
// insensitive, or locale-aware ordering) without changing how the data is
// stored: 'appLE' is still read back as 'appLE'.
//
// The specifier grammar follows the Apache Iceberg collation proposal
// (https://lists.apache.org/thread/zdvc0gmqjgws8whwxv0ztn7h1jqclzyk):
//
//	specifier := locale ("-" modifier)*
//	locale    := "utf8" | <CLDR locale, e.g. "en", "en_US", "de_DE", "sv">
//	modifier  := "ci" | "cs"        // case (in)sensitive
//	           | "ai" | "as"        // accent (in)sensitive
//	           | "rtrim" | "ltrim" | "trim"
//	           | "upper" | "lower" | "casefold"
//
// "utf8" is a pseudo-locale meaning the default byte-order (binary) comparison.
// A nil *Spec is also treated as binary.
//
// Collation comparison is backed by golang.org/x/text/collate, which implements
// the Unicode Collation Algorithm over CLDR locale data — the same family of
// rules ICU provides, and the natural ICU-equivalent available to Go without a
// new dependency.
//
// Prototype scope and known limitations (this is a demo to drive an Iceberg
// spec discussion, not production-complete):
//
//   - Collation-aware bounds ("collation metrics") live in the parent iceberg
//     package (CollationBoundEntry, ComputeCollatedBounds) and the manifest's
//     prototype data_file.collation_bounds field; this package only does the
//     specifier parsing and comparison.
//   - The collation_bounds Avro field uses provisional, experimental field IDs
//     (9000-9006) until an official range is reserved in the spec.
//   - The collation_spec schema-JSON shape (name + icu_collator_version) tracks
//     the current proposal and is provisional; a Java reference implementation
//     may settle on different keys.
//   - Collation annotations round-trip only at the NestedField level; a collated
//     string inside a list element or map key/value is not yet preserved.
//   - Changing a column's collation via UpdateColumn is not supported (PromoteType
//     rejects it); adding/altering collation needs a dedicated evolution path.
//   - Pruning is collation-aware only at the data-file level and only in the
//     inclusive evaluator; Parquet row-group pruning and the strict evaluator
//     conservatively keep everything for collated columns.
//   - The writer does not yet auto-compute collation bounds; ComputeCollatedBounds
//     is the building block a writer would call.
package collation

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"golang.org/x/text/cases"
	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

// ErrInvalidCollation is the sentinel wrapped by every Parse failure, so callers
// can match collation-parse errors with errors.Is (mirroring iceberg-go's
// ErrInvalidSchema / ErrBadCast convention).
var ErrInvalidCollation = errors.New("invalid collation")

// trimMode controls optional whitespace trimming before comparison.
type trimMode uint8

const (
	trimNone trimMode = iota
	trimRight
	trimLeft
	trimBoth
)

// caseMode controls optional case folding before comparison. Casing modifiers
// are only valid on the "utf8" (binary) base — a real locale must use the "ci"
// modifier instead.
type caseMode uint8

const (
	caseNone caseMode = iota
	caseUpper
	caseLower
	caseFold
)

// Spec is a parsed, immutable collation specifier. The zero value is not
// meaningful; build one with Parse. A nil *Spec means binary (UTF-8 byte order).
type Spec struct {
	// locale is the raw locale token as written ("en_US", "sv", ...). Empty for
	// the binary/"utf8" pseudo-locale.
	locale string
	binary bool

	caseInsensitive   bool
	accentInsensitive bool
	trim              trimMode
	casing            caseMode

	// version optionally pins the collation implementation version (the
	// proposal's icu_collator_version). It is metadata only: it does not change
	// comparison here, but two specs with different versions are not equal,
	// matching the "moving to a newer version is a new collation" rule.
	version string

	// tag is the BCP-47 tag derived from locale (only set when !binary).
	tag language.Tag

	// pool hands out reusable collators for locale comparison (collate.Collator
	// is not safe for concurrent use). Built once in Parse for non-binary specs
	// and shared across copies made by WithVersion (version doesn't affect the
	// collator). nil for binary specs. It is a pointer, so copying a Spec by
	// value just shares the pool.
	pool *sync.Pool
}

// Parse parses a collation specifier such as "en_US-ci" or "utf8".
func Parse(spec string) (*Spec, error) {
	if spec == "" {
		return nil, fmt.Errorf("%w: empty specifier", ErrInvalidCollation)
	}

	parts := strings.Split(spec, "-")
	locale := parts[0]
	s := &Spec{}

	if strings.EqualFold(locale, "utf8") {
		s.binary = true
	} else {
		s.locale = locale
		tag, err := language.Parse(strings.ReplaceAll(locale, "_", "-"))
		if err != nil {
			return nil, fmt.Errorf("%w: invalid locale %q: %w", ErrInvalidCollation, locale, err)
		}
		s.tag = tag
	}

	var sawCase, sawAccent, sawCasing bool
	for _, m := range parts[1:] {
		switch strings.ToLower(m) {
		case "cs", "ci":
			if sawCase {
				return nil, fmt.Errorf("%w: conflicting case modifiers in %q", ErrInvalidCollation, spec)
			}
			sawCase = true
			s.caseInsensitive = strings.EqualFold(m, "ci")
		case "as", "ai":
			if sawAccent {
				return nil, fmt.Errorf("%w: conflicting accent modifiers in %q", ErrInvalidCollation, spec)
			}
			sawAccent = true
			s.accentInsensitive = strings.EqualFold(m, "ai")
		case "rtrim":
			s.trim = trimRight
		case "ltrim":
			s.trim = trimLeft
		case "trim":
			s.trim = trimBoth
		case "upper", "lower", "casefold":
			if sawCasing {
				return nil, fmt.Errorf("%w: conflicting casing modifiers in %q", ErrInvalidCollation, spec)
			}
			sawCasing = true
			switch strings.ToLower(m) {
			case "upper":
				s.casing = caseUpper
			case "lower":
				s.casing = caseLower
			case "casefold":
				s.casing = caseFold
			}
		default:
			return nil, fmt.Errorf("%w: unknown modifier %q in %q", ErrInvalidCollation, m, spec)
		}
	}

	// Validation rules from the proposal.
	if s.binary {
		// Case/accent insensitivity is language-specific and needs a real locale.
		if s.caseInsensitive {
			return nil, fmt.Errorf("%w: 'ci' requires a locale, not utf8 (%q)", ErrInvalidCollation, spec)
		}
		if s.accentInsensitive {
			return nil, fmt.Errorf("%w: 'ai' requires a locale, not utf8 (%q)", ErrInvalidCollation, spec)
		}

		return s, nil
	}

	if s.casing != caseNone {
		// A real locale cannot be combined with a casing modifier; use 'ci'.
		return nil, fmt.Errorf("%w: casing modifiers cannot be combined with locale %q; use 'ci' instead", ErrInvalidCollation, locale)
	}

	// Build the pooled collator once per spec; locale comparison reuses pooled
	// instances across calls instead of allocating a pool (and collator) per
	// Comparator() call.
	tag, opts := s.tag, s.collatorOptions()
	s.pool = &sync.Pool{New: func() any { return collate.New(tag, opts...) }}

	return s, nil
}

// MustParse is Parse but panics on error. Intended for tests and constants.
func MustParse(spec string) *Spec {
	s, err := Parse(spec)
	if err != nil {
		panic(err)
	}

	return s
}

// WithVersion returns a copy of the spec with its implementation version pinned
// (the proposal's icu_collator_version).
func (s *Spec) WithVersion(version string) *Spec {
	if s == nil {
		return nil
	}
	cp := *s
	cp.version = version

	return &cp
}

// IsBinary reports whether comparison is exactly UTF-8 byte order, with no
// transform. This is the condition under which the original column's UTF-8
// lower/upper bounds remain valid for pruning. A nil *Spec is binary. Note that
// "utf8-lower" or "utf8-rtrim" are NOT binary: their transform changes equality
// relative to the stored (untransformed) byte-order bounds.
func (s *Spec) IsBinary() bool {
	return s == nil || (s.binary && s.casing == caseNone && s.trim == trimNone)
}

// Version returns the pinned implementation version, or "" if unset.
func (s *Spec) Version() string {
	if s == nil {
		return ""
	}

	return s.version
}

// String returns the canonical specifier. Default modifiers (cs/as) are
// omitted, so "en_US-cs" and "en_US" both render as "en_US".
func (s *Spec) String() string {
	if s == nil {
		return "utf8"
	}

	var b strings.Builder
	if s.binary {
		b.WriteString("utf8")
	} else {
		b.WriteString(s.locale)
		if s.caseInsensitive {
			b.WriteString("-ci")
		}
		if s.accentInsensitive {
			b.WriteString("-ai")
		}
	}
	switch s.trim {
	case trimRight:
		b.WriteString("-rtrim")
	case trimLeft:
		b.WriteString("-ltrim")
	case trimBoth:
		b.WriteString("-trim")
	}
	switch s.casing {
	case caseUpper:
		b.WriteString("-upper")
	case caseLower:
		b.WriteString("-lower")
	case caseFold:
		b.WriteString("-casefold")
	}

	return b.String()
}

// Equal reports whether two specs compare and order strings identically. Both
// nil/binary specs are equal to each other; pinned versions must match.
func Equal(a, b *Spec) bool {
	return a.String() == b.String() && a.Version() == b.Version()
}

// normalize applies the optional trim and casing transforms that are not
// expressed through collator options.
func (s *Spec) normalize(str string) string {
	if s == nil {
		return str
	}
	switch s.trim {
	case trimRight:
		str = strings.TrimRight(str, " ")
	case trimLeft:
		str = strings.TrimLeft(str, " ")
	case trimBoth:
		str = strings.Trim(str, " ")
	}
	switch s.casing {
	case caseUpper:
		str = strings.ToUpper(str)
	case caseLower:
		str = strings.ToLower(str)
	case caseFold:
		str = cases.Fold().String(str)
	}

	return str
}

func (s *Spec) collatorOptions() []collate.Option {
	var opts []collate.Option
	if s.caseInsensitive {
		opts = append(opts, collate.IgnoreCase)
	}
	if s.accentInsensitive {
		opts = append(opts, collate.IgnoreDiacritics)
	}

	return opts
}

// Comparator returns a string comparator honoring this collation. The returned
// function is safe for concurrent use. For a binary spec it is byte-order
// comparison (after any trim/casing transform).
func (s *Spec) Comparator() func(a, b string) int {
	if s == nil {
		return strings.Compare
	}
	if s.binary {
		if s.casing == caseNone && s.trim == trimNone {
			return strings.Compare
		}
		// Binary base with a trim/casing transform applied first.
		return func(a, b string) int {
			return strings.Compare(s.normalize(a), s.normalize(b))
		}
	}

	// Locale-aware comparison. collate.Collator is not safe for concurrent use,
	// so hand each goroutine its own instance from the spec's shared pool.
	return func(a, b string) int {
		c := s.pool.Get().(*collate.Collator)
		defer s.pool.Put(c)

		return c.CompareString(s.normalize(a), s.normalize(b))
	}
}

// Key returns the collation sort key for str: a binary-comparable encoding such
// that two strings equal under this collation share a key. This is the building
// block for collation-aware min/max bounds; it is provided for completeness and
// is not yet consumed by pruning in this prototype.
func (s *Spec) Key(str string) []byte {
	if s == nil {
		return []byte(str)
	}
	if s.binary {
		return []byte(s.normalize(str))
	}
	c := collate.New(s.tag, s.collatorOptions()...)
	var buf collate.Buffer

	return append([]byte(nil), c.KeyFromString(&buf, s.normalize(str))...)
}
