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

// Command gen scans the repository for example_*_test.go files that carry
// an "iceberg:doc" metadata block and regenerates the Concepts section of
// the mdbook website.
//
// A publishable example file looks like:
//
//	// iceberg:doc
//	// title: Multi-Table Transactions
//	// section: Transactions
//	// order: 10
//	// slug: multi-table-transactions
//	//
//	// Prose describing the concept...
//	package catalog_test
//
//	// ANCHOR: example
//	func Example_multiTable() { ... }
//	// ANCHOR_END: example
//
// Each matching file produces website/src/concepts/<slug>.md. The code
// blocks are emitted as mdbook "{{#include file:anchor}}" directives, so
// mdbook pulls the latest source at build time. The generator also
// rewrites the block between "<!-- GEN:START concepts -->" and
// "<!-- GEN:END -->" in website/src/SUMMARY.md.
//
// Run from the repo root:
//
//	go run ./website/gen
package main

import (
	"bytes"
	"errors"
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	marker      = "iceberg:doc"
	startMarker = "<!-- GEN:START concepts -->"
	endMarker   = "<!-- GEN:END -->"
	githubBase  = "https://github.com/apache/iceberg-go/blob/main/"
)

type concept struct {
	Title   string
	Section string
	Order   int
	Slug    string
	Prose   string
	SrcRel  string   // repo-relative path to the source .go file
	Anchors []string // in source order
}

func main() {
	root, err := findRepoRoot()
	if err != nil {
		die("find repo root: %v", err)
	}

	concepts, err := discover(root)
	if err != nil {
		die("discover: %v", err)
	}

	if len(concepts) == 0 {
		fmt.Fprintf(os.Stderr, "gen: no example_*_test.go files with %q header — nothing to do\n", marker)

		return
	}

	sort.SliceStable(concepts, func(i, j int) bool {
		if concepts[i].Section != concepts[j].Section {
			return sectionRank(concepts, concepts[i].Section) < sectionRank(concepts, concepts[j].Section)
		}
		if concepts[i].Order != concepts[j].Order {
			return concepts[i].Order < concepts[j].Order
		}

		return concepts[i].Title < concepts[j].Title
	})

	conceptsDir := filepath.Join(root, "website", "src", "concepts")
	if err := os.MkdirAll(conceptsDir, 0o755); err != nil {
		die("mkdir concepts: %v", err)
	}

	keep := map[string]bool{}
	for _, c := range concepts {
		page := renderPage(c)
		out := filepath.Join(conceptsDir, c.Slug+".md")
		if err := os.WriteFile(out, []byte(page), 0o644); err != nil {
			die("write %s: %v", out, err)
		}
		keep[c.Slug+".md"] = true
	}

	if err := pruneStale(conceptsDir, keep); err != nil {
		die("prune stale: %v", err)
	}

	if err := rewriteSummary(root, concepts); err != nil {
		die("rewrite summary: %v", err)
	}

	fmt.Fprintf(os.Stderr, "wrote %d concept page(s)\n", len(concepts))
}

// sectionRank returns a stable rank for a section based on the lowest
// Order value of any concept in that section. This keeps SUMMARY section
// ordering predictable as concepts are added.
func sectionRank(all []concept, section string) int {
	min := 1 << 30
	for _, c := range all {
		if c.Section == section && c.Order < min {
			min = c.Order
		}
	}

	return min
}

// discover walks the repo and returns every example_*_test.go file that
// carries an iceberg:doc block.
func discover(root string) ([]concept, error) {
	var out []concept
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == "vendor" || name == "node_modules" || name == ".git" || name == "book" {
				return filepath.SkipDir
			}

			return nil
		}
		if !strings.HasPrefix(d.Name(), "example_") || !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		c, ok, err := parseFile(root, path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		if ok {
			out = append(out, c)
		}

		return nil
	})

	return out, err
}

// parseFile returns the concept described by the iceberg:doc block in
// path, if any. ok is false when the file has no such block.
func parseFile(root, path string) (concept, bool, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
	if err != nil {
		return concept{}, false, err
	}

	var block string
	for _, cg := range f.Comments {
		txt := cg.Text()
		if strings.HasPrefix(strings.TrimSpace(txt), marker) {
			block = txt

			break
		}
	}
	if block == "" {
		return concept{}, false, nil
	}

	// Drop the "iceberg:doc" line itself.
	lines := strings.Split(block, "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != marker {
		return concept{}, false, fmt.Errorf("malformed header: first line must be %q", marker)
	}
	lines = lines[1:]

	// Parse key: value lines until a blank line.
	meta := map[string]string{}
	i := 0
	for ; i < len(lines); i++ {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			i++

			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			return concept{}, false, fmt.Errorf("metadata line without ':': %q", line)
		}
		meta[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	prose := strings.TrimSpace(strings.Join(lines[i:], "\n"))

	c := concept{
		Title:   meta["title"],
		Section: meta["section"],
		Slug:    meta["slug"],
		Prose:   prose,
	}
	if c.Title == "" {
		return concept{}, false, errors.New("missing title")
	}
	if c.Section == "" {
		return concept{}, false, errors.New("missing section")
	}
	if o := meta["order"]; o != "" {
		n, err := strconv.Atoi(o)
		if err != nil {
			return concept{}, false, fmt.Errorf("order: %w", err)
		}
		c.Order = n
	}
	if c.Slug == "" {
		c.Slug = slugify(c.Title)
	}

	rel, err := filepath.Rel(root, path)
	if err != nil {
		return concept{}, false, err
	}
	c.SrcRel = filepath.ToSlash(rel)

	// Scan the raw file for ANCHOR markers in source order.
	raw, err := os.ReadFile(path)
	if err != nil {
		return concept{}, false, err
	}
	c.Anchors = extractAnchors(raw)
	if len(c.Anchors) == 0 {
		return concept{}, false, errors.New("no ANCHOR markers found")
	}

	return c, true, nil
}

var anchorRe = regexp.MustCompile(`//\s*ANCHOR:\s*([A-Za-z0-9_\-]+)`)

func extractAnchors(src []byte) []string {
	var out []string
	seen := map[string]bool{}
	for _, m := range anchorRe.FindAllSubmatch(src, -1) {
		name := string(m[1])
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}

	return out
}

func slugify(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}

	return strings.TrimRight(b.String(), "-")
}

func renderPage(c concept) string {
	var b strings.Builder
	b.WriteString(licenseHTML())
	b.WriteString("\n")
	fmt.Fprintf(&b, "<!-- Generated by website/gen. Do not edit by hand. Edit %s instead. -->\n\n", c.SrcRel)
	fmt.Fprintf(&b, "# %s\n\n", c.Title)
	if c.Prose != "" {
		b.WriteString(c.Prose)
		b.WriteString("\n\n")
	}

	// Path from website/src/concepts/<slug>.md back to the repo root is
	// three levels up.
	includePath := "../../../" + c.SrcRel

	if len(c.Anchors) == 1 {
		b.WriteString("## Example\n\n")
		fmt.Fprintf(&b, "```go\n{{#include %s:%s}}\n```\n\n", includePath, c.Anchors[0])
	} else {
		b.WriteString("## Examples\n\n")
		for _, a := range c.Anchors {
			fmt.Fprintf(&b, "### %s\n\n", titleCase(a))
			fmt.Fprintf(&b, "```go\n{{#include %s:%s}}\n```\n\n", includePath, a)
		}
	}

	fmt.Fprintf(&b, "[View source on GitHub](%s%s)\n", githubBase, c.SrcRel)

	return b.String()
}

func titleCase(anchor string) string {
	parts := strings.FieldsFunc(anchor, func(r rune) bool { return r == '_' || r == '-' })
	for i, p := range parts {
		if p == "" {
			continue
		}
		parts[i] = strings.ToUpper(p[:1]) + p[1:]
	}

	return strings.Join(parts, " ")
}

func pruneStale(dir string, keep map[string]bool) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if !strings.HasSuffix(n, ".md") {
			continue
		}
		if !keep[n] {
			if err := os.Remove(filepath.Join(dir, n)); err != nil {
				return err
			}
		}
	}

	return nil
}

func rewriteSummary(root string, concepts []concept) error {
	path := filepath.Join(root, "website", "src", "SUMMARY.md")
	orig, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	startIdx := bytes.Index(orig, []byte(startMarker))
	endIdx := bytes.Index(orig, []byte(endMarker))
	if startIdx < 0 || endIdx < 0 || endIdx < startIdx {
		return fmt.Errorf("SUMMARY.md is missing %q / %q markers", startMarker, endMarker)
	}

	var inner strings.Builder
	inner.WriteString(startMarker)
	inner.WriteString("\n")

	// Group by section, preserving the section rank order we already
	// computed via the concepts slice's sort.
	var currentSection string
	for _, c := range concepts {
		if c.Section != currentSection {
			inner.WriteString("\n# ")
			inner.WriteString(c.Section)
			inner.WriteString("\n\n")
			currentSection = c.Section
		}
		fmt.Fprintf(&inner, "- [%s](./concepts/%s.md)\n", c.Title, c.Slug)
	}
	inner.WriteString("\n")
	inner.WriteString(endMarker)

	var out bytes.Buffer
	out.Write(orig[:startIdx])
	out.WriteString(inner.String())
	out.Write(orig[endIdx+len(endMarker):])

	return os.WriteFile(path, out.Bytes(), 0o644)
}

func findRepoRoot() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(wd, "go.mod")); err == nil {
			return wd, nil
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return "", errors.New("go.mod not found in any parent directory")
		}
		wd = parent
	}
}

func licenseHTML() string {
	return `<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->
`
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "gen: "+format+"\n", args...)
	os.Exit(1)
}
