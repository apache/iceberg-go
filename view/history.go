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

package view

import "slices"

// VersionByID returns a retained view version by ID, or nil if the version
// is not available in this view's metadata. The returned version is a copy.
func (v View) VersionByID(versionID int64) *Version {
	// The built-in metadata keeps a lazy version index. Avoid cloning the
	// entire version list to look up one version.
	if md, ok := v.metadata.(*metadata); ok {
		return md.lazyVersionsByID()[versionID].Clone()
	}

	// Preserve support for other Metadata implementations.
	for _, version := range v.metadata.Versions() {
		if version.VersionID == versionID {
			return version.Clone()
		}
	}

	return nil
}

// History returns the retained changes to the current view version in order.
// The returned slice is independent of the view's metadata.
func (v View) History() []VersionLogEntry {
	return slices.Clone(v.metadata.VersionLog())
}

// VersionLogEntryAsOf returns the entry with the greatest timestamp at or
// before timestampMs, or strictly before it when inclusive is false.
// Entries may be out of chronological order due to clock skew. If timestamps
// are equal, the first entry in log order wins, matching Iceberg's snapshot
// time-travel resolution. It returns false if no entry is eligible.
func VersionLogEntryAsOf(history []VersionLogEntry, timestampMs int64, inclusive bool) (VersionLogEntry, bool) {
	var (
		best  VersionLogEntry
		found bool
	)

	for _, entry := range history {
		if entry.TimestampMS > timestampMs || (!inclusive && entry.TimestampMS == timestampMs) {
			continue
		}

		if !found || entry.TimestampMS > best.TimestampMS {
			best = entry
			found = true
		}
	}

	return best, found
}

// VersionAsOf returns the version that was current at or before timestampMs,
// measured in milliseconds since the Unix epoch. It uses the version log
// rather than version creation timestamps, including when a view is rolled
// back to a previous version.
//
// It returns nil when no history entry is eligible or the selected version
// has expired from metadata. It does not substitute another retained version.
func (v View) VersionAsOf(timestampMs int64) *Version {
	entry, found := VersionLogEntryAsOf(v.metadata.VersionLog(), timestampMs, true)
	if !found {
		return nil
	}

	return v.VersionByID(entry.VersionID)
}
