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
	for _, version := range v.metadata.Versions() {
		if version.VersionID == versionID {
			return version
		}
	}

	return nil
}

// History returns the retained changes to the current view version in order.
// The returned slice is independent of the view's metadata.
func (v View) History() []VersionLogEntry {
	return v.metadata.VersionLog()
}

// VersionAsOf returns the version that was current at or before timestampMs,
// measured in milliseconds since the Unix epoch. It uses the version log
// rather than version creation timestamps, including when a view is rolled
// back to a previous version.
//
// When timestamps are equal or out of order, the last eligible log entry wins.
// It returns nil when no history entry is eligible or the selected version
// has expired from metadata. It does not substitute another retained version.
func (v View) VersionAsOf(timestampMs int64) *Version {
	history := v.metadata.VersionLog()
	for _, entry := range slices.Backward(history) {
		if entry.TimestampMS <= timestampMs {
			return v.VersionByID(entry.VersionID)
		}
	}

	return nil
}
