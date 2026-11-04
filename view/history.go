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

// VersionAsOf returns the version that was current at the given timestamp in
// milliseconds since the Unix epoch. When inclusive is false, changes at the
// exact timestamp are excluded. The version log, not the version creation
// timestamp, determines which version was current, including after rollbacks.
//
// It returns nil when the requested time precedes the retained history or the
// selected version is no longer retained. It never substitutes an older entry
// for a version that is missing from metadata.
func (v View) VersionAsOf(timestampMs int64, inclusive bool) *Version {
	history := v.metadata.VersionLog()
	for _, entry := range slices.Backward(history) {
		if (inclusive && entry.TimestampMS <= timestampMs) ||
			(!inclusive && entry.TimestampMS < timestampMs) {
			return v.VersionByID(entry.VersionID)
		}
	}

	return nil
}
