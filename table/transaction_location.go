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

package table

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/apache/iceberg-go"
)

// SetLocation stages a change to the table's base location. Subsequent
// writes in the transaction use the new location; existing files are not moved.
// Trailing slashes are stripped so generated paths have no double separators.
// Changes across storage schemes or buckets require multi-location FileIO support.
// Call Commit to persist the change.
func (t *Transaction) SetLocation(location string) error {
	location = strings.TrimRight(location, "/")
	if strings.TrimSpace(location) == "" {
		return fmt.Errorf("%w: table location must not be empty", iceberg.ErrInvalidArgument)
	}
	if _, err := t.txnMeta(); err != nil {
		return err
	}

	current, err := url.Parse(t.tbl.Location())
	if err != nil {
		return fmt.Errorf("%w: invalid current table location: %v", iceberg.ErrInvalidArgument, err)
	}
	next, err := url.Parse(location)
	if err != nil {
		return fmt.Errorf("%w: invalid table location: %v", iceberg.ErrInvalidArgument, err)
	}
	if current.Scheme != next.Scheme || current.Host != next.Host {
		return fmt.Errorf("%w: moving a table between storage schemes or buckets requires multi-location FileIO",
			iceberg.ErrNotImplemented)
	}

	return t.apply([]Update{NewSetLocationUpdate(location)}, nil)
}

// stagedLocationProvider uses the metadata already updated by this transaction,
// rather than the original table's location and write path properties.
func (t *Transaction) stagedLocationProvider() (LocationProvider, error) {
	meta, err := t.txnMeta()
	if err != nil {
		return nil, err
	}

	return LoadLocationProvider(meta.loc, meta.props)
}
