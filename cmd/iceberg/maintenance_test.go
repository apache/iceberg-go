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

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseDurationRejectsInvalidValues(t *testing.T) {
	for _, input := range []string{"-1d", "-1h", "NaNd", "+Infd", "106752d"} {
		t.Run(input, func(t *testing.T) {
			_, err := parseDuration(input)
			require.Error(t, err)
		})
	}
}

func TestParseDurationAcceptsZeroAndChecksDayOverflow(t *testing.T) {
	zero, err := parseDuration("0d")
	require.NoError(t, err)
	require.Zero(t, zero)

	withinRange, err := parseDuration("106751d")
	require.NoError(t, err)
	require.Equal(t, 106751*24*time.Hour, withinRange)

	_, err = parseDuration("2562047h47m16.854775808s")
	require.Error(t, err)
}
