// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package uuid is the only place in the module that names the UUID
// implementation, so it can be swapped for the standard library without
// touching callers. Only API that both github.com/google/uuid and the
// standard library uuid package can provide belongs here.
package uuid

import "github.com/google/uuid"

// UUID is a 16-byte RFC 9562 UUID.
type UUID = uuid.UUID

// New returns a random (version 4) UUID.
func New() UUID { return uuid.New() }

// Parse decodes s into a UUID.
func Parse(s string) (UUID, error) { return uuid.Parse(s) }

// MustParse is like Parse but panics if s cannot be parsed.
func MustParse(s string) UUID { return uuid.MustParse(s) }

// FromBytes returns the UUID held in b, which must be exactly 16 bytes long.
func FromBytes(b []byte) (UUID, error) { return uuid.FromBytes(b) }
