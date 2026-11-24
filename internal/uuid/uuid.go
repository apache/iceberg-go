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
// touching callers.
package uuid

import "github.com/google/uuid"

type UUID = uuid.UUID

var Nil = uuid.Nil

func New() UUID { return uuid.New() }

func NewRandom() (UUID, error) { return uuid.NewRandom() }

func Parse(s string) (UUID, error) { return uuid.Parse(s) }

func FromBytes(b []byte) (UUID, error) { return uuid.FromBytes(b) }
