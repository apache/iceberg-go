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

// Package sql implements a catalog that keeps table and view metadata
// locations in the same database tables as Java's JdbcCatalog. Import it to
// register the "sql" catalog type. That type needs the [DialectKey]
// ("sql.dialect") property and a [DriverKey] ("sql.driver") property naming a
// registered database/sql driver. A namespace with "." inside a level is stored
// under a JSON key, which Java's JdbcCatalog reads as a different namespace.
package sql
