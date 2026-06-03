<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

These test fixture files are canonical deletion vector and roaring bitmap files from the Apache Iceberg Java implementation:
https://github.com/apache/iceberg/tree/main/core/src/test/resources/org/apache/iceberg/deletes

`single-blob-dv.puffin` and `multi-blob-dv.puffin` are Java-authored Puffin
files used by the cross-client tests in `table/dv/dv_cross_client_test.go`.
Apache Iceberg upstream does not commit any `.puffin` DV fixtures, so they are
produced from `dev/dv-fixtures/GenerateDVFixtures.java` against an
`apache/iceberg` checkout — see `dev/dv-fixtures/README.md` for regeneration
instructions.
