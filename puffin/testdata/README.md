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

## Canonical fixtures from apache/iceberg

`empty-puffin-uncompressed.bin`, `sample-metric-data-uncompressed.bin`, and
`sample-metric-data-compressed-zstd.bin` are canonical Puffin files from the
Apache Iceberg Java implementation:
https://github.com/apache/iceberg/tree/main/core/src/test/resources/org/apache/iceberg/puffin/v1

## Deletion-vector cross-impl fixtures

`deletion-vector-v1-payload.bin` is a Java-produced 64-bit Roaring deletion
vector payload lifted directly from apache/iceberg's test resources. 50 bytes
total: 4-byte BE length, 4-byte 0xD1D33964 magic, serialized roaring bitmap
(38 bytes), 4-byte BE CRC32. The bitmap encodes 5 deleted positions
(1, 3, 5, 7, 9). Source:
https://github.com/apache/iceberg/blob/main/core/src/test/resources/org/apache/iceberg/deletes/small-alternating-values-position-index.bin

`deletion-vector-v1.puffin` wraps that payload in a complete Puffin envelope:
blob type `deletion-vector-v1`, snapshot-id and sequence-number set to -1
per spec, with `referenced-data-file` and `cardinality` properties. The
envelope is what `puffin.Writer` emits today; this is a Go-writer wire-
format pin, not a strong Java cross-impl pin. The basic envelope shape is
cross-checked by `TestWriterBitIdenticalWithJava`, but that test does not
exercise empty `Fields` arrays or multi-key blob `Properties` — both of
which this fixture relies on — and JSON key ordering of blob `Properties`
is encoder-defined. The property values
(`referenced-data-file=data/test.parquet`, `cardinality=5`,
`created-by="iceberg-go test fixture"`) are fixture choices, not bytes
inherited from any specific Java-emitted file.

To regenerate after a deliberate puffin-format change:

```
go generate ./puffin/...
```

The generator lives in `puffin/gen_dv_fixture.go` (built with the
`//go:build ignore` tag and run via the `//go:generate` directive in
`puffin/dv_golden_test.go`). It self-validates by reading the freshly-
written envelope back before overwriting the on-disk file, so a writer
bug producing a valid-but-unreadable file fails the regen rather than
calcifying into the fixture.

After regen, diff the file before committing to verify only intended bytes
changed:

```
git diff -- puffin/testdata/deletion-vector-v1.puffin
```
