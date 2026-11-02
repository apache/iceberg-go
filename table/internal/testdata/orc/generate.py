# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Generate ORC compatibility fixtures with PyArrow 24.0.0."""
from pathlib import Path

import pyarrow as pa
import pyarrow.orc as orc

out = Path(__file__).parent
schema = pa.schema([
    ("i", pa.int32()), ("l", pa.int64()), ("f", pa.float32()),
    ("d", pa.float64()), ("s", pa.string()), ("b", pa.binary()),
    ("yes", pa.bool_()), ("a", pa.list_(pa.int32())),
    ("nested", pa.struct([("x", pa.int64()), ("s", pa.string())])),
])
rows = [
    {"i": 1, "l": 9000000001, "f": 1.5, "d": 2.25, "s": "a", "b": b"\x00\xff", "yes": True, "a": [1, None, 3], "nested": {"x": 42, "s": "hi"}},
    {"i": None, "l": -5, "f": None, "d": -1.25, "s": "", "b": b"", "yes": False, "a": [], "nested": {"x": None, "s": "b"}},
    {"i": 3, "l": None, "f": -4.5, "d": None, "s": None, "b": None, "yes": None, "a": None, "nested": None},
]
table = pa.Table.from_pylist(rows, schema=schema)
for codec in ("uncompressed", "zlib", "snappy"):
    orc.write_table(table, out / f"pyarrow_{codec}.orc", compression=codec)

schema = pa.schema([("id", pa.int64()), ("v", pa.string()), ("a", pa.list_(pa.int32()))])
rows = [
    {"id": i, "v": None if i % 17 == 0 else str(i), "a": [] if i % 5 == 0 else [i, None]}
    for i in range(5000)
]
orc.write_table(
    pa.Table.from_pylist(rows, schema=schema), out / "pyarrow_multibatch.orc",
    compression="zlib", stripe_size=4096, batch_size=256,
)
