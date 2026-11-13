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

"""Create tiny ORC TIMESTAMP fixtures and verify wall-clock values with PyArrow.

Each fixture uses one uncompressed stripe, RLEv1 DATA and SECONDARY streams,
and the writerTimezone attribute defined by the ORC specification. The DATA
seconds are relative to 2015-01-01 midnight in the named writer timezone.
"""
from pathlib import Path
import datetime
import zoneinfo
import pyarrow as pa
import pyarrow.orc as orc


def uvarint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def varint_field(field, value):
    return uvarint(field << 3) + uvarint(value)


def bytes_field(field, value):
    return uvarint((field << 3) | 2) + uvarint(len(value)) + value


def stream(kind, column, data):
    return bytes_field(1, varint_field(1, kind) + varint_field(2, column) + varint_field(3, len(data)))


def literal(value, signed):
    encoded = ((value << 1) ^ (value >> 63)) if signed else value
    return b"\xff" + uvarint(encoded)


def fixture(zone, wall_clock, path, nested=False):
    location = zoneinfo.ZoneInfo(zone)
    base = datetime.datetime(2015, 1, 1, tzinfo=location)
    value = datetime.datetime.fromisoformat(wall_clock).replace(tzinfo=location)
    seconds = int(value.timestamp() - base.timestamp())
    primary, secondary = literal(seconds, True), literal(0, False)
    column = 2 if nested else 1
    sf = stream(1, column, primary) + stream(5, column, secondary)
    sf += bytes_field(2, varint_field(1, 0)) * (column + 1)
    sf += bytes_field(3, zone.encode())
    body = primary + secondary + sf
    stripe = varint_field(1, 3) + varint_field(2, 0)
    stripe += varint_field(3, len(primary + secondary)) + varint_field(4, len(sf)) + varint_field(5, 1)
    name = b"nested" if nested else b"ts"
    root = varint_field(1, 12) + bytes_field(2, uvarint(1)) + bytes_field(3, name)
    types = bytes_field(4, root)
    if nested:
        struct = varint_field(1, 12) + bytes_field(2, uvarint(2)) + bytes_field(3, b"ts")
        struct += bytes_field(7, bytes_field(1, b"iceberg.id") + bytes_field(2, b"1"))
        types += bytes_field(4, struct)
    field_id = b"2" if nested else b"1"
    attributes = bytes_field(1, b"iceberg.id") + bytes_field(2, field_id)
    timestamp = varint_field(1, 9) + bytes_field(7, attributes)
    types += bytes_field(4, timestamp)
    footer = varint_field(1, 3) + varint_field(2, len(body)) + bytes_field(3, stripe)
    footer += types + varint_field(6, 1) + varint_field(8, 0)
    ps = varint_field(1, len(footer)) + varint_field(2, 0) + varint_field(3, 262144)
    ps += bytes_field(4, b"\x00\x0c") + varint_field(5, 0) + varint_field(6, 6) + bytes_field(8000, b"ORC")
    path.write_bytes(b"ORC" + body + footer + ps + bytes([len(ps)]))
    table = orc.read_table(path)
    values = table["nested"].flatten()[0] if nested else table["ts"]
    actual = values.cast(pa.int64()).to_pylist()
    expected = int(datetime.datetime.fromisoformat(wall_clock).replace(tzinfo=datetime.timezone.utc).timestamp() * 1_000_000_000)
    assert actual == [expected], (path, actual, expected)
    print(f"{path.name}: writerTimezone={zone}, DATA={seconds}, PyArrow timestamp_ns={actual}, expected={wall_clock}")


out = Path(__file__).parent
fixture("America/Los_Angeles", "2024-07-09T12:34:56", out / "timestamp_los_angeles.orc")
fixture("Asia/Kathmandu", "1980-01-02T12:34:56", out / "timestamp_kathmandu.orc")

fixture("America/Los_Angeles", "2024-07-09T12:34:56", out / "timestamp_nested_los_angeles.orc", nested=True)
