# Shredded Variant Test Fixtures

These binary files are a subset of the canonical cross-client test
data from [apache/parquet-testing](https://github.com/apache/parquet-testing/tree/main/shredded_variant),
vendored here so the iceberg-go test suite can verify the shredded
variant writer produces output compatible with Java / pyiceberg / the
Parquet reference readers.

| File | Source case | Test name | Variant value |
| --- | --- | --- | --- |
| `case-004.parquet` + `_row-0.variant.bin` | 4 | `testShreddedVariantPrimitives` | `BOOLEAN_TRUE` (true) |
| `case-006.parquet` + `_row-0.variant.bin` | 6 | `testShreddedVariantPrimitives` | `INT8` (34) |
| `case-012.parquet` + `_row-0.variant.bin` | 12 | `testShreddedVariantPrimitives` | `INT64` (9876543210) |
| `case-046.parquet` + `_row-0.variant.bin` | 46 | `testShreddedObject` | `{a: null, b: ""}` |

## Variant `.bin` encoding

Each `*.variant.bin` file is the variant metadata bytes immediately
followed by the variant value bytes (no length prefix). Use the
in-package `splitVariantBin` helper to slice them apart before
constructing a `variant.Value`.
