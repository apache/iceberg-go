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

## Refreshing the fixtures

The upstream files live in
`apache/parquet-testing/shredded_variant/`. Update with `gh api`:

```bash
gh api repos/apache/parquet-testing/contents/shredded_variant/<file> \
    | python3 -c 'import sys, json, base64; data = json.load(sys.stdin); sys.stdout.buffer.write(base64.b64decode(data["content"]))' \
    > <file>
```

When apache/iceberg-go#986 (shredded reader) lands, this directory
should be expanded to cover every applicable `cases.json` entry so
the iceberg-go reader can self-verify too.
