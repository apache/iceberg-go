<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Verify a release

When a release candidate is announced on `dev@iceberg.apache.org`, anyone (committer or not) can help by verifying the artifacts. Verification is required from at least three Apache Iceberg PMC members for the vote to pass.

The repository ships a script that performs the full verification end-to-end: `dev/release/verify_rc.sh`.

## What an RC announcement contains

A `[VOTE] iceberg-go X.Y.Z RC<N>` thread on `dev@iceberg.apache.org` will reference:

- A signed source tarball under `https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-go-${VERSION}-rc${RC}/` (`.tar.gz`, `.tar.gz.asc`, `.tar.gz.sha512`)
- The Apache Iceberg `KEYS` file at `https://downloads.apache.org/iceberg/KEYS`
- A GitHub compare URL between the previous release tag and the new RC tag

## Prerequisites

The script requires:

- `curl`
- `gpg`
- `shasum` or `sha512sum`
- `tar`

You do not need Go installed - if Go is not on the system, the latest Go is downloaded automatically and used only for verification.

## Import the Apache Iceberg KEYS

```sh
curl https://downloads.apache.org/iceberg/KEYS -o KEYS
gpg --import KEYS
```

## Run the verification script

The script takes the version and RC number as positional arguments:

```sh
dev/release/verify_rc.sh ${VERSION} ${RC}
```

For example, to verify `0.6.0` RC1:

```sh
dev/release/verify_rc.sh 0.6.0 1
```

If the verification succeeds, the script prints:

```
RC looks good!
```

## Optional environment variables

`verify_rc.sh` honors these environment variables (all optional):

| Variable | Default | Effect |
|---|---|---|
| `VERIFY_DEFAULT` | `1` | Master switch propagated to `VERIFY_DOWNLOAD` and `VERIFY_SIGN` if they are unset. |
| `VERIFY_DOWNLOAD` | `${VERIFY_DEFAULT}` | Re-download artifacts when `1`; reuse the local copy when `0`. |
| `VERIFY_SIGN` | `${VERIFY_DEFAULT}` | Re-run signature and checksum verification when `1`. |
| `VERIFY_FORCE_USE_GO_BINARY` | `0` | When `1`, ignore any system Go and use the script's auto-downloaded Go. |
| `GITHUB_TOKEN` | unset | Optional - supplies authenticated requests when fetching the latest Go release, avoiding rate limits. |

## Manual verification fallback

If you would rather verify by hand, the underlying steps are:

```sh
# Download the artifacts
curl -O https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-go-${VERSION}-rc${RC}/apache-iceberg-go-${VERSION}.tar.gz
curl -O https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-go-${VERSION}-rc${RC}/apache-iceberg-go-${VERSION}.tar.gz.asc
curl -O https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-go-${VERSION}-rc${RC}/apache-iceberg-go-${VERSION}.tar.gz.sha512

# Verify the signature
gpg --verify apache-iceberg-go-${VERSION}.tar.gz.asc apache-iceberg-go-${VERSION}.tar.gz

# Verify the checksum (or sha512sum -c on systems without shasum)
shasum -a 512 --check apache-iceberg-go-${VERSION}.tar.gz.sha512

# Inspect the contents
tar xf apache-iceberg-go-${VERSION}.tar.gz
```

You should reply to the `[VOTE]` thread with `+1`, `+0`, or `-1` and a one-line description of what you verified (for example, "verified signatures, checksums, and `verify_rc.sh` passed on macOS arm64 with system Go 1.25.5").
