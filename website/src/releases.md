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

# Releases

Apache Iceberg Go follows the standard Apache release process. Releases are cut from `main`, voted on by the Apache Iceberg PMC, and published as signed source tarballs to `https://downloads.apache.org/iceberg/` and as Go module versions tagged `vX.Y.Z`.

## Latest release

- [Latest release on GitHub](https://github.com/apache/iceberg-go/releases/latest)
- All releases: [github.com/apache/iceberg-go/releases](https://github.com/apache/iceberg-go/releases)

## Using a release

Pin a tagged version directly with Go modules:

```sh
go get github.com/apache/iceberg-go@vX.Y.Z
```

To track `main`, use `@main` instead of a version tag.

## Release notes

Per-release notes (highlights, breaking changes, contributors) are published on the [GitHub Releases page](https://github.com/apache/iceberg-go/releases). Iceberg Go does not maintain a curated `CHANGELOG.md` in the repository; the GitHub Releases page is the canonical source.

## Verifying and producing releases

If you are validating an RC or cutting a new release, see:

- [Verify a release](./verify-release.md) - what to do when a `[VOTE]` thread is posted on `dev@iceberg.apache.org`.
- [How to release](./how-to-release.md) - PMC/committer process for cutting an RC and publishing.
