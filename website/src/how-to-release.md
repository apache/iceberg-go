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

# How to release

This page is for Apache Iceberg PMC members and committers who are cutting a new Apache Iceberg Go release. The canonical scripts live in [`dev/release/`](https://github.com/apache/iceberg-go/tree/main/dev/release) and `dev/release/README.md` is the source of truth - this page mirrors it and adds context.

## Requirements

- You must be an Apache Iceberg committer or PMC member.
- You must prepare a PGP key for signing. See [https://infra.apache.org/release-signing.html#generate](https://infra.apache.org/release-signing.html#generate).
- Your PGP key must be registered in the Apache Iceberg `KEYS` file at `https://downloads.apache.org/iceberg/KEYS`. To add a key:

  ```sh
  svn co https://dist.apache.org/repos/dist/release/iceberg
  cd iceberg
  $EDITOR KEYS
  svn ci KEYS
  ```

- You must run the release scripts from a working copy whose `origin` remote is `git@github.com:apache/iceberg-go.git` (not your fork). `release_rc.sh` enforces this.

## Overview

1. Test the revision to be released.
2. Prepare an RC and start a vote.
3. On a passing vote, publish.

## Prepare an RC and vote

Run `dev/release/release_rc.sh` against the canonical clone:

```sh
git clone git@github.com:apache/iceberg-go.git
cd iceberg-go
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh ${VERSION} ${RC}
```

Example for `0.6.0` RC1:

```sh
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh 0.6.0 1
```

The arguments are the version and the RC number. If `RC1` has a problem, increment the RC number to `RC2`, `RC3`, and so on.

`release_rc.sh` will:

- Tag `vX.Y.Z-rc<N>` and push the tag.
- Create a signed source tarball.
- Upload the artifacts to `https://dist.apache.org/repos/dist/dev/iceberg/`.
- Print a draft `[VOTE]` email you can use on `dev@iceberg.apache.org`.

Send the `[VOTE]` email. The vote runs for at least 72 hours and requires three +1 votes from PMC members with no -1 votes to pass.

## Publish

When the vote passes, run:

```sh
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release.sh ${VERSION} ${RC}
```

`release.sh` moves the artifacts from `https://dist.apache.org/repos/dist/dev/iceberg/` to `https://dist.apache.org/repos/dist/release/iceberg/` (which feeds `https://downloads.apache.org/iceberg/`) and creates a GitHub Release with auto-generated notes.

## Post-release tasks

After publishing, complete these steps:

1. Add the release to ASF's report database at the [Apache Committee Report Helper](https://reporter.apache.org/addrelease.html?iceberg).
2. Verify the GitHub Release at `https://github.com/apache/iceberg-go/releases` is correctly tagged, has generated release notes against the prior tag, and is marked as latest. `release.sh` runs `gh release create ... --generate-notes --verify-tag` so the release should already exist; double-check the notes and the "Latest" badge.
3. Send the `[ANNOUNCE]` email to `dev@iceberg.apache.org` and `announce@apache.org`.
4. File a release blog post in [`apache/iceberg`](https://github.com/apache/iceberg) under `site/docs/blog/posts/`. See the prior 0.5.0 post (`2026-03-05-iceberg-go-0.5.0-release.md`) for the frontmatter and structure.

## Patch releases

`dev/release/README.md` does not document a patch-branch convention (e.g. `iceberg-go-0.X.x`). Confirm with the PMC on `dev@iceberg.apache.org` before cutting a patch release.
