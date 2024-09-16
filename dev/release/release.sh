#!/usr/bin/env bash
#
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

set -eu

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 1.0.0 1"
  exit 1
fi

version=$1
rc=$2

git_origin_url="$(git remote get-url origin)"
repository="${git_origin_url#*github.com?}"
repository="${repository%.git}"
if [ "${git_origin_url}" != "git@github.com:apache/iceberg-go.git" ]; then
  echo "This script must be ran with a working copy of apache/iceberg-go."
  echo "The origin's URL: ${git_origin_url}"
  exit 1
fi

tag="v${version}"
rc_tag="${tag}-rc${rc}"
echo "Tagging for release: ${tag}"
git tag "${tag}" "${rc_tag}"
git push origin "${tag}"

dist_url="https://dist.apache.org/repos/dist/release/iceberg"
dist_dir="dev/release/dist"
echo "Checking out ${dist_url}"
rm -rf "${dist_dir}"
svn co --depth=empty "${dist_url}" "${dist_dir}"
gh release download "${rc_tag}" \
  --repo "${repository}" \
  --dir "${dist_dir}" \
  --skip-existing

release_id="apache-iceberg-go-${version}"
echo "Uploading to release/"
pushd "${dist_dir}"
svn add .
svn ci -m "Apache Iceberg Go ${version}"
popd
rm -rf "${dist_dir}"

echo "Keep only the latest verisons"
old_releases=$(
  svn ls https://dist.apache.org/repos/dist/release/iceberg/ |
  grep -E '^apache-iceberg-go-' |
  sort --version-sort --reverse |
  tail -n +2
)
for old_release_version in ${old_releases}; do
  echo "Remove old release ${old_release_version}"
  svn \
    delete \
    -m "Remove old Apache Iceberg Go release: ${old_release_version}" \
    "https://dist.apache.org/repos/dist/release/iceberg/${old_release_version}"
done

echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/iceberg/${release_id}"
echo
echo "Add this release to ASF's report database:"
echo "  https://reporter.apache.org/addrelease.html?iceberg"
