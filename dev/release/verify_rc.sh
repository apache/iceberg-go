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

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TOP_SOURCE_DIR="$(dirname "$(dirname "${SOURCE_DIR}")")"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 18.0.0 1"
  exit 1
fi

set -o pipefail
set -x

VERSION="$1"
RC="$2"

ICEBERG_DIST_BASE_URL="https://downloads.apache.org/iceberg"
DOWNLOAD_RC_BASE_URL="https://github.com/apache/iceberg-go/releases/download/v${VERSION}-rc${RC}"
ARCHIVE_BASE_NAME="apache-iceberg-go-${VERSION}"

: "${VERIFY_DEFAULT:=1}"
: "${VERIFY_DOWNLOAD:=${VERIFY_DEFAULT}}"
: "${VERIFY_FORCE_USE_GO_BINARY:=0}"
: "${VERIFY_SIGN:=${VERIFY_DEFAULT}}"

VERIFY_SUCCESS=no

setup_tmpdir() {
  cleanup() {
    go clean -modcache || :
    if [ "${VERIFY_SUCCESS}" = "yes" ]; then
      rm -rf "${VERIFY_TMPDIR}"
    else
      echo "Failed to verify release candidate. See ${VERIFY_TMPDIR} for details."
    fi
  }

  if [ -z "${VERIFY_TMPDIR:-}" ]; then
    VERIFY_TMPDIR="$(mktemp -d -t "$1.XXXXX")"
    trap cleanup EXIT
  else
    mkdir -p "${VERIFY_TMPDIR}"
  fi
}

download() {
  curl \
    --fail \
    --location \
    --remote-name \
    --show-error \
    --silent \
    "$1"
}

download_rc_file() {
  if [ "${VERIFY_DOWNLOAD}" -gt 0 ]; then
    download "${DOWNLOAD_RC_BASE_URL}/$1"
  else
    cp "${TOP_SOURCE_DIR}/$1" "$1"
  fi
}

import_gpg_keys() {
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download "${ICEBERG_DIST_BASE_URL}/KEYS"
    gpg --import KEYS
  fi
}

if type shasum >/dev/null 2>&1; then
  sha256_verify="shasum -a 256 -c"
  sha512_verify="shasum -a 512 -c"
else
  sha256_verify="sha256sum -c"
  sha512_verify="sha512sum -c"
fi

fetch_archive() {
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz"
  if [ "${VERIFY_SIGN}" -gt 0 ]; then
    download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.asc"
    gpg --verify "${ARCHIVE_BASE_NAME}.tar.gz.asc" "${ARCHIVE_BASE_NAME}.tar.gz"
  fi
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.sha256"
  ${sha256_verify} "${ARCHIVE_BASE_NAME}.tar.gz.sha256"
  download_rc_file "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
  ${sha512_verify} "${ARCHIVE_BASE_NAME}.tar.gz.sha512"
}

ensure_source_directory() {
  tar xf "${ARCHIVE_BASE_NAME}".tar.gz
}

latest_go_version() {
  local -a options
  options=(
    --fail
    --location
    --show-error
    --silent
  )
  if [ -n "${GITHUB_TOKEN:-}" ]; then
    options+=("--header" "Authorization: Bearer ${GITHUB_TOKEN}")
  fi
  curl \
    "${options[@]}" \
    https://api.github.com/repos/golang/go/git/matching-refs/tags/go |
    grep -o '"ref": "refs/tags/go.*"' |
    tail -n 1 |
    sed \
      -e 's,^"ref": "refs/tags/go,,g' \
      -e 's/"$//g'
}

ensure_go() {
  if [ "${VERIFY_FORCE_USE_GO_BINARY}" -le 0 ]; then
    if go version; then
      GOPATH="${VERIFY_TMPDIR}/gopath"
      export GOPATH
      mkdir -p "${GOPATH}"
      return
    fi
  fi

 local go_version
  go_version=$(latest_go_version)
  local go_os
  go_os="$(uname)"
  case "${go_os}" in
  Darwin)
    go_os="darwin"
    ;;
  Linux)
    go_os="linux"
    ;;
  esac
  local go_arch
  go_arch="$(arch)"
  case "${go_arch}" in
  i386 | x86_64)
    go_arch="amd64"
    ;;
  aarch64)
    go_arch="arm64"
    ;;
  esac
  local go_binary_tar_gz
  go_binary_tar_gz="go${go_version}.${go_os}-${go_arch}.tar.gz"
  local go_binary_url
  go_binary_url="https://go.dev/dl/${go_binary_tar_gz}"
  curl \
    --fail \
    --location \
    --output "${go_binary_tar_gz}" \
    --show-error \
    --silent \
    "${go_binary_url}"
  tar xf "${go_binary_tar_gz}"
  GOROOT="$(pwd)/go"
  export GOROOT
  GOPATH="$(pwd)/gopath"
  export GOPATH
  mkdir -p "${GOPATH}"
  PATH="${GOROOT}/bin:${GOPATH}/bin:${PATH}"
}

test_source_distribution() {
  go test -v ./...
  # TODO: run integration tests
}

setup_tmpdir "iceberg-go-${VERSION}-${RC}"
echo "Working in sandbox ${VERIFY_TMPDIR}"
cd "${VERIFY_TMPDIR}"

import_gpg_keys
fetch_archive
ensure_source_directory
ensure_go
pushd "${ARCHIVE_BASE_NAME}"
test_source_distribution
popd

VERIFY_SUCCESS=yes
echo "RC looks good!"
