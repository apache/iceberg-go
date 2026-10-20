#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Reports incompatible changes to the module's exported API between a base
# revision and the working tree, using golang.org/x/exp/cmd/apidiff.
#
# Usage: dev/check-api-compat.sh [BASE_REF]   (default: origin/main)
#
# Run from the repository root. Exits 0 when the API is compatible with
# BASE_REF. When it is not, the changes are printed and the exit status is 1,
# unless PR_TITLE is set and marks the change as breaking with a
# Conventional Commits "!" (for example "refactor(io)!: ..."), in which case
# the changes are reported and the exit status is 0.
#
# Exit status: 0 compatible (or breaking and declared), 1 undeclared
# incompatible changes, 2 the check itself could not run (for example the base
# or the working tree does not build).
#
# When GITHUB_STEP_SUMMARY is set, the report is also appended to it.
#
# apidiff only sees the API for the host platform and default build tags, and
# it does not detect behavior changes, so a pass is not a full compatibility
# guarantee.

set -euo pipefail

BASE_REF="${1:-origin/main}"
MODULE="github.com/apache/iceberg-go"
APIDIFF="${APIDIFF:-apidiff}"
# The apidiff version CI installs; .github/workflows/api-compat.yml reads it
# from this line.
APIDIFF_VERSION="v0.0.0-20261007192929-f45ad48fbe92"

if ! command -v "${APIDIFF}" >/dev/null 2>&1; then
  echo "apidiff not found; install it with:" >&2
  echo "  go install golang.org/x/exp/cmd/apidiff@${APIDIFF_VERSION}" >&2
  exit 2
fi

work="$(mktemp -d)"
base_dir="${work}/base"
cleanup() {
  git worktree remove --force "${base_dir}" >/dev/null 2>&1 || true
  git worktree prune >/dev/null 2>&1 || true
  rm -rf "${work}"
}
trap cleanup EXIT

if ! git worktree add --quiet --detach "${base_dir}" "${BASE_REF}"; then
  echo "cannot check out base ref ${BASE_REF}" >&2
  exit 2
fi

# run_apidiff runs apidiff with its stderr captured. On failure it prints that
# stderr, minus the "Ignoring internal package" notices apidiff always emits,
# and exits 2 so a broken build is not mistaken for an API break.
run_apidiff() {
  if ! "${APIDIFF}" "$@" 2>"${work}/stderr"; then
    echo "apidiff $* failed:" >&2
    grep -v '^Ignoring internal package' "${work}/stderr" >&2 || true
    exit 2
  fi
}

# apidiff resolves the module from the current directory, so export the base
# API from inside the base checkout and compare from the repository root.
(cd "${base_dir}" && run_apidiff -m -w "${work}/base.export" "${MODULE}")
run_apidiff -m -incompatible "${work}/base.export" "${MODULE}" > "${work}/changes.txt"

summary() {
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat >> "${GITHUB_STEP_SUMMARY}" || true
  else
    cat > /dev/null
  fi
}

if [[ ! -s "${work}/changes.txt" ]]; then
  echo "No incompatible API changes against ${BASE_REF}."
  echo "No incompatible API changes against \`${BASE_REF}\`." | summary
  exit 0
fi

breaking_title='^[a-z]+(\([^)]*\))?!:'
declared=false
if [[ -n "${PR_TITLE:-}" && "${PR_TITLE}" =~ ${breaking_title} ]]; then
  declared=true
fi

echo "Incompatible API changes against ${BASE_REF}:"
cat "${work}/changes.txt"

{
  echo "### Incompatible API changes against \`${BASE_REF}\`"
  echo
  echo '```'
  cat "${work}/changes.txt"
  echo '```'
} | summary

if [[ "${declared}" == true ]]; then
  echo
  echo "The PR title marks this change as breaking, so this is reported but not failed."
  echo "The PR title marks this change as breaking, so this is reported but not failed." | summary
  exit 0
fi

echo
echo "This changes the public API incompatibly. If that is intended, mark the PR"
echo "title as breaking with a \"!\" after the type or scope, for example"
echo "\"refactor(io)!: add context to IO.Open\". Otherwise keep the old API working."
{
  echo
  echo "If this break is intended, mark the PR title as breaking with a \`!\`"
  echo "after the type or scope, for example \`refactor(io)!: add context to IO.Open\`."
} | summary
exit 1
