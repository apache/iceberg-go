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

# Starts a local Apache Polaris server for TestExternalRestCatalogConformance
# and creates a file-backed catalog the conformance suite can use.
#
# Usage: internal/recipe/setup-polaris.sh
#
# Requires docker, curl and jq. Each run replaces the container, so the script
# can be rerun safely. On success it prints the environment variables the test
# reads. The credentials are for local testing only.

set -euo pipefail

image=apache/polaris:1.8.0
container=iceberg-go-polaris
api_port=8191
mgmt_port=8192
client_id=root
client_secret=s3cr3t
warehouse=conformance
location=file:///tmp/polaris/conformance

for cmd in docker curl jq; do
  command -v "$cmd" > /dev/null || {
    echo "setup-polaris: $cmd is required" >&2
    exit 1
  }
done

# call runs curl and prints the response body. On an HTTP error it reports the
# server's error body, which would otherwise be lost in a pipe or /dev/null.
call() {
  local body
  if ! body=$(curl -sS --fail-with-body "$@"); then
    echo "setup-polaris: request failed: ${body:-no response body}" >&2
    exit 1
  fi
  printf '%s' "$body"
}

docker rm -f "$container" > /dev/null 2>&1 || true

# Polaris refuses to start with FILE storage unless the readiness check is
# told to ignore it: FILE storage is flagged as a severe security risk, which
# is acceptable for a throwaway local server.
docker run -d --name "$container" \
  -p "$api_port:8181" -p "$mgmt_port:8182" \
  -e POLARIS_BOOTSTRAP_CREDENTIALS="POLARIS,$client_id,$client_secret" \
  -e 'polaris.features."ALLOW_INSECURE_STORAGE_TYPES"=true' \
  -e 'polaris.features."SUPPORTED_CATALOG_STORAGE_TYPES"=["FILE"]' \
  -e 'polaris.readiness.ignore-severe-issues=true' \
  "$image" > /dev/null

ready=false
for _ in $(seq 60); do
  if curl -sf "localhost:$mgmt_port/q/health/ready" > /dev/null; then
    ready=true
    break
  fi
  sleep 1
done
if [[ $ready != true ]]; then
  echo "setup-polaris: Polaris did not become ready within 60s; recent logs:" >&2
  docker logs --tail 20 "$container" >&2 || true
  exit 1
fi

api="http://localhost:$api_port/api"

# jq -e fails when the response has no access_token instead of passing an empty
# or "null" token to the calls below.
token=$(call -X POST "$api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id="$client_id" \
  -d client_secret="$client_secret" -d scope=PRINCIPAL_ROLE:ALL |
  jq -er .access_token)

call -X POST "$api/management/v1/catalogs" \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d "{\"catalog\":{\"name\":\"$warehouse\",\"type\":\"INTERNAL\",
       \"properties\":{\"default-base-location\":\"$location\"},
       \"storageConfigInfo\":{\"storageType\":\"FILE\",\"allowedLocations\":[\"$location\"]}}}" \
  > /dev/null

# The REST client requests vended credentials (X-Iceberg-Access-Delegation) by
# default, which Polaris authorizes with TABLE_WRITE_DATA. CATALOG_MANAGE_CONTENT
# includes it.
call -X PUT "$api/management/v1/catalogs/$warehouse/catalog-roles/catalog_admin/grants" \
  -H "Authorization: Bearer $token" -H 'Content-Type: application/json' \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}' \
  > /dev/null

cat << EOF
Polaris is running in container $container. Run the conformance suite with:

  REST_CATALOG_URI=$api/catalog \\
  REST_CATALOG_CREDENTIAL=$client_id:$client_secret \\
  REST_CATALOG_SCOPE=PRINCIPAL_ROLE:ALL \\
  REST_CATALOG_WAREHOUSE=$warehouse \\
    go test -tags integration -run TestExternalRestCatalogConformance ./catalog/rest

Remove the server with: docker rm -f $container
EOF
