# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License");
# you may not use it except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# golangci-lint version (keep in sync with CI and README)
GOLANGCI_LINT_VERSION := v2.8.0

.PHONY: test lint lint-install integration-setup integration-test integration-scanner integration-io integration-rest integration-spark

test:
	go test -v ./...

lint:
	golangci-lint run --timeout=10m

lint-install:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

integration-setup:
	docker compose -f internal/recipe/docker-compose.yml up -d
	sleep 10
	docker compose -f internal/recipe/docker-compose.yml exec -T spark-iceberg ipython ./provision.py
	sleep 10

integration-test: integration-scanner integration-io integration-rest integration-spark integration-hive

integration-scanner:
	go test -tags=integration -v -run="^TestScanner" ./table

integration-io:
	go test -tags=integration -v ./io/...

integration-rest:
	go test -tags=integration -v -run="^TestRestIntegration$$" ./catalog/rest

integration-spark:
	go test -tags=integration -v -run="^TestSparkIntegration" ./table

integration-hive:
	go test -tags=integration -v ./catalog/hive/...
