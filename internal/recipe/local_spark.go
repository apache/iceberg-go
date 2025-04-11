// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package recipe

import (
	"bytes"
	_ "embed"
	"fmt"
	"io"
	"os"
	"testing"

	"golang.org/x/xerrors"

	"github.com/testcontainers/testcontainers-go/modules/compose"
)

//go:embed docker-compose.yml
var composeFile []byte

func Start(t *testing.T) error {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return nil
	}
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackReaders(bytes.NewBuffer(composeFile)),
	)
	if err != nil {
		return xerrors.Errorf("fail to start compose: %w", err)
	}
	if err := stack.Up(t.Context()); err != nil {
		return xerrors.Errorf("fail to up compose: %w", err)
	}
	spark, err := stack.ServiceContainer(t.Context(), "spark-iceberg")
	if err != nil {
		return xerrors.Errorf("fail to find spark-iceberg: %w", err)
	}
	_, stdout, err := spark.Exec(t.Context(), []string{"ipython", "./provision.py"})
	if err != nil {
		return xerrors.Errorf("fail to seed provision.py: %w", err)
	}
	data, err := io.ReadAll(stdout)
	if err != nil {
		return xerrors.Errorf("fail to read stdout: %w", err)
	}
	fmt.Println(string(data))
	t.Setenv("AWS_S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("AWS_REGION", "us-east-1")

	return nil
}
