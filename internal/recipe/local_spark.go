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
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

const sparkContainer = "spark-iceberg"

func Start(t *testing.T) error {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return nil
	}
	if err := startStack(); err != nil {
		return err
	}
	t.Setenv("AWS_S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("AWS_REGION", "us-east-1")

	return nil
}

var startStack = newStartStack()

// newStartStack caches upAndProvision's first result, errors included: provision.py is not idempotent.
func newStartStack() func() error {
	return sync.OnceValue(upAndProvision)
}

func upAndProvision() error {
	// Compose resolves build: . and ./warehouse relative to the compose file.
	_, src, _, _ := runtime.Caller(0)
	composeFile := filepath.Join(filepath.Dir(src), "docker-compose.yml")

	out, err := exec.Command("docker", "compose", "-f", composeFile, "up", "-d", "--wait").CombinedOutput()
	if err != nil {
		return fmt.Errorf("fail to up compose: %w\n%s", err, out)
	}

	out, err = exec.Command("docker", "compose", "-f", composeFile,
		"exec", "-T", sparkContainer, "ipython", "./provision.py").CombinedOutput()
	if err != nil {
		return fmt.Errorf("fail to seed provision.py: %w\n%s", err, out)
	}
	fmt.Println(string(out))

	return nil
}

func ExecuteSpark(t *testing.T, scriptPath string, args ...string) (string, error) {
	return executeSparkContainer(t.Context(), scriptPath, args...)
}

// executeSparkContainer runs `python <scriptPath> <args...>` in the
// spark-iceberg container. Decoupled from *testing.T so it can be driven by
// non-test contexts (e.g. SparkMajorVersion uses context.Background()).
func executeSparkContainer(ctx context.Context, scriptPath string, args ...string) (string, error) {
	container := sparkContainer
	if id, ok := os.LookupEnv("SPARK_CONTAINER_ID"); ok {
		container = id
	}

	cmd := exec.CommandContext(ctx, "docker", append([]string{"exec", container, "python", scriptPath}, args...)...)
	output, err := cmd.CombinedOutput()
	fmt.Printf("%s\n", output)
	if err != nil {
		return "", fmt.Errorf("failed to execute script %s: %w", scriptPath, err)
	}

	return string(output), nil
}

var (
	sparkMajorMu sync.Mutex
	sparkMajor   int // 0 = not yet extracted; 3 or 4 once cached
)

// SparkMajorVersion returns the major pyspark version (3 or 4) of the
// spark-iceberg container. Extraction runs once per process on success;
// failures are not cached so transient docker-exec errors do not poison the
// result for the rest of the run.
func SparkMajorVersion() (int, error) {
	sparkMajorMu.Lock()
	defer sparkMajorMu.Unlock()

	if sparkMajor != 0 {
		return sparkMajor, nil
	}

	out, err := executeSparkContainer(context.Background(), "-c",
		`import pyspark; print('PYSPARK_MAJOR=' + pyspark.__version__.split('.')[0])`)
	if err != nil {
		return 0, fmt.Errorf("extract spark version: %w", err)
	}

	switch {
	case strings.Contains(out, "PYSPARK_MAJOR=4"):
		sparkMajor = 4
	case strings.Contains(out, "PYSPARK_MAJOR=3"):
		sparkMajor = 3
	default:
		return 0, fmt.Errorf("unrecognized pyspark version output: %q", out)
	}

	return sparkMajor, nil
}
