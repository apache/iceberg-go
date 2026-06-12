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
	"context"
	_ "embed"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/testcontainers/testcontainers-go/modules/compose"
)

//go:embed docker-compose.yml
var composeFile []byte

const sparkContainer = "spark-iceberg"

func Start(t *testing.T) (*compose.DockerCompose, error) {
	if _, ok := os.LookupEnv("AWS_S3_ENDPOINT"); ok {
		return nil, nil
	}
	stack, err := compose.NewDockerComposeWith(
		compose.WithStackReaders(bytes.NewBuffer(composeFile)),
	)
	if err != nil {
		return nil, fmt.Errorf("fail to start compose: %w", err)
	}
	if err := stack.Up(t.Context()); err != nil {
		return stack, fmt.Errorf("fail to up compose: %w", err)
	}
	spark, err := stack.ServiceContainer(t.Context(), sparkContainer)
	if err != nil {
		return stack, fmt.Errorf("fail to find spark-iceberg: %w", err)
	}
	_, stdout, err := spark.Exec(t.Context(), []string{"ipython", "./provision.py"})
	if err != nil {
		return stack, fmt.Errorf("fail to seed provision.py: %w", err)
	}
	data, err := io.ReadAll(stdout)
	if err != nil {
		return stack, fmt.Errorf("fail to read stdout: %w", err)
	}
	fmt.Println(string(data))
	t.Setenv("AWS_S3_ENDPOINT", "http://localhost:9000")
	t.Setenv("AWS_REGION", "us-east-1")

	return stack, nil
}

func ExecuteSpark(t *testing.T, scriptPath string, args ...string) (string, error) {
	return executeSparkContainer(t.Context(), scriptPath, args...)
}

// executeSparkContainer runs `python <scriptPath> <args...>` in the
// spark-iceberg container. Decoupled from *testing.T so it can be driven by
// non-test contexts (e.g. SparkMajorVersion uses context.Background()).
func executeSparkContainer(ctx context.Context, scriptPath string, args ...string) (string, error) {
	var cli *client.Client
	var err error

	if apiVersion, ok := os.LookupEnv("DOCKER_API_VERSION"); ok {
		cli, err = client.NewClientWithOpts(
			client.FromEnv,
			client.WithVersion(apiVersion),
		)
	} else {
		cli, err = client.NewClientWithOpts(
			client.FromEnv,
		)
	}
	if err != nil {
		return "", err
	}
	defer cli.Close()

	var sparkContainerID string
	if _, ok := os.LookupEnv("SPARK_CONTAINER_ID"); ok {
		sparkContainerID = os.Getenv("SPARK_CONTAINER_ID")
	} else {
		filter := filters.NewArgs(filters.Arg("name", sparkContainer))
		containers, err := cli.ContainerList(ctx, container.ListOptions{
			Filters: filter,
		})
		if err != nil {
			return "", err
		}
		if len(containers) != 1 {
			return "", fmt.Errorf("unable to find container: %s", sparkContainer)
		}
		sparkContainerID = containers[0].ID
	}

	response, err := cli.ContainerExecCreate(ctx, sparkContainerID, container.ExecOptions{
		Cmd:          append([]string{"python", scriptPath}, args...),
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", err
	}

	attachResp, err := cli.ContainerExecAttach(ctx, response.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", err
	}
	defer attachResp.Close()

	output, err := io.ReadAll(attachResp.Reader)
	if err != nil {
		return "", err
	}
	fmt.Printf("%s\n", string(output))

	inspect, err := cli.ContainerExecInspect(ctx, response.ID)
	if err != nil {
		return "", err
	}
	if inspect.ExitCode != 0 {
		return "", fmt.Errorf("failed to execute script with exit code: %d", inspect.ExitCode)
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
