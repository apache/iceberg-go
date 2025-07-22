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
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	_ "embed"

	"github.com/docker/docker/api/types/container"
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
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithVersion("1.48"),
	)
	if err != nil {
		return "", err
	}
	defer func(cli *client.Client) {
		err := cli.Close()
		if err != nil {
			t.Logf("failed to close docker client")
		}
	}(cli)

	containers, err := cli.ContainerList(t.Context(), container.ListOptions{
		All: true,
	})
	if err != nil {
		return "", err
	}

	var sparkContainerID string
	for _, c := range containers {
		for _, name := range c.Names {
			if strings.Contains(name, sparkContainer) {
				sparkContainerID = c.ID

				break
			}
		}
	}
	if sparkContainerID == "" {
		return "", fmt.Errorf("unable to find container: %s", sparkContainer)
	}

	response, err := cli.ContainerExecCreate(t.Context(), sparkContainerID, container.ExecOptions{
		Cmd:          append([]string{"python", scriptPath}, args...),
		AttachStdout: true,
		AttachStderr: true,
	})
	if err != nil {
		return "", err
	}

	attachResp, err := cli.ContainerExecAttach(t.Context(), response.ID, container.ExecAttachOptions{})
	if err != nil {
		return "", err
	}
	defer attachResp.Close()

	output, err := io.ReadAll(attachResp.Reader)
	if err != nil {
		return "", err
	}
	fmt.Printf("%s\n", string(output))

	inspect, err := cli.ContainerExecInspect(t.Context(), response.ID)
	if err != nil {
		return "", err
	}
	if inspect.ExitCode != 0 {
		return "", fmt.Errorf("failed to execute script with exit code: %d", inspect.ExitCode)
	}

	return string(output), nil
}
