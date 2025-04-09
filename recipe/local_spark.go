package recipe

import (
	"bytes"
	_ "embed"
	"fmt"
	"golang.org/x/xerrors"
	"io"
	"os"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/compose"
)

var (
	//go:embed docker-compose.yml
	composeFile []byte
)

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
