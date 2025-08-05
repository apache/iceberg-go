//go:build integration

package hive

import (
	"context"
	"testing"

	compose "github.com/testcontainers/testcontainers-go/modules/compose"

	"github.com/apache/iceberg-go/table"
)

// Example integration test using docker-compose to start a Hive Metastore.
// Run with: go test -tags=integration ./catalog/hive -run TestHiveCatalogIntegration
func TestHiveCatalogIntegration(t *testing.T) {
	ctx := context.Background()
	cm, err := compose.NewDockerComposeWith(ctx, compose.WithStackFiles("testdata/docker-compose.yml"))
	if err != nil {
		t.Skipf("compose not available: %v", err)
	}
	t.Cleanup(func() {
		cm.Down(ctx, compose.RemoveOrphans(true), compose.RemoveImagesLocal)
	})
	if err := cm.Up(ctx); err != nil {
		t.Fatalf("compose up: %v", err)
	}

	cfg := Config{Host: "localhost", Port: 9083, Auth: "NONE"}
	cat, err := NewHiveCatalog(cfg)
	if err != nil {
		t.Fatalf("new catalog: %v", err)
	}

	if err := cat.CreateNamespace(ctx, table.Identifier{"default"}, nil); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if _, err := cat.ListNamespaces(ctx, nil); err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
}
