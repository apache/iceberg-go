package hive

import (
	"testing"

	"github.com/beltran/gohive"
)

func TestNewHiveCatalogConnectionError(t *testing.T) {
	cfg := Config{Host: "127.0.0.1", Port: 1, Auth: "NONE"}
	if _, err := NewHiveCatalog(cfg); err == nil {
		t.Fatalf("expected error connecting to metastore")
	}
}

func TestHiveCatalogReconnectError(t *testing.T) {
	c := &HiveCatalog{host: "127.0.0.1", port: 1, auth: "NONE", options: gohive.NewMetastoreConnectConfiguration()}
	if err := c.withRetry(func(client metastoreClient) error { return nil }); err == nil {
		t.Fatalf("expected reconnection error")
	}
}
