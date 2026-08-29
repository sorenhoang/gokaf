package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManagerReturnsSameLogForSameTopicPartition(t *testing.T) {
	manager := NewManager(t.TempDir())
	defer closeManager(t, manager)

	first, err := manager.Log("orders", 0)
	if err != nil {
		t.Fatalf("Log first: unexpected error: %v", err)
	}
	second, err := manager.Log("orders", 0)
	if err != nil {
		t.Fatalf("Log second: unexpected error: %v", err)
	}

	if first != second {
		t.Fatal("Log returned different handles for the same topic partition")
	}
}

func TestManagerOpensTopicPartitionDirectory(t *testing.T) {
	dir := t.TempDir()
	manager := NewManager(dir)
	defer closeManager(t, manager)

	log, err := manager.Log("orders", 0)
	if err != nil {
		t.Fatalf("Log: unexpected error: %v", err)
	}
	if _, err := log.Append([]byte("hello")); err != nil {
		t.Fatalf("Append: unexpected error: %v", err)
	}

	wantPath := filepath.Join(dir, "orders-0", "00000000000000000000.log")
	if !fileExists(wantPath) {
		t.Fatalf("segment file %s does not exist", wantPath)
	}
}

func closeManager(t *testing.T, manager *Manager) {
	t.Helper()

	if err := manager.Close(); err != nil {
		t.Fatalf("Close manager: unexpected error: %v", err)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
