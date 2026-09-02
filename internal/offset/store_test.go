package offset

import (
	"path/filepath"
	"testing"

	"github.com/sorenhoang/gokaf/internal/storage"
)

func TestStoreCommitFetchAndMiss(t *testing.T) {
	log, err := storage.Open(filepath.Join(t.TempDir(), "__consumer_offsets-0"))
	if err != nil {
		t.Fatalf("open log: unexpected error: %v", err)
	}
	defer log.Close()

	store, err := NewStore(log)
	if err != nil {
		t.Fatalf("NewStore: unexpected error: %v", err)
	}

	if got := store.Fetch("group-a", "orders", 0); got != -1 {
		t.Fatalf("missing offset: got %d, want -1", got)
	}
	if err := store.Commit("group-a", "orders", 0, 42); err != nil {
		t.Fatalf("Commit: unexpected error: %v", err)
	}
	if got := store.Fetch("group-a", "orders", 0); got != 42 {
		t.Fatalf("committed offset: got %d, want 42", got)
	}
}

func TestStoreReplayRestoresLastCommittedOffset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "__consumer_offsets-0")
	log, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("open log: unexpected error: %v", err)
	}
	store, err := NewStore(log)
	if err != nil {
		t.Fatalf("NewStore: unexpected error: %v", err)
	}
	if err := store.Commit("group-a", "orders", 0, 41); err != nil {
		t.Fatalf("Commit first: unexpected error: %v", err)
	}
	if err := store.Commit("group-a", "orders", 0, 42); err != nil {
		t.Fatalf("Commit second: unexpected error: %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close log: unexpected error: %v", err)
	}

	reopenedLog, err := storage.Open(dir)
	if err != nil {
		t.Fatalf("reopen log: unexpected error: %v", err)
	}
	defer reopenedLog.Close()
	replayed, err := NewStore(reopenedLog)
	if err != nil {
		t.Fatalf("NewStore replay: unexpected error: %v", err)
	}

	if got := replayed.Fetch("group-a", "orders", 0); got != 42 {
		t.Fatalf("replayed offset: got %d, want 42", got)
	}
}
