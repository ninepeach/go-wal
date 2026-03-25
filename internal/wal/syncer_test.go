package wal

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestFileSyncerSyncAlways(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 0, 0, 0)

	if err := s.Request(context.Background(), SyncRequest{Bytes: 10}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := s.Request(context.Background(), SyncRequest{Bytes: 20}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	if got := s.SyncCount(); got != 2 {
		t.Fatalf("SyncCount() = %d, want 2", got)
	}
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0", got)
	}
}

func TestFileSyncerSyncBatchFlushOnThreshold(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 1, time.Hour, 16)
	defer s.Close()

	if err := s.Request(context.Background(), SyncRequest{Bytes: 8}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if got := s.SyncCount(); got != 0 {
		t.Fatalf("SyncCount() = %d, want 0 before threshold", got)
	}

	if err := s.Request(context.Background(), SyncRequest{Bytes: 8}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	waitForInternalSyncCount(t, s, 1)

	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0 after threshold flush", got)
	}
}

func TestFileSyncerSyncBatchFlushOnInterval(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 1, 20*time.Millisecond, 0)
	defer s.Close()

	if err := s.Request(context.Background(), SyncRequest{Bytes: 8}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	waitForInternalSyncCount(t, s, 1)

	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0 after interval flush", got)
	}
}

func TestFileSyncerSyncNone(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 2, 0, 0)
	defer s.Close()

	if err := s.Request(context.Background(), SyncRequest{Bytes: 10}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := s.Request(context.Background(), SyncRequest{Bytes: 20}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}

	if got := s.SyncCount(); got != 0 {
		t.Fatalf("SyncCount() = %d, want 0", got)
	}
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0", got)
	}
}

func TestFileSyncerFlushBatch(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 1, time.Hour, 0)
	defer s.Close()

	if err := s.Request(context.Background(), SyncRequest{Bytes: 12}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if got := s.PendingBytes(); got != 12 {
		t.Fatalf("PendingBytes() = %d, want 12", got)
	}

	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got := s.SyncCount(); got != 1 {
		t.Fatalf("SyncCount() = %d, want 1", got)
	}
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0", got)
	}
}

func TestFileSyncerCloseFlushesPendingBatchWrites(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 1, time.Hour, 0)

	if err := s.Request(context.Background(), SyncRequest{Bytes: 10}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if got := s.PendingBytes(); got == 0 {
		t.Fatalf("PendingBytes() = %d, want > 0 before Close", got)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	if got := s.SyncCount(); got != 1 {
		t.Fatalf("SyncCount() = %d, want 1", got)
	}
	if got := s.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0", got)
	}
}

func TestFileSyncerCloseIsIdempotent(t *testing.T) {
	file := openTempFile(t)
	defer file.Close()

	s := NewFileSyncer(file, 1, time.Hour, 0)

	if err := s.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestFileSyncerSetFile(t *testing.T) {
	file1 := openTempFile(t)
	defer file1.Close()

	file2 := openTempFile(t)
	defer file2.Close()

	s := NewFileSyncer(file1, 1, time.Hour, 0)
	defer s.Close()

	if err := s.Request(context.Background(), SyncRequest{Bytes: 10}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got := s.SyncCount(); got != 1 {
		t.Fatalf("SyncCount() = %d, want 1", got)
	}

	s.SetFile(file2)

	if err := s.Request(context.Background(), SyncRequest{Bytes: 10}); err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if err := s.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	if got := s.SyncCount(); got != 2 {
		t.Fatalf("SyncCount() = %d, want 2", got)
	}
}

func openTempFile(t *testing.T) *os.File {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "syncer-*.wal")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	return file
}

func waitForInternalSyncCount(t *testing.T, s *FileSyncer, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.SyncCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("SyncCount() did not reach %d", want)
}
