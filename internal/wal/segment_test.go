package wal

import (
	"os"
	"testing"
)

func TestOpenSegmentsEmptyDir(t *testing.T) {
	dir := t.TempDir()

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	if got := mgr.Active().ID; got != InitialSegmentID {
		t.Fatalf("Active().ID = %d, want %d", got, InitialSegmentID)
	}
	if got := mgr.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}

	snap := mgr.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("Snapshot() len = %d, want 1", len(snap))
	}
	if snap[0].ID != InitialSegmentID {
		t.Fatalf("Snapshot()[0].ID = %d, want %d", snap[0].ID, InitialSegmentID)
	}
}

func TestOpenSegmentsExistingSingleSegment(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, InitialSegmentID)

	data := []byte("hello")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	if got := mgr.Active().ID; got != InitialSegmentID {
		t.Fatalf("Active().ID = %d, want %d", got, InitialSegmentID)
	}
	if got := mgr.Size(); got != uint64(len(data)) {
		t.Fatalf("Size() = %d, want %d", got, len(data))
	}
}

func TestOpenSegmentsExistingMultipleSegments(t *testing.T) {
	dir := t.TempDir()

	for i := uint64(1); i <= 3; i++ {
		if err := os.WriteFile(SegmentPath(dir, i), []byte{byte(i)}, 0o644); err != nil {
			t.Fatalf("WriteFile(segment %d) error = %v", i, err)
		}
	}

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	if got := mgr.Active().ID; got != 3 {
		t.Fatalf("Active().ID = %d, want 3", got)
	}

	snap := mgr.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("Snapshot() len = %d, want 3", len(snap))
	}
	for i, ref := range snap {
		want := uint64(i + 1)
		if ref.ID != want {
			t.Fatalf("Snapshot()[%d].ID = %d, want %d", i, ref.ID, want)
		}
	}
}

func TestOpenSegmentsRejectsNonContiguousSequence(t *testing.T) {
	dir := t.TempDir()

	if err := os.WriteFile(SegmentPath(dir, 1), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile(1) error = %v", err)
	}
	if err := os.WriteFile(SegmentPath(dir, 3), []byte("c"), 0o644); err != nil {
		t.Fatalf("WriteFile(3) error = %v", err)
	}

	_, err := OpenSegments(dir)
	if err == nil {
		t.Fatal("OpenSegments() error = nil, want error")
	}
}

func TestFileSegmentManagerRollover(t *testing.T) {
	dir := t.TempDir()

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	if _, err := mgr.Rollover(); err != nil {
		t.Fatalf("Rollover() error = %v", err)
	}

	if got := mgr.Active().ID; got != 2 {
		t.Fatalf("Active().ID = %d, want 2", got)
	}
	if got := mgr.Size(); got != 0 {
		t.Fatalf("Size() = %d, want 0", got)
	}

	snap := mgr.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("Snapshot() len = %d, want 2", len(snap))
	}
	if snap[1].ID != 2 {
		t.Fatalf("Snapshot()[1].ID = %d, want 2", snap[1].ID)
	}
}

func TestFileSegmentManagerAdvance(t *testing.T) {
	dir := t.TempDir()

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	mgr.Advance(10)
	if got := mgr.Size(); got != 10 {
		t.Fatalf("Size() = %d, want 10", got)
	}

	mgr.Advance(5)
	if got := mgr.Size(); got != 15 {
		t.Fatalf("Size() = %d, want 15", got)
	}
}

func TestFileSegmentManagerSnapshotReturnsCopy(t *testing.T) {
	dir := t.TempDir()

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}
	defer mgr.Close()

	snap := mgr.Snapshot()
	snap[0].ID = 999

	again := mgr.Snapshot()
	if again[0].ID != InitialSegmentID {
		t.Fatalf("Snapshot() leaked internal state, got %d want %d", again[0].ID, InitialSegmentID)
	}
}

func TestFileSegmentManagerCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()

	mgr, err := OpenSegments(dir)
	if err != nil {
		t.Fatalf("OpenSegments() error = %v", err)
	}

	if err := mgr.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := mgr.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}
