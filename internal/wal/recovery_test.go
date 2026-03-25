package wal

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestRecoverEmptyDirectory(t *testing.T) {
	dir := t.TempDir()

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if result.LastSegment != 0 || result.LastOffset != 0 {
		t.Fatalf("Recover() = %+v, want zero result", result)
	}
}

func TestRecoverCleanSegment(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	var buf bytes.Buffer
	writeTestFrame(t, &buf, FrameTypeFull, []byte("alpha"))
	writeTestFrame(t, &buf, FrameTypeFull, []byte("beta"))

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if result.LastSegment != 1 {
		t.Fatalf("LastSegment = %d, want 1", result.LastSegment)
	}
	if result.LastOffset != uint64(info.Size()) {
		t.Fatalf("LastOffset = %d, want %d", result.LastOffset, info.Size())
	}
}

func TestRecoverTruncatesPartialHeaderAtTail(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	frame1 := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame2 := buildTestFrame(t, FrameTypeFull, []byte("beta"))

	data := make([]byte, 0, len(frame1)+HeaderSize-1)
	data = append(data, frame1...)
	data = append(data, frame2[:HeaderSize-1]...)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	if result.LastOffset != uint64(len(frame1)) {
		t.Fatalf("LastOffset = %d, want %d", result.LastOffset, len(frame1))
	}
	assertFileSize(t, path, int64(len(frame1)))
}

func TestRecoverTruncatesPartialPayloadAtTail(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	frame1 := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame2 := buildTestFrame(t, FrameTypeFull, []byte("beta"))

	data := make([]byte, 0, len(frame1)+len(frame2)-1)
	data = append(data, frame1...)
	data = append(data, frame2[:len(frame2)-1]...)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	if result.LastOffset != uint64(len(frame1)) {
		t.Fatalf("LastOffset = %d, want %d", result.LastOffset, len(frame1))
	}
	assertFileSize(t, path, int64(len(frame1)))
}

func TestRecoverTruncatesCorruptedTailChecksum(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	frame1 := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame2 := buildTestFrame(t, FrameTypeFull, []byte("beta"))
	frame2[len(frame2)-1] ^= 0xFF

	data := make([]byte, 0, len(frame1)+len(frame2))
	data = append(data, frame1...)
	data = append(data, frame2...)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	if result.LastOffset != uint64(len(frame1)) {
		t.Fatalf("LastOffset = %d, want %d", result.LastOffset, len(frame1))
	}
	assertFileSize(t, path, int64(len(frame1)))
}

func TestRecoverFailsOnMidLogCorruption(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	frame1 := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame2 := buildTestFrame(t, FrameTypeFull, []byte("beta"))

	data := make([]byte, 0, len(frame1)+len(frame2))
	data = append(data, frame1...)
	data = append(data, frame2...)

	// Corrupt a byte in the middle of the first payload.
	data[HeaderSize] ^= 0xFF

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Recover(dir, BinaryCodec{})
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("Recover() error = %v, want ErrCorruption", err)
	}
}

func TestRecoverFailsOnInvalidMagicInMiddle(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	frame1 := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	frame2 := buildTestFrame(t, FrameTypeFull, []byte("beta"))

	data := make([]byte, 0, len(frame1)+len(frame2))
	data = append(data, frame1...)
	data = append(data, frame2...)

	// Corrupt the magic of the second frame.
	secondOffset := len(frame1)
	data[secondOffset+0] ^= 0xFF

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Recover(dir, BinaryCodec{})
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("Recover() error = %v, want ErrCorruption", err)
	}
}

func TestRecoverTruncatesIncompleteChunkSequenceAtTail(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	full := buildTestFrame(t, FrameTypeFull, []byte("alpha"))
	chunks, err := BuildTestFrames([]byte("abcdefghij"), 4)
	if err != nil {
		t.Fatalf("BuildTestFrames() error = %v", err)
	}

	// Keep only the first two chunk frames, making the logical record incomplete.
	data := make([]byte, 0, len(full)+len(chunks[0])+len(chunks[1]))
	data = append(data, full...)
	data = append(data, chunks[0]...)
	data = append(data, chunks[1]...)

	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	result, err := Recover(dir, BinaryCodec{})
	if err != nil {
		t.Fatalf("Recover() error = %v", err)
	}

	if result.LastOffset != uint64(len(full)) {
		t.Fatalf("LastOffset = %d, want %d", result.LastOffset, len(full))
	}
	assertFileSize(t, path, int64(len(full)))
}

func TestRecoverFailsOnInvalidChunkSequenceInMiddle(t *testing.T) {
	dir := t.TempDir()
	path := SegmentPath(dir, 1)

	var buf bytes.Buffer
	writeTestFrame(t, &buf, FrameTypeFull, []byte("alpha"))
	writeTestFrame(t, &buf, FrameTypeMiddle, []byte("bbbb"))
	writeTestFrame(t, &buf, FrameTypeLast, []byte("cccc"))

	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	_, err := Recover(dir, BinaryCodec{})
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("Recover() error = %v, want ErrCorruption", err)
	}
}

func writeTestFrame(t *testing.T, buf *bytes.Buffer, frameType FrameType, payload []byte) {
	t.Helper()

	err := BinaryCodec{}.Encode(buf, FrameHeader{Type: frameType}, payload)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
}

func buildTestFrame(t *testing.T, frameType FrameType, payload []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	writeTestFrame(t, &buf, frameType, payload)
	return buf.Bytes()
}

func assertFileSize(t *testing.T, path string, want int64) {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if info.Size() != want {
		t.Fatalf("file size = %d, want %d", info.Size(), want)
	}
}
