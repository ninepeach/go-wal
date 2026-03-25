package wal

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	internalwal "github.com/ninepeach/go-wal/internal/wal"
)

func TestConfigValidate(t *testing.T) {
	cfg := Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    512,
		SyncPolicy:       SyncAlways,
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestConfigValidateInvalid(t *testing.T) {
	cfg := Config{}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Validate() error = %v, want ErrInvalidConfig", err)
	}
}

func TestAppendOneRecordAndReplay(t *testing.T) {
	log := openTestWAL(t)

	pos, err := log.Append(context.Background(), []byte("alpha"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if pos != (Position{Segment: 1, Offset: 0}) {
		t.Fatalf("Append() position = %+v, want {Segment:1 Offset:0}", pos)
	}

	r, err := log.NewReader(ZeroPosition())
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	record, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if record.Position != pos {
		t.Fatalf("Next() position = %+v, want %+v", record.Position, pos)
	}
	if string(record.Data) != "alpha" {
		t.Fatalf("Next() data = %q, want %q", string(record.Data), "alpha")
	}

	_, err = r.Next(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next() final error = %v, want io.EOF", err)
	}
}

func TestAppendMultipleRecordsAndReplayInOrder(t *testing.T) {
	log := openTestWAL(t)

	want := []string{"alpha", "beta", "gamma"}
	var positions []Position
	for _, item := range want {
		pos, err := log.Append(context.Background(), []byte(item))
		if err != nil {
			t.Fatalf("Append(%q) error = %v", item, err)
		}
		positions = append(positions, pos)
	}

	if positions[0] != (Position{Segment: 1, Offset: 0}) {
		t.Fatalf("positions[0] = %+v, want start of segment", positions[0])
	}
	if positions[0].Offset >= positions[1].Offset || positions[1].Offset >= positions[2].Offset {
		t.Fatalf("positions not strictly increasing: %+v", positions)
	}

	r, err := log.NewReader(ZeroPosition())
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	for i, item := range want {
		record, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() #%d error = %v", i, err)
		}
		if record.Position != positions[i] {
			t.Fatalf("Next() #%d position = %+v, want %+v", i, record.Position, positions[i])
		}
		if string(record.Data) != item {
			t.Fatalf("Next() #%d data = %q, want %q", i, string(record.Data), item)
		}
	}

	_, err = r.Next(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next() final error = %v, want io.EOF", err)
	}
}

func TestAppendEmptyPayload(t *testing.T) {
	log := openTestWAL(t)

	pos, err := log.Append(context.Background(), nil)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	r, err := log.NewReader(ZeroPosition())
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	record, err := r.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if record.Position != pos {
		t.Fatalf("Next() position = %+v, want %+v", record.Position, pos)
	}
	if len(record.Data) != 0 {
		t.Fatalf("Next() data length = %d, want 0", len(record.Data))
	}
}

func TestRolloverAndReplayAcrossSegments(t *testing.T) {
	log := openSizedTestWAL(t, 21, 5)

	want := []string{"aaaaa", "bbbbb", "ccccc"}
	var positions []Position
	for _, item := range want {
		pos, err := log.Append(context.Background(), []byte(item))
		if err != nil {
			t.Fatalf("Append(%q) error = %v", item, err)
		}
		positions = append(positions, pos)
	}

	if positions[0] != (Position{Segment: 1, Offset: 0}) {
		t.Fatalf("positions[0] = %+v, want first segment start", positions[0])
	}
	if positions[1] != (Position{Segment: 2, Offset: 0}) {
		t.Fatalf("positions[1] = %+v, want second segment start", positions[1])
	}
	if positions[2] != (Position{Segment: 3, Offset: 0}) {
		t.Fatalf("positions[2] = %+v, want third segment start", positions[2])
	}

	r, err := log.NewReader(ZeroPosition())
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	for i, item := range want {
		record, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() #%d error = %v", i, err)
		}
		if record.Position != positions[i] {
			t.Fatalf("Next() #%d position = %+v, want %+v", i, record.Position, positions[i])
		}
		if string(record.Data) != item {
			t.Fatalf("Next() #%d data = %q, want %q", i, string(record.Data), item)
		}
	}
}

func TestReplayFromPositionAcrossSegments(t *testing.T) {
	log := openSizedTestWAL(t, 21, 5)

	want := []string{"aaaaa", "bbbbb", "ccccc"}
	var positions []Position
	for _, item := range want {
		pos, err := log.Append(context.Background(), []byte(item))
		if err != nil {
			t.Fatalf("Append(%q) error = %v", item, err)
		}
		positions = append(positions, pos)
	}

	r, err := log.NewReader(positions[1])
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	for i, item := range want[1:] {
		record, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() #%d error = %v", i, err)
		}
		if record.Position != positions[i+1] {
			t.Fatalf("Next() #%d position = %+v, want %+v", i, record.Position, positions[i+1])
		}
		if string(record.Data) != item {
			t.Fatalf("Next() #%d data = %q, want %q", i, string(record.Data), item)
		}
	}

	_, err = r.Next(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next() final error = %v, want io.EOF", err)
	}
}

func TestNewReaderRejectsInvalidPositions(t *testing.T) {
	log := openSizedTestWAL(t, 64, 32)

	first, err := log.Append(context.Background(), []byte("alpha"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	second, err := log.Append(context.Background(), []byte("beta"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	tests := []struct {
		name string
		pos  Position
	}{
		{
			name: "inside record payload",
			pos: Position{
				Segment: first.Segment,
				Offset:  first.Offset + 1,
			},
		},
		{
			name: "unknown segment",
			pos: Position{
				Segment: second.Segment + 10,
				Offset:  0,
			},
		},
		{
			name: "segment end",
			pos: Position{
				Segment: second.Segment,
				Offset:  second.Offset + 16 + uint64(len("beta")),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := log.NewReader(tc.pos)
			if !errors.Is(err, ErrInvalidPosition) {
				t.Fatalf("NewReader(%+v) error = %v, want ErrInvalidPosition", tc.pos, err)
			}
		})
	}
}

func TestSyncAlwaysSyncsOnEveryAppend(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncAlways,
	})

	for i := 0; i < 3; i++ {
		if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
			t.Fatalf("Append() #%d error = %v", i, err)
		}
	}

	if got := log.syncer.SyncCount(); got != 3 {
		t.Fatalf("SyncCount() = %d, want 3", got)
	}
	if got := log.syncer.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0", got)
	}
}

func TestSyncBatchFlushesOnThreshold(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncBatch,
		SyncInterval:     time.Hour,
		BytesPerSync:     21,
	})

	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	waitForSyncCount(t, log.syncer, 1)
	if got := log.syncer.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() = %d, want 0 after threshold flush", got)
	}
}

func TestSyncNoneDoesNotForceSync(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncNone,
	})

	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := log.syncer.SyncCount(); got != 0 {
		t.Fatalf("SyncCount() = %d, want 0", got)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := log.syncer.SyncCount(); got != 0 {
		t.Fatalf("SyncCount() after Close = %d, want 0", got)
	}
}

func TestCloseFlushesPendingBatchWrites(t *testing.T) {
	log := openConcreteTestWALNoCleanup(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncBatch,
		SyncInterval:     time.Hour,
		BytesPerSync:     0,
	})

	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := log.syncer.PendingBytes(); got == 0 {
		t.Fatalf("PendingBytes() = %d, want pending bytes before Close", got)
	}

	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if got := log.syncer.SyncCount(); got != 1 {
		t.Fatalf("SyncCount() after Close = %d, want 1", got)
	}
	if got := log.syncer.PendingBytes(); got != 0 {
		t.Fatalf("PendingBytes() after Close = %d, want 0", got)
	}
	if _, err := log.Append(context.Background(), []byte("beta")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append() after Close error = %v, want ErrClosed", err)
	}
}

func TestRecoveryTruncatesPartialTailAndAllowsAppend(t *testing.T) {
	cfg := Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncNone,
	}

	log := openConcreteTestWALNoCleanup(t, cfg)
	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("beta")); err != nil {
		t.Fatalf("Append(beta) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	appendPartialTail(t, cfg.Dir, 1, []byte("gamma"), 7)

	reopened := openConcreteTestWALNoCleanup(t, cfg)
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	if pos, err := reopened.Append(context.Background(), []byte("gamma")); err != nil {
		t.Fatalf("Append(gamma) after recovery error = %v", err)
	} else if pos.Segment != 1 {
		t.Fatalf("Append(gamma) segment = %d, want 1", pos.Segment)
	}

	assertReplayData(t, reopened, ZeroPosition(), []string{"alpha", "beta", "gamma"})
}

func TestRecoveryTruncatesCorruptedTailRecord(t *testing.T) {
	cfg := Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncNone,
	}

	log := openConcreteTestWALNoCleanup(t, cfg)
	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("beta")); err != nil {
		t.Fatalf("Append(beta) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	appendCorruptedTail(t, cfg.Dir, 1, []byte("gamma"))

	reopened, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() after corrupted tail error = %v", err)
	}
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	assertReplayData(t, reopened, ZeroPosition(), []string{"alpha", "beta"})
}

func TestRecoveryFailsOnMidLogCorruption(t *testing.T) {
	cfg := Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   64,
		MaxRecordSize:    512,
		SyncPolicy:       SyncNone,
	}

	log := openConcreteTestWALNoCleanup(t, cfg)
	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if _, err := log.Append(context.Background(), []byte("beta")); err != nil {
		t.Fatalf("Append(beta) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	corruptMidLogByte(t, cfg.Dir, 1, internalwal.HeaderSize)

	_, err := Open(cfg)
	if !errors.Is(err, ErrCorruption) {
		t.Fatalf("Open() error = %v, want ErrCorruption", err)
	}
}

func TestAppendChunkedRecordAndReplay(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 256,
		ChunkSizeBytes:   4,
		MaxRecordSize:    32,
		SyncPolicy:       SyncNone,
	})

	payload := []byte("abcdefghij")
	pos, err := log.Append(context.Background(), payload)
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if pos != (Position{Segment: 1, Offset: 0}) {
		t.Fatalf("Append() position = %+v, want segment 1 offset 0", pos)
	}

	assertReplayData(t, log, ZeroPosition(), []string{"abcdefghij"})
}

func TestReplayFromChunkedRecordPosition(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 256,
		ChunkSizeBytes:   4,
		MaxRecordSize:    32,
		SyncPolicy:       SyncNone,
	})

	first, err := log.Append(context.Background(), []byte("abcdefghij"))
	if err != nil {
		t.Fatalf("Append(first) error = %v", err)
	}
	second, err := log.Append(context.Background(), []byte("tail"))
	if err != nil {
		t.Fatalf("Append(second) error = %v", err)
	}

	assertReplayData(t, log, first, []string{"abcdefghij", "tail"})
	assertReplayData(t, log, second, []string{"tail"})
}

func TestNewReaderRejectsPositionInsideChunkedRecord(t *testing.T) {
	log := openConcreteTestWAL(t, Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 256,
		ChunkSizeBytes:   4,
		MaxRecordSize:    32,
		SyncPolicy:       SyncNone,
	})

	pos, err := log.Append(context.Background(), []byte("abcdefghij"))
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	insideSecondChunk := Position{
		Segment: pos.Segment,
		Offset:  pos.Offset + uint64(internalwal.HeaderSize) + 4,
	}

	_, err = log.NewReader(insideSecondChunk)
	if !errors.Is(err, ErrInvalidPosition) {
		t.Fatalf("NewReader(%+v) error = %v, want ErrInvalidPosition", insideSecondChunk, err)
	}
}

func TestRecoveryTruncatesIncompleteChunkSequence(t *testing.T) {
	cfg := Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 256,
		ChunkSizeBytes:   4,
		MaxRecordSize:    32,
		SyncPolicy:       SyncNone,
	}

	log := openConcreteTestWALNoCleanup(t, cfg)
	if _, err := log.Append(context.Background(), []byte("alpha")); err != nil {
		t.Fatalf("Append(alpha) error = %v", err)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	appendPartialChunkSequence(t, cfg.Dir, 1, []byte("abcdefghij"), cfg.ChunkSizeBytes, 2)

	reopened := openConcreteTestWALNoCleanup(t, cfg)
	t.Cleanup(func() {
		_ = reopened.Close()
	})

	assertReplayData(t, reopened, ZeroPosition(), []string{"alpha"})
	if _, err := reopened.Append(context.Background(), []byte("tail")); err != nil {
		t.Fatalf("Append(tail) after recovery error = %v", err)
	}
	assertReplayData(t, reopened, ZeroPosition(), []string{"alpha", "tail"})
}

func openTestWAL(t *testing.T) WAL {
	t.Helper()

	log, err := Open(Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    512,
		SyncPolicy:       SyncNone,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(func() {
		_ = log.Close()
	})

	return log
}

func openSizedTestWAL(t *testing.T, segmentSize uint64, maxRecordSize uint64) WAL {
	t.Helper()

	log, err := Open(Config{
		Dir:              t.TempDir(),
		SegmentSizeBytes: segmentSize,
		ChunkSizeBytes:   16,
		MaxRecordSize:    maxRecordSize,
		SyncPolicy:       SyncNone,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	t.Cleanup(func() {
		_ = log.Close()
	})

	return log
}

func openConcreteTestWAL(t *testing.T, cfg Config) *log {
	t.Helper()

	log := openConcreteTestWALNoCleanup(t, cfg)
	t.Cleanup(func() {
		_ = log.Close()
	})
	return log
}

func openConcreteTestWALNoCleanup(t *testing.T, cfg Config) *log {
	t.Helper()

	w, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	log, ok := w.(*log)
	if !ok {
		t.Fatalf("Open() returned %T, want *log", w)
	}
	return log
}

func waitForSyncCount(t *testing.T, syncer interface{ SyncCount() uint64 }, want uint64) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if syncer.SyncCount() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("SyncCount() did not reach %d", want)
}

func assertReplayData(t *testing.T, wal WAL, from Position, want []string) {
	t.Helper()

	r, err := wal.NewReader(from)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}
	t.Cleanup(func() {
		_ = r.Close()
	})

	for i, item := range want {
		record, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("Next() #%d error = %v", i, err)
		}
		if string(record.Data) != item {
			t.Fatalf("Next() #%d data = %q, want %q", i, string(record.Data), item)
		}
	}

	_, err = r.Next(context.Background())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Next() final error = %v, want io.EOF", err)
	}
}

func appendPartialTail(t *testing.T, dir string, segment uint64, data []byte, n int) {
	t.Helper()

	frame := encodeFrame(t, data)
	if n > len(frame) {
		t.Fatalf("partial tail length %d exceeds frame length %d", n, len(frame))
	}

	file, err := os.OpenFile(internalwal.SegmentPath(dir, segment), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	if _, err := file.Write(frame[:n]); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
}

func appendCorruptedTail(t *testing.T, dir string, segment uint64, data []byte) {
	t.Helper()

	frame := encodeFrame(t, data)
	frame[len(frame)-1] ^= 0xFF

	file, err := os.OpenFile(internalwal.SegmentPath(dir, segment), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	if _, err := file.Write(frame); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
}

func corruptMidLogByte(t *testing.T, dir string, segment uint64, offset int) {
	t.Helper()

	path := internalwal.SegmentPath(dir, segment)
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	buf := []byte{0}
	if _, err := file.ReadAt(buf, int64(offset)); err != nil {
		t.Fatalf("ReadAt() error = %v", err)
	}
	buf[0] ^= 0xFF
	if _, err := file.WriteAt(buf, int64(offset)); err != nil {
		t.Fatalf("WriteAt() error = %v", err)
	}
}

func encodeFrame(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	if err := (internalwal.BinaryCodec{}).Encode(&buf, internalwal.FrameHeader{
		Type: internalwal.FrameTypeFull,
	}, data); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	return buf.Bytes()
}

func appendPartialChunkSequence(t *testing.T, dir string, segment uint64, data []byte, chunkSize uint32, frames int) {
	t.Helper()

	built, err := internalwal.BuildTestFrames(data, chunkSize)
	if err != nil {
		t.Fatalf("BuildTestFrames() error = %v", err)
	}
	if frames > len(built) {
		t.Fatalf("frames = %d exceeds built frame count %d", frames, len(built))
	}

	file, err := os.OpenFile(internalwal.SegmentPath(dir, segment), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}
	defer file.Close()

	for i := 0; i < frames; i++ {
		if _, err := file.Write(built[i]); err != nil {
			t.Fatalf("Write() frame #%d error = %v", i, err)
		}
	}
}
