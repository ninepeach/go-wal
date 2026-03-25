package wal

import (
	"context"
	"errors"
	"io"
	"testing"
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
