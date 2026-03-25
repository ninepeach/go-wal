package wal

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	internalwal "github.com/ninepeach/go-wal/internal/wal"
)

// SyncPolicy defines how the WAL translates append success into durability.
type SyncPolicy int

const (
	SyncAlways SyncPolicy = iota
	SyncBatch
	SyncNone
)

// Config defines the WAL runtime configuration.
type Config struct {
	Dir              string
	SegmentSizeBytes uint64
	ChunkSizeBytes   uint32
	MaxRecordSize    uint64
	SyncPolicy       SyncPolicy
	SyncInterval     time.Duration
	BytesPerSync     uint64
}

// Position identifies the start position of a logical record in the WAL.
type Position struct {
	Segment uint64
	Offset  uint64
}

// Record is one logical WAL record.
type Record struct {
	// Position is the start position of the logical record in the WAL.
	Position Position
	Data     []byte
}

// WAL is the public write-ahead log interface.
type WAL interface {
	Append(ctx context.Context, data []byte) (Position, error)
	NewReader(from Position) (Reader, error)
	Close() error
}

// Open validates configuration and opens the WAL.
func Open(cfg Config) (WAL, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if _, err := internalwal.Recover(cfg.Dir, internalwal.BinaryCodec{}); err != nil {
		if errors.Is(err, internalwal.ErrCorruption) {
			return nil, ErrCorruption
		}
		return nil, err
	}

	segments, err := internalwal.OpenSegments(cfg.Dir)
	if err != nil {
		return nil, err
	}

	syncer := internalwal.NewFileSyncer(
		segments.WriterFile(),
		int(cfg.SyncPolicy),
		cfg.SyncInterval,
		cfg.BytesPerSync,
	)
	writer := internalwal.NewFileWriter(segments, internalwal.BinaryCodec{}, syncer)

	return &log{
		cfg:      cfg,
		segments: segments,
		writer:   writer,
		syncer:   syncer,
	}, nil
}

// ZeroPosition returns the sentinel position for replay from the beginning.
func ZeroPosition() Position {
	return Position{}
}

// Validate performs structural configuration validation.
func (cfg Config) Validate() error {
	switch {
	case cfg.Dir == "":
		return wrapInvalidConfig("dir is required")
	case cfg.SegmentSizeBytes == 0:
		return wrapInvalidConfig("segment size must be greater than zero")
	case cfg.ChunkSizeBytes == 0:
		return wrapInvalidConfig("chunk size must be greater than zero")
	case cfg.MaxRecordSize == 0:
		return wrapInvalidConfig("max record size must be greater than zero")
	case physicalSizeForRecord(cfg.MaxRecordSize, cfg.ChunkSizeBytes) > cfg.SegmentSizeBytes:
		return wrapInvalidConfig("max record size plus frame overhead must fit within one segment")
	}

	switch cfg.SyncPolicy {
	case SyncAlways, SyncBatch, SyncNone:
	default:
		return wrapInvalidConfig("sync policy is invalid")
	}

	if cfg.SyncPolicy == SyncBatch && cfg.SyncInterval < 0 {
		return wrapInvalidConfig("sync interval must not be negative")
	}

	return nil
}

type log struct {
	mu       sync.RWMutex
	cfg      Config
	segments *internalwal.FileSegmentManager
	writer   *internalwal.FileWriter
	syncer   *internalwal.FileSyncer
	closed   bool
}

func (l *log) Append(ctx context.Context, data []byte) (Position, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return ZeroPosition(), ErrClosed
	}
	if uint64(len(data)) > l.cfg.MaxRecordSize {
		return ZeroPosition(), ErrRecordTooLarge
	}

	frameSize := physicalSizeForRecord(uint64(len(data)), l.cfg.ChunkSizeBytes)
	if frameSize > l.cfg.SegmentSizeBytes {
		return ZeroPosition(), ErrRecordTooLarge
	}
	if l.segments.Size()+frameSize > l.cfg.SegmentSizeBytes {
		if err := l.syncer.Flush(); err != nil {
			return ZeroPosition(), err
		}
		if _, err := l.segments.Rollover(); err != nil {
			return ZeroPosition(), err
		}
		l.syncer.SetFile(l.segments.WriterFile())
	}

	res, err := l.writer.Append(ctx, internalwal.AppendRequest{
		Data:      data,
		ChunkSize: l.cfg.ChunkSizeBytes,
	})
	if err != nil {
		return ZeroPosition(), err
	}

	return Position{
		Segment: res.Segment,
		Offset:  res.Offset,
	}, nil
}

func (l *log) NewReader(from Position) (Reader, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return nil, ErrClosed
	}
	refs, err := snapshotReplaySegments(l.segments.Snapshot())
	if err != nil {
		return nil, err
	}

	inner, err := internalwal.NewFileReader(refs, internalwal.ReplayRequest{
		Segment: from.Segment,
		Offset:  from.Offset,
	}, internalwal.BinaryCodec{})
	if err != nil {
		if errors.Is(err, internalwal.ErrReplayPositionNotFound) {
			return nil, ErrInvalidPosition
		}
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrInvalidPosition
		}
		return nil, err
	}

	return &reader{
		inner: inner,
	}, nil
}

func (l *log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil
	}

	l.closed = true

	var errs []error
	if l.writer != nil {
		if err := l.writer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if l.segments != nil {
		if err := l.segments.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func wrapInvalidConfig(msg string) error {
	return errors.Join(ErrInvalidConfig, errors.New(msg))
}

func snapshotReplaySegments(refs []internalwal.SegmentRef) ([]internalwal.ReplaySegment, error) {
	out := make([]internalwal.ReplaySegment, 0, len(refs))
	for _, ref := range refs {
		info, err := os.Stat(ref.Path)
		if err != nil {
			return nil, err
		}
		out = append(out, internalwal.ReplaySegment{
			ID:   ref.ID,
			Path: ref.Path,
			Size: uint64(info.Size()),
		})
	}
	return out, nil
}

func physicalSizeForRecord(payloadSize uint64, chunkSize uint32) uint64 {
	chunks := uint64(1)
	if payloadSize > 0 && payloadSize > uint64(chunkSize) {
		chunks = (payloadSize + uint64(chunkSize) - 1) / uint64(chunkSize)
	}
	return payloadSize + chunks*uint64(internalwal.HeaderSize)
}
