package wal

import (
	"bytes"
	"context"
)

// AppendRequest describes one logical append request.
type AppendRequest struct {
	Data []byte
}

// AppendResult describes the result of one append request.
type AppendResult struct {
	Segment uint64
	Offset  uint64
}

// Writer serializes logical appends into physical WAL writes.
type Writer interface {
	Append(ctx context.Context, req AppendRequest) (AppendResult, error)
	Close() error
}

// FileWriter appends full logical records into the active segment.
type FileWriter struct {
	segments *FileSegmentManager
	codec    Codec
	syncer   Syncer
}

// NewFileWriter creates a phase 1 writer.
func NewFileWriter(segments *FileSegmentManager, codec Codec, syncer Syncer) *FileWriter {
	return &FileWriter{
		segments: segments,
		codec:    codec,
		syncer:   syncer,
	}
}

// Append writes one full physical frame into the active segment.
func (w *FileWriter) Append(ctx context.Context, req AppendRequest) (AppendResult, error) {
	select {
	case <-ctx.Done():
		return AppendResult{}, ctx.Err()
	default:
	}

	offset := w.segments.Size()
	header := FrameHeader{
		Type: FrameTypeFull,
	}

	var buf bytes.Buffer
	if err := w.codec.Encode(&buf, header, req.Data); err != nil {
		return AppendResult{}, err
	}

	if _, err := w.segments.WriterFile().Write(buf.Bytes()); err != nil {
		return AppendResult{}, err
	}
	w.segments.Advance(uint64(buf.Len()))

	if err := w.syncer.Request(ctx, SyncRequest{Bytes: uint64(buf.Len())}); err != nil {
		return AppendResult{}, err
	}

	return AppendResult{
		Segment: w.segments.Active().ID,
		Offset:  offset,
	}, nil
}

// Close closes any writer-owned resources.
func (w *FileWriter) Close() error {
	if w.syncer == nil {
		return nil
	}
	return w.syncer.Close()
}
