package wal

import (
	"bytes"
	"context"
)

// AppendRequest describes one logical append request.
type AppendRequest struct {
	Data      []byte
	ChunkSize uint32
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
	frames, err := buildFrames(w.codec, req.Data, req.ChunkSize)
	if err != nil {
		return AppendResult{}, err
	}

	var totalBytes uint64
	for _, frame := range frames {
		if _, err := w.segments.WriterFile().Write(frame); err != nil {
			return AppendResult{}, err
		}
		totalBytes += uint64(len(frame))
	}
	w.segments.Advance(totalBytes)

	if err := w.syncer.Request(ctx, SyncRequest{Bytes: totalBytes}); err != nil {
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

func buildFrames(codec Codec, data []byte, chunkSize uint32) ([][]byte, error) {
	if len(data) == 0 || uint32(len(data)) <= chunkSize {
		frame, err := encodeFrame(codec, FrameTypeFull, data)
		if err != nil {
			return nil, err
		}
		return [][]byte{frame}, nil
	}

	frames := make([][]byte, 0, chunkCount(len(data), chunkSize))
	for start := 0; start < len(data); start += int(chunkSize) {
		end := start + int(chunkSize)
		if end > len(data) {
			end = len(data)
		}

		frameType := FrameTypeMiddle
		switch {
		case start == 0:
			frameType = FrameTypeFirst
		case end == len(data):
			frameType = FrameTypeLast
		}

		frame, err := encodeFrame(codec, frameType, data[start:end])
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}

	return frames, nil
}

func encodeFrame(codec Codec, frameType FrameType, payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := codec.Encode(&buf, FrameHeader{Type: frameType}, payload); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func chunkCount(length int, chunkSize uint32) int {
	if length == 0 {
		return 1
	}
	if uint32(length) <= chunkSize {
		return 1
	}
	size := int(chunkSize)
	return (length + size - 1) / size
}

// BuildTestFrames constructs the physical frames for one logical record.
// It is intended for package tests that need crash-style file mutation.
func BuildTestFrames(data []byte, chunkSize uint32) ([][]byte, error) {
	return buildFrames(BinaryCodec{}, data, chunkSize)
}
