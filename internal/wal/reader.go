package wal

import (
	"context"
	"errors"
	"io"
	"os"
)

// ReplayRequest defines the starting point for internal replay.
type ReplayRequest struct {
	Segment uint64
	Offset  uint64
}

// ReplayRecord is one internally decoded logical record.
type ReplayRecord struct {
	Segment uint64
	Offset  uint64
	Data    []byte
}

// Reader scans WAL segments and reassembles logical records.
type Reader interface {
	Next(ctx context.Context) (ReplayRecord, error)
	Close() error
}

// FileReader replays full logical records from a snapshot of one segment.
type FileReader struct {
	file      *os.File
	codec     Codec
	segmentID uint64
	limit     uint64
	offset    uint64
}

// NewFileReader creates a snapshot reader for one segment.
func NewFileReader(file *os.File, segmentID uint64, limit uint64, codec Codec) *FileReader {
	return &FileReader{
		file:      file,
		codec:     codec,
		segmentID: segmentID,
		limit:     limit,
	}
}

// Next returns the next full logical record from the snapshot.
func (r *FileReader) Next(ctx context.Context) (ReplayRecord, error) {
	select {
	case <-ctx.Done():
		return ReplayRecord{}, ctx.Err()
	default:
	}

	if r.offset >= r.limit {
		return ReplayRecord{}, io.EOF
	}

	start := r.offset
	section := io.NewSectionReader(r.file, int64(r.offset), int64(r.limit-r.offset))
	header, payload, err := r.codec.Decode(section)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return ReplayRecord{}, io.ErrUnexpectedEOF
		}
		return ReplayRecord{}, err
	}

	r.offset += uint64(HeaderSize) + uint64(header.Length)
	return ReplayRecord{
		Segment: r.segmentID,
		Offset:  start,
		Data:    append([]byte(nil), payload...),
	}, nil
}

// Close closes the snapshot file handle.
func (r *FileReader) Close() error {
	if r.file == nil {
		return nil
	}
	err := r.file.Close()
	r.file = nil
	return err
}
