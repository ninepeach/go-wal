package wal

import (
	"context"
	"errors"
	"io"
	"os"
)

// ErrReplayPositionNotFound indicates that a replay start position is not a record boundary.
var ErrReplayPositionNotFound = errors.New("wal: replay position not found")

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

// ReplaySegment defines one visible segment in a replay snapshot.
type ReplaySegment struct {
	ID   uint64
	Path string
	Size uint64
}

// Reader scans WAL segments and reassembles logical records.
type Reader interface {
	Next(ctx context.Context) (ReplayRecord, error)
	Close() error
}

// FileReader replays full logical records from a snapshot of ordered segments.
type FileReader struct {
	codec   Codec
	current int
	offset  uint64
	files   []*os.File
	refs    []ReplaySegment
}

// NewFileReader creates a snapshot reader from ordered segment snapshots.
func NewFileReader(refs []ReplaySegment, from ReplayRequest, codec Codec) (*FileReader, error) {
	files := make([]*os.File, 0, len(refs))
	for _, ref := range refs {
		file, err := os.Open(ref.Path)
		if err != nil {
			closeFiles(files)
			return nil, err
		}
		files = append(files, file)
	}

	index, ok, err := locateReplayStart(files, refs, from, codec)
	if err != nil {
		closeFiles(files)
		return nil, err
	}
	if !ok {
		closeFiles(files)
		return nil, ErrReplayPositionNotFound
	}

	return &FileReader{
		codec:   codec,
		current: index,
		offset:  from.Offset,
		files:   files,
		refs:    refs,
	}, nil
}

func locateReplayStart(files []*os.File, refs []ReplaySegment, from ReplayRequest, codec Codec) (int, bool, error) {
	if from.Segment == 0 && from.Offset == 0 {
		return 0, true, nil
	}

	for i, ref := range refs {
		if ref.ID != from.Segment {
			continue
		}
		ok, err := isLogicalBoundary(files[i], ref, from.Offset, codec)
		if err != nil {
			return 0, false, err
		}
		return i, ok, nil
	}

	return 0, false, nil
}

func isLogicalBoundary(file *os.File, ref ReplaySegment, want uint64, codec Codec) (bool, error) {
	if want >= ref.Size {
		return false, nil
	}

	var offset uint64
	for offset < ref.Size {
		if offset == want {
			return true, nil
		}

		next, _, err := scanLogicalRecord(file, ref.Size, offset, codec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return false, io.ErrUnexpectedEOF
			}
			return false, err
		}
		offset = next
	}

	return false, nil
}

// Next returns the next full logical record from the snapshot.
func (r *FileReader) Next(ctx context.Context) (ReplayRecord, error) {
	select {
	case <-ctx.Done():
		return ReplayRecord{}, ctx.Err()
	default:
	}

	for r.current < len(r.refs) {
		ref := r.refs[r.current]
		if r.offset >= ref.Size {
			r.current++
			r.offset = 0
			continue
		}

		start := r.offset
		next, payload, err := scanLogicalRecord(r.files[r.current], ref.Size, r.offset, r.codec)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return ReplayRecord{}, io.ErrUnexpectedEOF
			}
			return ReplayRecord{}, err
		}

		r.offset = next
		return ReplayRecord{
			Segment: ref.ID,
			Offset:  start,
			Data:    append([]byte(nil), payload...),
		}, nil
	}

	return ReplayRecord{}, io.EOF
}

// Close closes the snapshot file handles.
func (r *FileReader) Close() error {
	closeFiles(r.files)
	r.files = nil
	return nil
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		if file != nil {
			_ = file.Close()
		}
	}
}

func scanLogicalRecord(file *os.File, size, offset uint64, codec Codec) (uint64, []byte, error) {
	section := io.NewSectionReader(file, int64(offset), int64(size-offset))
	header, payload, err := codec.Decode(section)
	if err != nil {
		return 0, nil, err
	}

	next := offset + uint64(HeaderSize) + uint64(header.Length)
	switch header.Type {
	case FrameTypeFull:
		return next, payload, nil
	case FrameTypeFirst:
		data := make([]byte, 0, len(payload))
		data = append(data, payload...)
		for next < size {
			section = io.NewSectionReader(file, int64(next), int64(size-next))
			partHeader, partPayload, err := codec.Decode(section)
			if err != nil {
				return 0, nil, err
			}

			data = append(data, partPayload...)
			next += uint64(HeaderSize) + uint64(partHeader.Length)

			switch partHeader.Type {
			case FrameTypeMiddle:
				continue
			case FrameTypeLast:
				return next, data, nil
			default:
				return 0, nil, errors.New("wal: invalid chunk sequence")
			}
		}
		return 0, nil, io.ErrUnexpectedEOF
	default:
		return 0, nil, errors.New("wal: invalid chunk sequence")
	}
}
