package wal

import (
	"context"
	"encoding/binary"
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

	index, ok, err := locateReplayStart(files, refs, from)
	if err != nil {
		closeFiles(files)
		return nil, err
	}
	if !ok {
		closeFiles(files)
		return nil, ErrReplayPositionNotFound
	}

	return &FileReader{
		current: index,
		offset:  from.Offset,
		files:   files,
		refs:    refs,
	}, nil
}

func locateReplayStart(files []*os.File, refs []ReplaySegment, from ReplayRequest) (int, bool, error) {
	if from.Segment == 0 && from.Offset == 0 {
		return 0, true, nil
	}

	for i, ref := range refs {
		if ref.ID != from.Segment {
			continue
		}
		ok, err := isLogicalBoundary(files[i], ref, from.Offset)
		if err != nil {
			return 0, false, err
		}
		return i, ok, nil
	}

	return 0, false, nil
}

func isLogicalBoundary(file *os.File, ref ReplaySegment, want uint64) (bool, error) {
	if want >= ref.Size {
		return false, nil
	}

	var offset uint64
	for offset < ref.Size {
		if offset == want {
			return true, nil
		}

		next, _, err := scanLogicalRecord(file, ref.Size, offset)
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
		next, payload, err := scanLogicalRecord(r.files[r.current], ref.Size, r.offset)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return ReplayRecord{}, io.ErrUnexpectedEOF
			}
			if errors.Is(err, ErrCorruption) {
				return ReplayRecord{}, ErrCorruption
			}
			return ReplayRecord{}, err
		}

		r.offset = next
		return ReplayRecord{
			Segment: ref.ID,
			Offset:  start,
			Data:    payload,
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

func scanLogicalRecord(file *os.File, size, offset uint64) (uint64, []byte, error) {
	header, payload, next, err := readFrameAt(file, size, offset)
	if err != nil {
		return 0, nil, err
	}

	switch header.Type {
	case FrameTypeFull:
		return next, payload, nil

	case FrameTypeFirst:
		data := make([]byte, 0, len(payload)*2)
		data = append(data, payload...)

		for next < size {
			partHeader, partPayload, partNext, err := readFrameAt(file, size, next)
			if err != nil {
				return 0, nil, err
			}

			data = append(data, partPayload...)
			next = partNext

			switch partHeader.Type {
			case FrameTypeMiddle:
				continue
			case FrameTypeLast:
				return next, data, nil
			default:
				return 0, nil, ErrCorruption
			}
		}

		return 0, nil, io.ErrUnexpectedEOF

	default:
		return 0, nil, ErrCorruption
	}
}

func readFrameAt(file *os.File, size, offset uint64) (FrameHeader, []byte, uint64, error) {
	if size-offset < uint64(HeaderSize) {
		return FrameHeader{}, nil, 0, io.ErrUnexpectedEOF
	}

	var buf [HeaderSize]byte
	if _, err := file.ReadAt(buf[:], int64(offset)); err != nil {
		if errors.Is(err, io.EOF) {
			return FrameHeader{}, nil, 0, io.ErrUnexpectedEOF
		}
		return FrameHeader{}, nil, 0, err
	}

	header := FrameHeader{
		Magic:    binary.LittleEndian.Uint32(buf[0:4]),
		Version:  binary.LittleEndian.Uint16(buf[4:6]),
		Type:     FrameType(buf[6]),
		Length:   binary.LittleEndian.Uint32(buf[8:12]),
		Checksum: binary.LittleEndian.Uint32(buf[12:16]),
	}

	if header.Magic != Magic {
		return FrameHeader{}, nil, 0, ErrInvalidFrameMagic
	}
	if header.Version != FormatVersion {
		return FrameHeader{}, nil, 0, ErrInvalidFrameVersion
	}
	if !header.Type.Valid() {
		return FrameHeader{}, nil, 0, ErrUnsupportedFrameType
	}

	next := offset + uint64(HeaderSize) + uint64(header.Length)
	if next > size {
		return FrameHeader{}, nil, 0, io.ErrUnexpectedEOF
	}

	payload := make([]byte, header.Length)
	if _, err := file.ReadAt(payload, int64(offset+uint64(HeaderSize))); err != nil {
		if errors.Is(err, io.EOF) {
			return FrameHeader{}, nil, 0, io.ErrUnexpectedEOF
		}
		return FrameHeader{}, nil, 0, err
	}

	if checksumFrame(header.Type, payload) != header.Checksum {
		return FrameHeader{}, nil, 0, ErrInvalidFrameChecksum
	}

	return header, payload, next, nil
}
