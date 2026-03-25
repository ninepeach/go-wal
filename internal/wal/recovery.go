package wal

import (
	"encoding/binary"
	"errors"
	"os"
	"io"
)

// ErrCorruption indicates unrecoverable non-tail WAL corruption.
var ErrCorruption = errors.New("wal: corruption detected")

var errTailCorruption = errors.New("wal: tail corruption")

// RecoveryPlan describes the work required to make the WAL readable.
type RecoveryPlan struct {
	TruncateSegment uint64
	TruncateOffset  uint64
}

// RecoveryResult describes the WAL state after startup recovery.
type RecoveryResult struct {
	LastSegment uint64
	LastOffset  uint64
}

// RecoveryManager validates and repairs the WAL tail on open.
type RecoveryManager interface {
	Plan() (RecoveryPlan, error)
	Recover() (RecoveryResult, error)
}

// Recover scans existing segments and truncates an incomplete or corrupted tail record.
func Recover(dir string, _ Codec) (RecoveryResult, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return RecoveryResult{}, err
	}

	refs, err := discoverSegments(dir)
	if err != nil {
		return RecoveryResult{}, err
	}
	if len(refs) == 0 {
		return RecoveryResult{}, nil
	}

	lastDataIndex, sizes, err := lastNonEmptySegment(refs)
	if err != nil {
		return RecoveryResult{}, err
	}
	if lastDataIndex < 0 {
		return RecoveryResult{}, nil
	}

	result := RecoveryResult{
		LastSegment: refs[lastDataIndex].ID,
		LastOffset:  sizes[lastDataIndex],
	}

	for i, ref := range refs {
		size := sizes[i]
		if size == 0 {
			continue
		}

		validSize, scanErr := scanSegment(ref.Path, size)
		if scanErr == nil {
			if i == lastDataIndex {
				result.LastOffset = validSize
			}
			continue
		}
		if errors.Is(scanErr, ErrCorruption) {
			return RecoveryResult{}, ErrCorruption
		}
		if i != lastDataIndex {
			return RecoveryResult{}, ErrCorruption
		}

		if err := truncateSegment(ref.Path, validSize); err != nil {
			return RecoveryResult{}, err
		}
		result.LastOffset = validSize
	}

	return result, nil
}

func lastNonEmptySegment(refs []SegmentRef) (int, []uint64, error) {
	sizes := make([]uint64, len(refs))
	last := -1
	for i, ref := range refs {
		info, err := os.Stat(ref.Path)
		if err != nil {
			return -1, nil, err
		}
		sizes[i] = uint64(info.Size())
		if sizes[i] > 0 {
			last = i
		}
	}
	return last, sizes, nil
}

func scanSegment(path string, size uint64) (uint64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var offset uint64
	var recordStart uint64
	inChunks := false
	for offset < size {
		header, err := readFrameHeader(file, offset, size)
		if err != nil {
			switch {
			case errors.Is(err, io.ErrUnexpectedEOF):
				if inChunks {
					return recordStart, errTailCorruption
				}
				return offset, errTailCorruption
			case errors.Is(err, ErrCorruption):
				return offset, ErrCorruption
			default:
				return 0, err
			}
		}

		frameEnd := offset + uint64(HeaderSize) + uint64(header.Length)
		if frameEnd > size {
			if inChunks {
				return recordStart, errTailCorruption
			}
			return offset, errTailCorruption
		}
		
		payload := make([]byte, header.Length)
		if _, err := file.ReadAt(payload, int64(offset+uint64(HeaderSize))); err != nil {
			if inChunks {
				return recordStart, errTailCorruption
			}
			return offset, errTailCorruption
		}
		
		if checksumFrame(header.Type, payload) != header.Checksum {
			if frameEnd < size {
				if inChunks {
					return recordStart, ErrCorruption
				}
				return offset, ErrCorruption
			}
			if inChunks {
				return recordStart, errTailCorruption
			}
			return offset, errTailCorruption
		}

		switch header.Type {
		case FrameTypeFull:
			if inChunks {
				return recordStart, ErrCorruption
			}
		case FrameTypeFirst:
			if inChunks {
				return recordStart, ErrCorruption
			}
			recordStart = offset
			inChunks = true
		case FrameTypeMiddle:
			if !inChunks {
				return offset, ErrCorruption
			}
		case FrameTypeLast:
			if !inChunks {
				return offset, ErrCorruption
			}
			inChunks = false
		default:
			if inChunks {
				return recordStart, errTailCorruption
			}
			return offset, errTailCorruption
		}

		offset = frameEnd
	}

	if inChunks {
		return recordStart, errTailCorruption
	}
	return offset, nil
}

func truncateSegment(path string, size uint64) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	if err := file.Truncate(int64(size)); err != nil {
		return err
	}
	return file.Sync()
}

func readFrameHeader(file *os.File, offset uint64, size uint64) (FrameHeader, error) {
	if size-offset < uint64(HeaderSize) {
		return FrameHeader{}, io.ErrUnexpectedEOF
	}

	var buf [HeaderSize]byte
	if _, err := file.ReadAt(buf[:], int64(offset)); err != nil {
		return FrameHeader{}, io.ErrUnexpectedEOF
	}

	header := FrameHeader{
		Magic:    binary.LittleEndian.Uint32(buf[0:4]),
		Version:  binary.LittleEndian.Uint16(buf[4:6]),
		Type:     FrameType(buf[6]),
		Length:   binary.LittleEndian.Uint32(buf[8:12]),
		Checksum: binary.LittleEndian.Uint32(buf[12:16]),
	}

	switch {
	case header.Magic != Magic:
		return FrameHeader{}, ErrCorruption
	case header.Version != FormatVersion:
		return FrameHeader{}, ErrCorruption
	case !header.Type.Valid():
		return FrameHeader{}, ErrCorruption
	default:
		return header, nil
	}
}
