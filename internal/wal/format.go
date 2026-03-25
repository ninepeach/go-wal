package wal

// FrameType identifies the role of a physical frame in the WAL.
type FrameType uint8

const (
	FrameTypeFull FrameType = iota + 1
	FrameTypeFirst
	FrameTypeMiddle
	FrameTypeLast
)

// Valid reports whether the frame type is supported by the current format.
func (t FrameType) Valid() bool {
	switch t {
	case FrameTypeFull, FrameTypeFirst, FrameTypeMiddle, FrameTypeLast:
		return true
	default:
		return false
	}
}

const (
	// SegmentFileExt is the default file extension for WAL segments.
	SegmentFileExt = ".wal"
	// FormatVersion is the current on-disk format version.
	FormatVersion uint16 = 1
	// HeaderSize is the fixed size of the frame header in bytes.
	HeaderSize = 16
	// Magic identifies frames that belong to this WAL format.
	Magic uint32 = 0x314C4157
	// InitialSegmentID is the first and only segment used in phase 1.
	InitialSegmentID uint64 = 1
)
