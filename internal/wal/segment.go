package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

// SegmentRef identifies one WAL segment file.
type SegmentRef struct {
	ID   uint64
	Path string
}

// SegmentManager owns segment discovery and lifecycle.
type SegmentManager interface {
	Active() SegmentRef
	Size() uint64
	WriterFile() *os.File
	ReaderFile() (*os.File, error)
	Close() error
}

// FileSegmentManager manages the single active segment used in phase 1.
type FileSegmentManager struct {
	ref  SegmentRef
	file *os.File
	size uint64
}

// OpenSingleSegment opens or creates the phase 1 active segment.
func OpenSingleSegment(dir string) (*FileSegmentManager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	ref := SegmentRef{
		ID:   InitialSegmentID,
		Path: SegmentPath(dir, InitialSegmentID),
	}

	file, err := os.OpenFile(ref.Path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, 2); err != nil {
		_ = file.Close()
		return nil, err
	}

	return &FileSegmentManager{
		ref:  ref,
		file: file,
		size: uint64(info.Size()),
	}, nil
}

// Active returns the active segment reference.
func (m *FileSegmentManager) Active() SegmentRef {
	return m.ref
}

// Size returns the current size of the active segment.
func (m *FileSegmentManager) Size() uint64 {
	return m.size
}

// WriterFile returns the open writable segment file.
func (m *FileSegmentManager) WriterFile() *os.File {
	return m.file
}

// ReaderFile opens a separate read handle for snapshot replay.
func (m *FileSegmentManager) ReaderFile() (*os.File, error) {
	return os.Open(m.ref.Path)
}

// Advance increments the active segment size after a successful append.
func (m *FileSegmentManager) Advance(n uint64) {
	m.size += n
}

// Close closes the active segment file.
func (m *FileSegmentManager) Close() error {
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

// SegmentPath builds a path for a segment file.
func SegmentPath(dir string, id uint64) string {
	return filepath.Join(dir, segmentFileName(id))
}

func segmentFileName(id uint64) string {
	return fmt.Sprintf("%016d%s", id, SegmentFileExt)
}
