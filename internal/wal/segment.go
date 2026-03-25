package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
	Snapshot() []SegmentRef
	Rollover() (SegmentRef, error)
	Close() error
}

// FileSegmentManager manages WAL segment lifecycle.
type FileSegmentManager struct {
	dir    string
	active SegmentRef
	file   *os.File
	size   uint64
	refs   []SegmentRef
}

// OpenSegments opens the WAL segment set and prepares the active segment.
func OpenSegments(dir string) (*FileSegmentManager, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	refs, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		refs = []SegmentRef{{
			ID:   InitialSegmentID,
			Path: SegmentPath(dir, InitialSegmentID),
		}}
	}

	active := refs[len(refs)-1]

	file, err := os.OpenFile(active.Path, os.O_RDWR|os.O_CREATE, 0o644)
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
		dir:    dir,
		active: active,
		file:   file,
		size:   uint64(info.Size()),
		refs:   refs,
	}, nil
}

// Active returns the active segment reference.
func (m *FileSegmentManager) Active() SegmentRef {
	return m.active
}

// Size returns the current size of the active segment.
func (m *FileSegmentManager) Size() uint64 {
	return m.size
}

// WriterFile returns the open writable segment file.
func (m *FileSegmentManager) WriterFile() *os.File {
	return m.file
}

// Snapshot returns the current visible ordered segment set.
func (m *FileSegmentManager) Snapshot() []SegmentRef {
	out := make([]SegmentRef, len(m.refs))
	copy(out, m.refs)
	return out
}

// Advance increments the active segment size after a successful append.
func (m *FileSegmentManager) Advance(n uint64) {
	m.size += n
}

// Rollover creates and activates the next segment.
func (m *FileSegmentManager) Rollover() (SegmentRef, error) {
	next := SegmentRef{
		ID:   m.active.ID + 1,
		Path: SegmentPath(m.dir, m.active.ID+1),
	}

	if m.file != nil {
		if err := m.file.Close(); err != nil {
			return SegmentRef{}, err
		}
	}

	file, err := os.OpenFile(next.Path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return SegmentRef{}, err
	}

	m.active = next
	m.file = file
	m.size = 0
	m.refs = append(m.refs, next)
	return next, nil
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

func discoverSegments(dir string) ([]SegmentRef, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var refs []SegmentRef
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		id, ok := parseSegmentFileName(entry.Name())
		if !ok {
			continue
		}
		refs = append(refs, SegmentRef{
			ID:   id,
			Path: filepath.Join(dir, entry.Name()),
		})
	}

	sort.Slice(refs, func(i, j int) bool {
		return refs[i].ID < refs[j].ID
	})

	for i, ref := range refs {
		want := uint64(i) + InitialSegmentID
		if ref.ID != want {
			return nil, errors.New("wal: invalid segment sequence")
		}
	}

	return refs, nil
}

func parseSegmentFileName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, SegmentFileExt) {
		return 0, false
	}
	base := strings.TrimSuffix(name, SegmentFileExt)
	id, err := strconv.ParseUint(base, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}
