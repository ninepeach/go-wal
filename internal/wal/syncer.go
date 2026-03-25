package wal

import (
	"context"
	"os"
)

// SyncRequest describes one sync coordination event.
type SyncRequest struct {
	Bytes uint64
}

// Syncer coordinates durability sync work for the active segment.
type Syncer interface {
	Request(ctx context.Context, req SyncRequest) error
	Close() error
}

// FileSyncer provides the minimal phase 1 sync behavior.
type FileSyncer struct {
	file   *os.File
	policy int
}

// NewFileSyncer creates a minimal sync coordinator.
func NewFileSyncer(file *os.File, policy int) *FileSyncer {
	return &FileSyncer{
		file:   file,
		policy: policy,
	}
}

// Request performs a synchronous flush only when configured to do so.
func (s *FileSyncer) Request(ctx context.Context, _ SyncRequest) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if s.file == nil {
		return nil
	}
	if s.policy != 0 {
		return nil
	}
	return s.file.Sync()
}

// Close performs no additional work in phase 1.
func (s *FileSyncer) Close() error {
	return nil
}
