package wal

import (
	"context"
	"os"
	"sync"
	"time"
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

// FileSyncer coordinates sync policy behavior for the active segment.
type FileSyncer struct {
	mu           sync.Mutex
	file         *os.File
	policy       int
	syncInterval time.Duration
	bytesPerSync uint64
	pendingBytes uint64
	syncCount    uint64
	closed       bool
	wakeCh       chan struct{}
	stopCh       chan struct{}
	doneCh       chan struct{}
}

// NewFileSyncer creates a sync coordinator for the active segment.
func NewFileSyncer(file *os.File, policy int, syncInterval time.Duration, bytesPerSync uint64) *FileSyncer {
	s := &FileSyncer{
		file:         file,
		policy:       policy,
		syncInterval: syncInterval,
		bytesPerSync: bytesPerSync,
	}

	if policy == 1 {
		s.wakeCh = make(chan struct{}, 1)
		s.stopCh = make(chan struct{})
		s.doneCh = make(chan struct{})
		go s.run()
	}

	return s
}

// SetFile updates the active segment file used for sync.
func (s *FileSyncer) SetFile(file *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.file = file
}

// Request records sync work for one append according to the configured policy.
func (s *FileSyncer) Request(_ context.Context, req SyncRequest) error {
	switch s.policy {
	case 0:
		return s.syncCurrent()
	case 1:
		s.mu.Lock()
		defer s.mu.Unlock()

		if s.closed {
			return nil
		}
		s.pendingBytes += req.Bytes
		if s.bytesPerSync > 0 && s.pendingBytes >= s.bytesPerSync {
			s.signalLocked()
		}
		return nil
	default:
		return nil
	}
}

// Flush performs any currently pending sync work.
func (s *FileSyncer) Flush() error {
	switch s.policy {
	case 0:
		return s.syncCurrent()
	case 1:
		return s.flushPending()
	default:
		return nil
	}
}

// Close flushes pending sync work and stops background coordination.
func (s *FileSyncer) Close() error {
	switch s.policy {
	case 1:
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return nil
		}
		s.closed = true
		close(s.stopCh)
		s.mu.Unlock()
		<-s.doneCh
		return nil
	default:
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		return nil
	}
}

// PendingBytes reports unsynced bytes currently tracked by the coordinator.
func (s *FileSyncer) PendingBytes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.pendingBytes
}

// SyncCount reports the number of successful file sync operations.
func (s *FileSyncer) SyncCount() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.syncCount
}

func (s *FileSyncer) run() {
	defer close(s.doneCh)

	var ticker *time.Ticker
	if s.syncInterval > 0 {
		ticker = time.NewTicker(s.syncInterval)
		defer ticker.Stop()
	}

	for {
		var tick <-chan time.Time
		if ticker != nil {
			tick = ticker.C
		}

		select {
		case <-tick:
			_ = s.flushPending()
		case <-s.wakeCh:
			_ = s.flushPending()
		case <-s.stopCh:
			_ = s.flushPending()
			return
		}
	}
}

func (s *FileSyncer) signalLocked() {
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *FileSyncer) syncCurrent() error {
	s.mu.Lock()
	file := s.file
	s.mu.Unlock()

	if file == nil {
		return nil
	}
	if err := file.Sync(); err != nil {
		return err
	}

	s.mu.Lock()
	s.syncCount++
	s.mu.Unlock()
	return nil
}

func (s *FileSyncer) flushPending() error {
	s.mu.Lock()
	file := s.file
	pending := s.pendingBytes
	if file == nil || pending == 0 {
		s.mu.Unlock()
		return nil
	}
	s.pendingBytes = 0
	s.mu.Unlock()

	if err := file.Sync(); err != nil {
		s.mu.Lock()
		s.pendingBytes += pending
		s.mu.Unlock()
		return err
	}

	s.mu.Lock()
	s.syncCount++
	s.mu.Unlock()
	return nil
}
