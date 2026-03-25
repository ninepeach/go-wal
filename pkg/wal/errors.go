package wal

import "errors"

var (
	// ErrInvalidConfig indicates that Config is structurally invalid.
	ErrInvalidConfig = errors.New("wal: invalid config")
	// ErrClosed indicates that the WAL has been closed.
	ErrClosed = errors.New("wal: closed")
	// ErrInvalidPosition indicates that a replay position is not valid.
	ErrInvalidPosition = errors.New("wal: invalid position")
	// ErrCorruption indicates that on-disk WAL data is corrupted.
	ErrCorruption = errors.New("wal: corruption detected")
	// ErrRecordTooLarge indicates that a logical record exceeds MaxRecordSize.
	ErrRecordTooLarge = errors.New("wal: record too large")
	// ErrNotImplemented is returned by skeleton methods that have not been implemented yet.
	ErrNotImplemented = errors.New("wal: not implemented")
)
