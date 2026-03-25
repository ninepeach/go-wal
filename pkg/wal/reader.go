package wal

import (
	"context"
	"errors"
	"io"
	"sync"

	internalwal "github.com/ninepeach/go-wal/internal/wal"
)

// Reader replays logical records from a starting WAL position.
//
// A Reader is safe for concurrent calls to Next and Close.
type Reader interface {
	Next(ctx context.Context) (Record, error)
	Close() error
}

type reader struct {
	mu     sync.Mutex
	inner  internalwal.Reader
	closed bool
}

func (r *reader) Next(ctx context.Context) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return Record{}, ErrClosed
	}
	if r.inner == nil {
		return Record{}, ErrClosed
	}

	rec, err := r.inner.Next(ctx)
	if err != nil {
		switch {
		case errors.Is(err, io.ErrUnexpectedEOF):
			return Record{}, ErrCorruption
		case errors.Is(err, internalwal.ErrCorruption):
			return Record{}, ErrCorruption
		default:
			return Record{}, err
		}
	}

	return Record{
		Position: Position{
			Segment: rec.Segment,
			Offset:  rec.Offset,
		},
		Data: rec.Data,
	}, nil
}

func (r *reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true

	if r.inner == nil {
		return nil
	}
	return r.inner.Close()
}