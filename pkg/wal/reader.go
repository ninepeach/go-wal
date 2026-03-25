package wal

import (
	"context"
	"errors"
	"io"

	internalwal "github.com/ninepeach/go-wal/internal/wal"
)

// Reader replays logical records from a starting WAL position.
type Reader interface {
	Next(ctx context.Context) (Record, error)
	Close() error
}

type reader struct {
	inner  internalwal.Reader
	closed bool
}

func (r *reader) Next(ctx context.Context) (Record, error) {
	if r.closed {
		return Record{}, ErrClosed
	}

	rec, err := r.inner.Next(ctx)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, ErrCorruption
		}
		return Record{}, err
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
	if r.closed {
		return nil
	}
	r.closed = true
	if r.inner == nil {
		return nil
	}
	return r.inner.Close()
}
