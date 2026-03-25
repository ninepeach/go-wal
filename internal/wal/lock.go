package wal

import "sync"

// AppendGate serializes append-side state transitions.
type AppendGate struct {
	mu     sync.Mutex
	closed bool
}

// Close marks the gate as closed for future appends.
func (g *AppendGate) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.closed = true
}

// Closed reports whether the gate has been closed.
func (g *AppendGate) Closed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.closed
}
