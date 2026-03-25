# go-wal

A production-oriented write-ahead log (WAL) package in Go.

## Scope

This project implements a single-node WAL infrastructure for reliable storage systems.

## Features (planned)

- Append-only logical records
- Thread-safe multi-goroutine append
- Segmented log files
- Replay from beginning and from position
- Crash recovery
- Internal chunking for large records
