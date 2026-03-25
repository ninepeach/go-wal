# AGENTS.md

## Project
This repository implements a WAL package in Go.

## Scope
This package is infrastructure only. It must not contain any chat, session, telegram, or business semantics.

## Responsibilities
- Append-only logical records
- Thread-safe multi-goroutine append
- Segmented log files
- Replay from beginning
- Replay from position
- Configurable sync policy
- Startup recovery
- Safe handling of incomplete tail writes
- Internal chunking and reassembly for large logical records

## Non-goals
- Snapshot
- Compaction
- Query engine
- Redis protocol
- Network service
- Replication
- Compression
- Encryption

## Code rules
- Go only
- English comments
- Small and focused interfaces
- Explicit structs and clear package boundaries
- Prefer correctness and clarity over optimization
- Add tests for append/replay, recovery, rollover, and chunking
