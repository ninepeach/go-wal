# DESIGN

## Goals

This package provides a production-oriented, single-node write-ahead log (WAL) for Go applications.
It is infrastructure only. The package stores opaque byte records and does not embed business semantics.

The WAL is designed for:

- Append-only logical records
- Thread-safe append from multiple goroutines
- Segmented on-disk storage
- Replay from the beginning of the log
- Replay from an arbitrary position
- Configurable durability and sync behavior
- Safe startup recovery after crashes or partial tail writes
- Internal chunking and reassembly for large logical records

The WAL is explicitly not responsible for snapshots, compaction, replication, networking, query capabilities, compression, or encryption.

## Proposed Package Structure

The public package should remain small. Most moving parts should be internal to avoid leaking storage details into application code.

```text
pkg/wal/
  wal.go          // Public types: WAL, Config, Position, Record, SyncPolicy
  reader.go       // Public replay APIs and reader/iterator types
  errors.go       // Public sentinel errors and validation helpers

internal/wal/
  format.go       // On-disk record and chunk frame layout constants
  codec.go        // Encode/decode frame headers and trailers
  segment.go      // Segment file lifecycle, naming, open/close, rollover
  writer.go       // Serialized append path, chunk emission, position assignment
  reader.go       // Segment scanning, chunk reassembly, record iteration
  recovery.go     // Startup scan, tail truncation, recovery decisions
  syncer.go       // Sync policy execution and flush coordination
  lock.go         // Internal synchronization primitives for multi-goroutine append
```

### Package boundary

- `pkg/wal` exposes only the stable API for append, replay, and positions.
- `internal/wal` owns the file format and operational details so the format can evolve without widening the public surface.

## Responsibilities

- Persist opaque logical records as ordered append-only entries
- Assign a stable position to each logical record
- Preserve record order as committed by the WAL
- Support concurrent callers for append
- Roll over to new segment files when configured limits are reached
- Replay complete logical records from the beginning or a supplied position
- Recover safely from incomplete tail writes after process or machine failure
- Hide physical chunk boundaries from callers

## Non-goals

- No snapshotting or compaction
- No replication or multi-node coordination
- No transport protocol or network API
- No indexing or query features
- No domain-specific metadata
- No compression or encryption in the WAL layer

## Invariants

- Records are append-only and immutable once written
- A logical record has exactly one start position
- Chunk boundaries are invisible to callers
- Replay must only emit complete logical records
- Incomplete tail data may be truncated during recovery
- Mid-log corruption must not be silently skipped
- In v1, a logical record must not span segments

## Logical Record Model

A logical record is the unit visible to callers.

- Payload type: `[]byte`
- Zero-length payloads are valid logical records
- Ordering: total order within one WAL instance
- Visibility: callers append and replay whole logical records only
- Durability boundary: a record is considered durable according to the configured sync policy

Public record shape:

```go
type Record struct {
    // Position is the start position of the logical record in the WAL.
    Position Position
    Data     []byte
}
```

The WAL may store one logical record as one physical frame or many chunk frames internally, but replay must always return one full logical record.
`Record.Position` is always the position of the first physical frame belonging to that logical record.

## Position Model

The public position identifies where a logical record begins.

```go
type Position struct {
    Segment uint64
    Offset  uint64
}
```

Semantics:

- `ZeroPosition()` is a sentinel meaning "replay from the beginning of the WAL"
- `Segment` is the monotonically increasing segment number
- `Offset` is the byte offset of the first physical frame for the logical record within that segment
- Positions are stable after successful append
- Any non-zero position must refer exactly to the start position of a logical record
- Replay from a non-zero position starts at the logical record whose first frame begins at that position
- Replay from a non-boundary position must fail with `invalid position`

Why this model:

- It is simple to reason about operationally
- It maps directly to segmented storage
- It is sufficient for checkpointing and recovery in upper layers without exposing chunk details

## Segment Model

The WAL stores data in append-only segment files.

Segment properties:

- Fixed monotonic segment numbering starting at `0000000000000001`
- One active writable segment at a time
- Older segments are immutable once rotated
- Rollover occurs before an append that would exceed configured segment size

Suggested segment naming:

```text
0000000000000001.wal
0000000000000002.wal
...
```

Segment size behavior:

- Configured by `SegmentSizeBytes`
- The active segment may contain multiple records and chunk frames
- A single logical record larger than remaining space rolls over before its first chunk
- A logical record may span multiple chunks, but in v1 it must not span segments

That v1 constraint keeps recovery and replay simpler and is appropriate for the initial production version. It implies `MaxRecordSize` must be less than or equal to the usable capacity of one segment.

## Sync Policy

Durability should be configurable because workloads differ.

Suggested public type:

```go
type SyncPolicy int

const (
    SyncAlways SyncPolicy = iota
    SyncBatch
    SyncNone
)
```

Behavior:

- `SyncAlways`: each successful append performs `fdatasync`/`fsync` before returning
- `SyncBatch`: `Append()` returns after the record has been written to the active segment file, but before a durability sync boundary; the WAL performs sync on batch boundaries or time-based flush intervals
- `SyncNone`: relies on the OS page cache; useful only where weaker durability is acceptable
- `Close()`: flushes any pending buffered writes, performs the final required sync work for the configured policy, and stops background sync coordination cleanly before returning

Suggested config knobs:

- `SyncPolicy`
- `SyncInterval` for batch mode
- `BytesPerSync` optional threshold for batch mode

The append API should document exactly what "success" means for each policy:

- `SyncAlways`: written and synced before `Append()` returns
- `SyncBatch`: written to the active segment file before `Append()` returns, but not necessarily synced yet
- `SyncNone`: written to the active segment file before `Append()` returns, with no forced durability sync

## Recovery Model

Recovery happens on open before the WAL accepts new writes.

Recovery steps:

1. Discover all segment files and sort by segment number.
2. Validate naming and monotonic continuity.
3. Scan each segment from the beginning using per-frame checksums and lengths.
4. Stop at the first invalid or incomplete tail frame in the last affected segment.
5. Truncate only the invalid tail bytes.
6. Ignore any fully empty trailing segment created before a crash.
7. Reconstruct the next append position from the last valid complete logical record.

Recovery rules:

- Complete records before the first invalid tail remain readable.
- Partial chunk sequences at the end of the log are discarded.
- Corruption in a non-tail region should fail open rather than silently skip data.

This distinguishes expected crash tails from true corruption:

- Incomplete or torn write at the tail: recover by truncation
- Checksum or structural error in the middle: return an error and require operator intervention

## Chunking Model

Chunking is internal. Callers never append or read chunks directly.

Physical frame flags should support:

- Full record
- First chunk
- Middle chunk
- Last chunk

Suggested frame contents:

- magic/version
- frame type
- payload length
- checksum for that physical frame
- payload bytes

Replay rules:

- `Full` yields one logical record whose `Record.Position` is that frame's start offset
- `First` + zero or more `Middle` + `Last` yields one logical record whose `Record.Position` is the `First` frame's start offset
- Any incomplete chunk sequence at the valid tail is discarded during recovery
- Any malformed chunk sequence in the middle of the log is a corruption error

Initial production-oriented constraint:

- Chunking exists to bound in-memory write buffers and frame size
- In v1, a logical record must remain within one segment
- `MaxRecordSize` and `ChunkSizeBytes` must be validated against segment capacity

## Public Interfaces

The public surface should stay small and explicit.

```go
package wal

import "context"

type Config struct {
    Dir              string
    SegmentSizeBytes uint64
    ChunkSizeBytes   uint32
    MaxRecordSize    uint64
    SyncPolicy       SyncPolicy
    SyncInterval     time.Duration
    BytesPerSync     uint64
}

type Position struct {
    Segment uint64
    Offset  uint64
}

type Record struct {
    // Position is the start position of the logical record in the WAL.
    Position Position
    Data     []byte
}

type WAL interface {
    Append(ctx context.Context, data []byte) (Position, error)
    NewReader(from Position) (Reader, error)
    Close() error
}

type Reader interface {
    Next(ctx context.Context) (Record, error)
    Close() error
}

func Open(cfg Config) (WAL, error)
func ZeroPosition() Position
```

API notes:

- `Append` is the only write operation
- `NewReader(ZeroPosition())` means replay from the beginning of the WAL
- Any non-zero `Position` passed to `NewReader` must be a known logical record boundary
- `NewReader(pos)` must fail with `invalid position` when `pos` is not a logical record boundary
- A reader only guarantees replay of records that were fully visible in the WAL at the time the reader was created
- `Record.Position` returned by replay is the start position of that logical record
- `Close()` must flush pending buffered writes and stop any background sync worker cleanly
- After `Close()` begins, new appends must be rejected with `closed WAL`
- A reader is read-only and isolated from writer internals

Optional future additions, but not required initially:

- `TruncateFront` is out of scope
- `LastPosition` can be added later if a clear use case appears
- `AppendBatch` can be added later after single-record correctness is proven

## Concurrency Model

Append must be thread-safe.

Recommended approach:

- Use one internal serialized write path guarded by a mutex
- Append callers may enter concurrently, but physical file writes are ordered through one critical section
- Sync coordination may be shared, but ordering of committed positions must remain deterministic
- `Close()` should transition the WAL into a closing state, block new appends, drain in-flight buffered writes, perform the final flush/sync, then stop background coordination

This is simpler and safer than lock-free or multi-writer file access and is appropriate for a production-oriented first design.

## Error Model

Expected public errors:

- invalid config
- closed WAL
- invalid position
- corruption detected
- record exceeds maximum size

Operational principle:

- Tail truncation should be automatic only when the invalid bytes are consistent with an incomplete final write
- Mid-log corruption should return a hard error

## Implementation Phases

### phase1

- Config and validation
- Core public types
- Basic append and replay from the beginning
- Single-segment storage only
- No recovery yet

### phase2

- Segmented storage and rollover
- Position model
- Replay from position

### phase3

- Sync policy

### phase4

- Recovery

### phase5

- Chunking

## Test Plan

The test plan should focus on correctness, crash safety, and boundary behavior.

### Append and replay

- append one record, replay one record
- append many records, replay in order
- append zero-length payload
- append records from multiple goroutines and verify total ordering without loss or duplication

### Replay from position

- replay from the first record position
- replay from a middle record position
- replay from `ZeroPosition()` and verify it starts from the beginning of the WAL
- reject unknown or non-boundary positions
- reject positions that point inside a chunked logical record
- replay across multiple segments starting from a stored position

### Segment rollover

- roll over exactly at segment boundary
- roll over when remaining bytes are insufficient for next record
- preserve ordering across segments
- reopen after rollover and continue appending

### Sync behavior

- `SyncAlways` returns only after data is persisted according to the platform call used
- `SyncBatch` flushes on interval or configured byte threshold
- `SyncNone` does not force sync on append
- `Close()` flushes pending buffered writes before returning
- `Close()` stops background sync coordination cleanly and is safe after batch mode activity

### Recovery

- recover from empty directory
- recover from clean shutdown
- recover from partial final frame
- recover from partial final chunk sequence
- ignore empty trailing segment left by crash during rotation
- fail on checksum mismatch in a non-tail region

### Chunking

- append and replay a record that fits in one frame
- append and replay a record split into multiple chunks
- chunk exactly at chunk boundary
- reject record larger than `MaxRecordSize`
- ensure replay never exposes partial chunks

### Validation and robustness

- invalid config combinations
- invalid segment filenames
- truncated header
- truncated payload
- duplicate or out-of-order segment number detection
- closed WAL rejects further append or reader creation
- concurrent `Append` racing with `Close()` yields ordered completion or `closed WAL`, never silent loss

### Stress and soak

- high-concurrency append workload with periodic readers
- repeated open/append/close cycles
- large numbers of segments
- randomized record sizes around rollover and chunk boundaries
