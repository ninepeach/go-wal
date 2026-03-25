package wal

// RecoveryPlan describes the work required to make the WAL readable.
type RecoveryPlan struct {
	TruncateSegment uint64
	TruncateOffset  uint64
}

// RecoveryResult describes the WAL state after startup recovery.
type RecoveryResult struct {
	LastSegment uint64
	LastOffset  uint64
}

// RecoveryManager validates and repairs the WAL tail on open.
type RecoveryManager interface {
	Plan() (RecoveryPlan, error)
	Recover() (RecoveryResult, error)
}
