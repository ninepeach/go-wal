package benchmark

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/ninepeach/go-wal/pkg/wal"
)

func openWAL(b *testing.B, cfg wal.Config) wal.WAL {
	b.Helper()

	w, err := wal.Open(cfg)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}

	b.Cleanup(func() {
		_ = w.Close()
	})

	return w
}

func BenchmarkAppendSmallSyncNone(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 16 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    1024,
		SyncPolicy:       wal.SyncNone,
	})

	payload := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.Append(context.Background(), payload); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func BenchmarkAppendSmallSyncAlways(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 16 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    1024,
		SyncPolicy:       wal.SyncAlways,
	})

	payload := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.Append(context.Background(), payload); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func BenchmarkAppendBatchSync(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 16 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    1024,
		SyncPolicy:       wal.SyncBatch,
		SyncInterval:     50 * time.Millisecond,
		BytesPerSync:     32 * 1024,
	})

	payload := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.Append(context.Background(), payload); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func BenchmarkAppendChunked(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 32 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    64 * 1024,
		SyncPolicy:       wal.SyncNone,
	})

	payload := make([]byte, 8*1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := log.Append(context.Background(), payload); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}
}

func BenchmarkConcurrentAppend(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 64 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    1024,
		SyncPolicy:       wal.SyncNone,
	})

	payload := []byte("hello world")

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := log.Append(context.Background(), payload); err != nil {
				b.Fatalf("Append() error = %v", err)
			}
		}
	})
}

func BenchmarkReplay(b *testing.B) {
	log := openWAL(b, wal.Config{
		Dir:              b.TempDir(),
		SegmentSizeBytes: 32 * 1024 * 1024,
		ChunkSizeBytes:   256,
		MaxRecordSize:    1024,
		SyncPolicy:       wal.SyncNone,
	})

	payload := []byte("hello world")

	// 预填充
	for i := 0; i < 10000; i++ {
		if _, err := log.Append(context.Background(), payload); err != nil {
			b.Fatalf("Append() error = %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		r, err := log.NewReader(wal.ZeroPosition())
		if err != nil {
			b.Fatalf("NewReader() error = %v", err)
		}

		for {
			_, err := r.Next(context.Background())
			if err != nil {
				if err == io.EOF {
					break
				}
				b.Fatalf("Next() error = %v", err)
			}
		}
		_ = r.Close()
	}
}
