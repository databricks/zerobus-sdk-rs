package stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkCoreStreamAckPath(b *testing.B) {
	for _, tc := range []struct {
		name  string
		batch bool
	}{
		{name: "single"},
		{name: "batch", batch: true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			rpc := newFakeRPC()
			cfg := testConfig()
			cfg.MaxInflight = 1024
			cs := newCoreForTest(testParams(), cfg, newFakeOpener(rpc), nil)
			defer cs.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			ackErr := make(chan error, 1)
			go func() {
				for offset := range b.N {
					select {
					case <-rpc.sends:
						rpc.ack(int64(offset))
					case <-ctx.Done():
						ackErr <- ctx.Err()
						return
					}
				}
				ackErr <- nil
			}()

			record := []byte(`{"value":1}`)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if tc.batch {
					_, err = cs.IngestBatch(ctx, [][]byte{record, record})
				} else {
					_, err = cs.Ingest(ctx, record)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			if err := cs.Flush(ctx); err != nil {
				b.Fatal(err)
			}
			if err := <-ackErr; err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkBufferRequeue(b *testing.B) {
	for _, depth := range []int{1_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("depth_%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				b.StopTimer()
				buf := newBuffer[encodedMsg](depth, 0)
				for i := range depth {
					if err := buf.enqueue(context.Background(), int64(i), dummyMsg(int64(i))); err != nil {
						b.Fatal(err)
					}
					if _, err := buf.next(context.Background()); err != nil {
						b.Fatal(err)
					}
				}
				b.StartTimer()
				buf.requeue()
			}
		})
	}
}
