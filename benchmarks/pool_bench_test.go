package benchmarks_test

import (
	"context"
	"testing"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/pool"
)

func benchmarkPool(b *testing.B, workers, jobs int) {
	b.Helper()

	cfg := config.Config{
		WorkerCount: workers,
		JobBufSize:  jobs,
		ResBufSize:  jobs,
		JobDelayMs:  0,
	}

	b.ResetTimer()

	for range b.N {
		p := pool.New(cfg)
		p.Start(context.Background())

		go func() {
			for i := range jobs {
				id := i
				p.Submit(job.Job{
					ID:       id,
					MaxRetry: 0,
					Task: func() (any, error) {
						// lightweight CPU work to avoid pure channel benchmarking
						sum := 0
						for k := range 100 {
							sum += k
						}
						return sum, nil
					},
				})
			}
			p.Stop()
		}()

		for range p.Results() {
		}
	}
}

// vary worker count, fixed job load
func BenchmarkPool_Workers2_Jobs1000(b *testing.B)  { benchmarkPool(b, 2, 1000) }
func BenchmarkPool_Workers4_Jobs1000(b *testing.B)  { benchmarkPool(b, 4, 1000) }
func BenchmarkPool_Workers8_Jobs1000(b *testing.B)  { benchmarkPool(b, 8, 1000) }
func BenchmarkPool_Workers16_Jobs1000(b *testing.B) { benchmarkPool(b, 16, 1000) }
func BenchmarkPool_Workers32_Jobs1000(b *testing.B) { benchmarkPool(b, 32, 1000) }

// vary job load, fixed worker count
func BenchmarkPool_Workers8_Jobs100(b *testing.B)   { benchmarkPool(b, 8, 100) }
func BenchmarkPool_Workers8_Jobs5000(b *testing.B)  { benchmarkPool(b, 8, 5000) }
func BenchmarkPool_Workers8_Jobs10000(b *testing.B) { benchmarkPool(b, 8, 10000) }

// single worker baseline — shows channel overhead with no parallelism
func BenchmarkPool_Workers1_Jobs1000(b *testing.B) { benchmarkPool(b, 1, 1000) }
