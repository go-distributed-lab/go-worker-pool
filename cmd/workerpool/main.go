package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/pool"
)

func main() {
	cfg := config.Load()

	fmt.Printf("starting pool: workers=%d jobBuf=%d resBuf=%d\n\n",
		cfg.WorkerCount, cfg.JobBufSize, cfg.ResBufSize)

	// overall timeout — if jobs aren't done in time, pool cancels everything
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	p := pool.New(cfg)
	p.Start(ctx)

	const totalJobs = 15

	go func() {
		for i := range totalJobs {
			id := i
			p.Submit(job.Job{
				ID:       id,
				Payload:  fmt.Sprintf("task-%d", id),
				MaxRetry: 2,
				Task: func() (any, error) {
					duration := time.Duration(rand.Intn(150)+50) * time.Millisecond
					time.Sleep(duration)

					// simulate ~25% failure rate to exercise retry + DLQ
					if rand.Intn(4) == 0 {
						return nil, errors.New("simulated transient failure")
					}
					return fmt.Sprintf("task-%d done in %v", id, duration), nil
				},
			})
		}
		p.Stop()
	}()

	succeeded := 0
	failed := 0

	for r := range p.Results() {
		if r.Err != nil {
			log.Printf("[result] job=%-3d FAILED: %v", r.JobID, r.Err)
			failed++
			continue
		}
		fmt.Printf("[result] job=%-3d value=%v\n", r.JobID, r.Value)
		succeeded++
	}

	fmt.Printf("\n--- summary ---\n")
	fmt.Printf("total=%d  succeeded=%d  failed=%d  dead-letters=%d\n",
		totalJobs, succeeded, failed, len(p.DeadLetters()))
}
