package main

import (
	"context"
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
				MaxRetry: 2,
				Task: func() (any, error) {
					duration := time.Duration(rand.Intn(150)+50) * time.Millisecond
					time.Sleep(duration)
					if rand.Intn(4) == 0 {
						return nil, fmt.Errorf("transient failure")
					}
					return fmt.Sprintf("task-%d done in %v", id, duration), nil
				},
			})
		}
		p.Stop()
	}()

	succeeded, failed := 0, 0
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
