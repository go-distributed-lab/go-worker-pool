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

	fmt.Printf("[main] starting worker pool : workers = %d  , jobsBuffer = %d  , resultsBuffer = %d  , jobDelayMs = %d\n",
		cfg.WorkerCount, cfg.JobBufSize, cfg.ResBufSize, cfg.JobDelayMs)
	p := pool.New(cfg)
	p.Start(context.Background())

	const totalJobs = 20

	go func() {
		for i := range totalJobs {
			id := i
			p.Submit(job.Job{
				ID:      id,
				Payload: fmt.Sprintf("task-%d", i),
				Task: func() (any, error) {
					duration := time.Duration(rand.Intn(200)+50) * time.Millisecond
					time.Sleep(duration)
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
			log.Printf("job %d failed: %v", r.JobID, r.Err)
			failed++
			continue
		}
		fmt.Printf("[result] job=%-3d value=%v\n", r.JobID, r.Value)
		succeeded++
	}
	fmt.Printf("\n-----summary-----")
	fmt.Printf("total = %d , succeeded = %d , failed = %d\n", totalJobs, succeeded, failed)
}
