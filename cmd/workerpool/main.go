package main

import (
	"context"
	"fmt"
	"log"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/pool"
)

func main() {
	cfg := config.Load()
	p := pool.New(cfg)
	p.Start(context.Background())

	go func() {
		for i := range 10 {
			p.Submit(job.Job{ID: i, Payload: fmt.Sprintf("task-%d", i)})
		}
		p.Stop()
	}()

	for r := range p.Results() {
		if r.Err != nil {
			log.Printf("job %d failed: %v", r.JobID, r.Err)
			continue
		}
		fmt.Printf("[result] job=%-3d value=%v\n", r.JobID, r.Value)
	}
}
