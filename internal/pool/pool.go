package pool

import (
	"context"
	"sync"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/dispatcher"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
)

type Pool struct {
	cfg     config.Config
	jobs    chan job.Job
	results chan result.Result
	wg      sync.WaitGroup
	cancel  context.CancelFunc
}

func New(cfg config.Config) *Pool {
	return &Pool{
		cfg:     cfg,
		jobs:    make(chan job.Job, cfg.JobBufSize),
		results: make(chan result.Result, cfg.ResBufSize),
	}
}

func (p *Pool) Start(ctx context.Context) {
	ctx, p.cancel = context.WithCancel(ctx)
	dispatcher.Spawn(ctx, p.cfg.WorkerCount, p.jobs, p.results, p.cfg.JobDelayMs, &p.wg)
}

func (p *Pool) Submit(j job.Job) {
	p.jobs <- j
}

func (p *Pool) Results() <-chan result.Result {
	return p.results
}

func (p *Pool) Stop() {
	close(p.jobs)    // signals workers: no more jobs coming
	p.wg.Wait()      // wait for all workers to finish
	close(p.results) // safe to close now — no writers remain
}
