package pool

import (
	"context"
	"sync"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/dispatcher"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
)

type Pool struct {
	cfg     config.Config
	jobs    chan job.Job
	retry   chan job.Job
	results chan result.Result
	dlq     *deadletter.Queue

	workerWg sync.WaitGroup // tracks worker goroutines (for clean exit)
	jobWg    sync.WaitGroup // tracks in-flight jobs (for knowing when done)

	ctx    context.Context
	cancel context.CancelFunc
}

func New(cfg config.Config) *Pool {
	return &Pool{
		cfg:     cfg,
		jobs:    make(chan job.Job, cfg.JobBufSize),
		retry:   make(chan job.Job, cfg.JobBufSize),
		results: make(chan result.Result, cfg.ResBufSize),
		dlq:     deadletter.New(),
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.ctx, p.cancel = context.WithCancel(ctx)
	dispatcher.Spawn(p.ctx, p.cfg.WorkerCount, p.jobs, p.retry, p.results, p.dlq, p.cfg.JobDelayMs, &p.workerWg, &p.jobWg)

	// retry pump: forwards retried jobs back into the main job queue.
	// Note: jobWg.Done() was NOT called for a requeued job (worker.go
	// must NOT call Done() on retry — only on final success/dead-letter),
	// so the count stays accurate.
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				return
			case j, ok := <-p.retry:
				if !ok {
					return
				}
				select {
				case p.jobs <- j:
				case <-p.ctx.Done():
					return
				}
			}
		}
	}()

	// closer: once every submitted job has fully resolved, shut down workers
	// and close results. This runs exactly once, in its own goroutine, so
	// Stop() just has to wait for it.
	go func() {
		p.jobWg.Wait()    // blocks until every job is fully resolved
		p.cancel()        // tell worker + retry-pump select loops to exit
		p.workerWg.Wait() // wait for worker goroutines to actually return
		close(p.results)  // safe — guaranteed no more writers
	}()
}

// Submit adds a job to the queue. Must not be called after Stop() returns.
func (p *Pool) Submit(j job.Job) {
	p.jobWg.Add(1)
	p.jobs <- j
}

func (p *Pool) Results() <-chan result.Result {
	return p.results
}

func (p *Pool) DeadLetters() []job.Job {
	return p.dlq.All()
}

// Stop blocks until all submitted jobs are resolved and the pool has
// fully shut down. Safe to call once after all Submit calls are done.
func (p *Pool) Stop() {
	p.workerWg.Wait() // returns once the closer goroutine above has cancelled + drained workers
}
