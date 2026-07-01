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
	cfg      config.Config
	jobs     chan job.Job
	results  chan result.Result
	dlq      *deadletter.Queue
	workerWg sync.WaitGroup
	jobWg    sync.WaitGroup
	stopOnce sync.Once
}

func New(cfg config.Config) *Pool {
	return &Pool{
		cfg: cfg,
		// buffer must be large enough for submitted jobs AND requeued retries.
		// Use 2× JobBufSize so retries never block workers.
		jobs:    make(chan job.Job, cfg.JobBufSize*2),
		results: make(chan result.Result, cfg.ResBufSize),
		dlq:     deadletter.New(),
	}
}

func (p *Pool) Start(ctx context.Context) {
	dispatcher.Spawn(p.cfg.WorkerCount, p.jobs, p.results, p.dlq, p.cfg.JobDelayMs, &p.workerWg)

	// context watcher: external cancel/timeout triggers Stop()
	go func() {
		<-ctx.Done()
		p.Stop()
	}()
}

func (p *Pool) Submit(j job.Job) {
	p.jobWg.Add(1)
	// attach the done callback now, after jobWg.Add
	j2 := job.New(j.ID, j.Payload, j.Task, j.MaxRetry, func() {
		p.jobWg.Done()
	})
	p.jobs <- j2
}

func (p *Pool) Results() <-chan result.Result {
	return p.results
}

func (p *Pool) DeadLetters() []job.Job {
	return p.dlq.All()
}

// Stop waits for all jobs to resolve, then shuts down workers cleanly.
func (p *Pool) Stop() {
	p.stopOnce.Do(func() {
		p.jobWg.Wait()    // wait until every job calls Resolve()
		close(p.jobs)     // workers see EOF, exit their range loop
		p.workerWg.Wait() // wait for all goroutines to return
		close(p.results)  // safe — no writers remain
	})
}
