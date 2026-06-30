package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
)

type Worker struct {
	ID         int
	Jobs       <-chan job.Job
	Retry      chan<- job.Job
	Results    chan<- result.Result
	DeadLetter *deadletter.Queue
	JobDelayMs int
	JobWg      *sync.WaitGroup // decremented exactly once per job, on final resolution
}

func New(id int, jobs <-chan job.Job, retry chan<- job.Job, results chan<- result.Result, dlq *deadletter.Queue, delayMs int, jobWg *sync.WaitGroup) *Worker {
	return &Worker{
		ID:         id,
		Jobs:       jobs,
		Retry:      retry,
		Results:    results,
		DeadLetter: dlq,
		JobDelayMs: delayMs,
		JobWg:      jobWg,
	}
}

func (w *Worker) Start(ctx context.Context) {
	fmt.Printf("[worker-%d] started\n", w.ID)
	defer fmt.Printf("[worker-%d] shutting down\n", w.ID)

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[worker-%d] context cancelled, exiting\n", w.ID)
			return
		case j, ok := <-w.Jobs:
			if !ok {
				return
			}
			w.process(ctx, j)
		}
	}
}

func (w *Worker) process(ctx context.Context, j job.Job) {
	fmt.Printf("[worker-%d] picked up job-%d (attempt %d)\n", w.ID, j.ID, j.Attempt+1)

	if w.JobDelayMs > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(w.JobDelayMs) * time.Millisecond):
		}
	}

	var val any
	var err error

	if j.Task != nil {
		val, err = j.Task()
	} else {
		val = j.Payload
	}

	if err != nil {
		j.Attempt++
		if j.Attempt <= j.MaxRetry {
			fmt.Printf("[worker-%d] job-%d failed, requeueing (attempt %d/%d): %v\n",
				w.ID, j.ID, j.Attempt, j.MaxRetry, err)
			// NOT calling JobWg.Done() here — job isn't resolved yet, it's
			// going back into the queue. The jobWg count must stay > 0.
			select {
			case w.Retry <- j:
			case <-ctx.Done():
				w.JobWg.Done() // pool is shutting down, job won't be retried — release it
			}
			return
		}
		// exhausted retries — this IS final resolution
		w.DeadLetter.Add(j, err)
		w.Results <- result.Result{JobID: j.ID, Err: err}
		w.JobWg.Done()
		return
	}

	// success — final resolution
	w.Results <- result.Result{JobID: j.ID, Value: val}
	w.JobWg.Done()
}
