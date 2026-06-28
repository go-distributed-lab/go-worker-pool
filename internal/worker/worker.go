package worker

import (
	"context"
	"time"

	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
)

type Worker struct {
	ID         int
	Jobs       <-chan job.Job
	Results    chan<- result.Result
	JobDelayMs int
}

func New(id int, jobs <-chan job.Job, results chan<- result.Result, delayMs int) *Worker {
	return &Worker{
		ID:         id,
		Jobs:       jobs,
		Results:    results,
		JobDelayMs: delayMs,
	}
}

func (w *Worker) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-w.Jobs:
			if !ok {
				return
			}
			if w.JobDelayMs > 0 {
				time.Sleep(time.Duration(w.JobDelayMs) * time.Millisecond)
			}
			w.Results <- result.Result{JobID: j.ID, Value: j.Payload}
		}
	}
}
