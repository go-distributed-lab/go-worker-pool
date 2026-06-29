package worker

import (
	"context"
	"fmt"
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
	fmt.Printf("[worker-%d] started\n", w.ID)
	defer fmt.Printf("[worker-%d] stopped\n", w.ID)

	for {
		select {
		case <-ctx.Done():
			return
		case j, ok := <-w.Jobs:
			if !ok {
				return
			}

			fmt.Printf("[worker-%d] picked up job-%d\n", w.ID, j.ID)

			if w.JobDelayMs > 0 {
				time.Sleep(time.Duration(w.JobDelayMs) * time.Millisecond)
			}

			var val any
			var err error

			if j.Task != nil {
				val, err = j.Task()
			} else {
				val = j.Payload
			}
			w.Results <- result.Result{JobID: j.ID, Value: val, Err: err}
		}
	}
}
