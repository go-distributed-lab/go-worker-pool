package worker

import (
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
)

var (
	logMu  sync.Mutex
	logout io.Writer = os.Stdout
)

func logf(format string, args ...any) {
	logMu.Lock()
	fmt.Fprintf(logout, format, args...)
	logMu.Unlock()
}

// Silence disables all worker logs — call in benchmarks via TestMain.
func Silence() {
	logMu.Lock()
	logout = io.Discard
	logMu.Unlock()
}

// Verbose re-enables worker logs.
func Verbose() {
	logMu.Lock()
	logout = os.Stdout
	logMu.Unlock()
}

type Worker struct {
	ID         int
	Jobs       chan job.Job // bidirectional — worker requeues here directly
	Results    chan<- result.Result
	DeadLetter *deadletter.Queue
	JobDelayMs int
}

func New(id int, jobs chan job.Job, results chan<- result.Result, dlq *deadletter.Queue, delayMs int) *Worker {
	return &Worker{
		ID:         id,
		Jobs:       jobs,
		Results:    results,
		DeadLetter: dlq,
		JobDelayMs: delayMs,
	}
}

func (w *Worker) Start() {
	logf("[worker-%d] started\n", w.ID)
	defer logf("[worker-%d] shutting down\n", w.ID)

	for j := range w.Jobs {
		w.process(j)
	}
}

func (w *Worker) process(j job.Job) {
	logf("[worker-%d] picked up job-%d (attempt %d)\n", w.ID, j.ID, j.Attempt+1)

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

	if err != nil {
		j.Attempt++
		if j.Attempt <= j.MaxRetry {
			logf("[worker-%d] job-%d failed, requeueing (attempt %d/%d): %v\n",
				w.ID, j.ID, j.Attempt, j.MaxRetry, err)
			// requeue directly — job.done NOT called yet
			w.Jobs <- j
			return
		}
		// exhausted retries — final resolution
		w.DeadLetter.Add(j, err)
		w.Results <- result.Result{JobID: j.ID, Err: err}
		j.Resolve() // calls jobWg.Done() via callback
		return
	}

	// success — final resolution
	w.Results <- result.Result{JobID: j.ID, Value: val}
	j.Resolve() // calls jobWg.Done() via callback
}
