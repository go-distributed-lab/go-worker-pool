package deadletter

import (
	"fmt"
	"sync"

	"go-worker-pool/internal/job"
)

// Queue stores jobs that exhausted all retries.
type Queue struct {
	mu   sync.Mutex
	jobs []job.Job
}

func New() *Queue {
	return &Queue{}
}

func (q *Queue) Add(j job.Job, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs = append(q.jobs, j)
	fmt.Printf("[dead-letter] job-%d permanently failed after %d attempts: %v\n", j.ID, j.Attempt, err)
}

func (q *Queue) All() []job.Job {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]job.Job, len(q.jobs))
	copy(out, q.jobs)
	return out
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.jobs)
}
