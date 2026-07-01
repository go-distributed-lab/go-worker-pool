package job

type Job struct {
	ID       int
	Payload  any
	Task     func() (any, error)
	Attempt  int
	MaxRetry int
	done     func() // called exactly once when job is fully resolved
}

// New creates a job with a completion callback.
func New(id int, payload any, task func() (any, error), maxRetry int, done func()) Job {
	return Job{
		ID:       id,
		Payload:  payload,
		Task:     task,
		MaxRetry: maxRetry,
		done:     done,
	}
}

// Resolve marks the job as fully done. Safe to call exactly once.
func (j *Job) Resolve() {
	if j.done != nil {
		j.done()
	}
}
