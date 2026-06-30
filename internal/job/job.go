package job

type Job struct {
	ID       int
	Payload  any
	Task     func() (any, error)
	Attempt  int // current attempt number (starts at 0)
	MaxRetry int // max retries before going to dead-letter queue
}
