package job

type Job struct {
	ID      int
	Payload any
}

type Result struct {
	JobID int
	Value any
	Err   error
}
