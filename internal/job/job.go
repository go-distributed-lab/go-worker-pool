package job

type Job struct {
	ID      int
	Payload any
	Task    func() (any, error)
}
