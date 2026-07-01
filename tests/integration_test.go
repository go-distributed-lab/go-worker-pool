package tests_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/pool"
)

func newPool(workers int) *pool.Pool {
	return pool.New(config.Config{
		WorkerCount: workers,
		JobBufSize:  512,
		ResBufSize:  512,
	})
}

func TestIntegration_RetryThenDeadLetter(t *testing.T) {
	const (
		alwaysOK   = 30
		retryOK    = 10
		alwaysFail = 5
		totalJobs  = alwaysOK + retryOK + alwaysFail
		expectedOK = alwaysOK + retryOK
		expectedDL = alwaysFail
	)

	p := newPool(6)
	p.Start(context.Background())

	go func() {
		// always-OK jobs
		for i := range alwaysOK {
			id := i // capture loop variable
			p.Submit(job.Job{
				ID:       id,
				MaxRetry: 0,
				Task:     func() (any, error) { return id, nil },
			})
		}

		// retry-then-OK: each job gets its OWN attempts counter
		for i := range retryOK {
			id := alwaysOK + i // capture loop variable
			var attempts atomic.Int32
			p.Submit(job.Job{
				ID:       id,
				MaxRetry: 2,
				Task: func() (any, error) {
					if attempts.Add(1) < 3 {
						return nil, errors.New("transient")
					}
					return id, nil
				},
			})
		}

		// always-fail jobs
		for i := range alwaysFail {
			id := alwaysOK + retryOK + i // capture loop variable
			p.Submit(job.Job{
				ID:       id,
				MaxRetry: 1,
				Task:     func() (any, error) { return nil, errors.New("permanent") },
			})
		}

		p.Stop()
	}()

	ok, failed := 0, 0
	for r := range p.Results() {
		if r.Err != nil {
			failed++
		} else {
			ok++
		}
	}

	if ok != expectedOK {
		t.Errorf("succeeded: want %d got %d", expectedOK, ok)
	}
	if failed != expectedDL {
		t.Errorf("failed results: want %d got %d", expectedDL, failed)
	}
	if dl := len(p.DeadLetters()); dl != expectedDL {
		t.Errorf("dead letters: want %d got %d", expectedDL, dl)
	}
}

func TestIntegration_GracefulShutdownUnderLoad(t *testing.T) {
	p := newPool(8)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	p.Start(ctx)

	done := make(chan struct{})
	go func() {
		for i := range 500 {
			p.Submit(job.Job{
				ID:       i,
				MaxRetry: 0,
				Task: func() (any, error) {
					time.Sleep(5 * time.Millisecond)
					return "ok", nil
				},
			})
		}
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not shut down within 5s — possible goroutine leak")
	}
}
