package pool_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go-worker-pool/internal/config"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/pool"
)

func newTestPool(workers int) *pool.Pool {
	return pool.New(config.Config{
		WorkerCount: workers,
		JobBufSize:  256,
		ResBufSize:  256,
	})
}

func drainResults(p *pool.Pool) (ok []any, errs []error) {
	for r := range p.Results() {
		if r.Err != nil {
			errs = append(errs, r.Err)
		} else {
			ok = append(ok, r.Value)
		}
	}
	return
}

func TestAllJobsComplete(t *testing.T) {
	const total = 100
	p := newTestPool(4)
	p.Start(context.Background())

	go func() {
		for i := range total {
			id := i
			p.Submit(job.Job{
				ID:       id,
				MaxRetry: 0,
				Task:     func() (any, error) { return id, nil },
			})
		}
		p.Stop()
	}()

	ok, errs := drainResults(p)
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(ok) != total {
		t.Errorf("expected %d results, got %d", total, len(ok))
	}
}

func TestRetrySuccess(t *testing.T) {
	var attempts atomic.Int32

	p := newTestPool(2)
	p.Start(context.Background())

	go func() {
		p.Submit(job.Job{
			ID:       1,
			MaxRetry: 2,
			Task: func() (any, error) {
				n := attempts.Add(1)
				if n < 3 {
					return nil, errors.New("not yet")
				}
				return "ok", nil
			},
		})
		p.Stop()
	}()

	ok, errs := drainResults(p)

	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(ok) != 1 {
		t.Fatalf("expected 1 result, got %d", len(ok))
	}
	if ok[0] != "ok" {
		t.Errorf("expected 'ok', got %v", ok[0])
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("expected 3 attempts, got %d", n)
	}
	if dl := len(p.DeadLetters()); dl != 0 {
		t.Errorf("expected 0 dead letters, got %d", dl)
	}
}

func TestDeadLetter(t *testing.T) {
	p := newTestPool(2)
	p.Start(context.Background())

	go func() {
		p.Submit(job.Job{
			ID:       42,
			MaxRetry: 2,
			Task:     func() (any, error) { return nil, errors.New("always fails") },
		})
		p.Stop()
	}()

	ok, errs := drainResults(p)

	if len(ok) != 0 {
		t.Errorf("expected 0 successes, got %d", len(ok))
	}
	if len(errs) != 1 {
		t.Errorf("expected 1 failed result, got %d", len(errs))
	}
	dl := p.DeadLetters()
	if len(dl) != 1 {
		t.Fatalf("expected 1 dead letter, got %d", len(dl))
	}
	if dl[0].ID != 42 {
		t.Errorf("expected dead letter job id=42, got %d", dl[0].ID)
	}
}

func TestConcurrentSubmit(t *testing.T) {
	const (
		workers    = 8
		submitters = 10
		perSub     = 50
		total      = submitters * perSub
	)

	p := newTestPool(workers)
	p.Start(context.Background())

	var submitWg sync.WaitGroup
	var counter atomic.Int32

	for range submitters {
		submitWg.Add(1)
		go func() {
			defer submitWg.Done()
			for range perSub {
				id := int(counter.Add(1))
				p.Submit(job.Job{
					ID:       id,
					MaxRetry: 0,
					Task:     func() (any, error) { return id, nil },
				})
			}
		}()
	}

	go func() {
		submitWg.Wait()
		p.Stop()
	}()

	ok, errs := drainResults(p)
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(ok) != total {
		t.Errorf("expected %d results, got %d", total, len(ok))
	}
}

func TestContextCancellation(t *testing.T) {
	p := newTestPool(4)
	ctx, cancel := context.WithCancel(context.Background())
	p.Start(ctx)

	go func() {
		for i := range 200 {
			p.Submit(job.Job{
				ID:       i,
				MaxRetry: 0,
				Task: func() (any, error) {
					time.Sleep(5 * time.Millisecond)
					return "ok", nil
				},
			})
			if i == 20 {
				cancel()
			}
		}
		p.Stop()
	}()

	done := make(chan struct{})
	go func() {
		for range p.Results() {
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pool did not shut down within 5s after context cancel")
	}
}

func TestZeroJobsSubmitted(t *testing.T) {
	p := newTestPool(4)
	p.Start(context.Background())

	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() deadlocked with zero submitted jobs")
	}

	got := 0
	for range p.Results() {
		got++
	}
	if got != 0 {
		t.Errorf("expected 0 results, got %d", got)
	}
}

func TestResultsAllDelivered(t *testing.T) {
	const total = 1000

	p := newTestPool(8)
	p.Start(context.Background())

	go func() {
		for i := range total {
			id := i
			p.Submit(job.Job{
				ID:       id,
				MaxRetry: 1,
				Task:     func() (any, error) { return fmt.Sprintf("job-%d", id), nil },
			})
		}
		p.Stop()
	}()

	ok, errs := drainResults(p)
	if len(errs) != 0 {
		t.Errorf("unexpected errors: %v", errs)
	}
	if len(ok) != total {
		t.Errorf("expected %d results, got %d", total, len(ok))
	}
}
