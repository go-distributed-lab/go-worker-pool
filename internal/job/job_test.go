package job_test

import (
	"sync/atomic"
	"testing"

	"go-worker-pool/internal/job"
)

func TestNew_FieldsSetCorrectly(t *testing.T) {
	called := atomic.Int32{}
	done := func() { called.Add(1) }

	j := job.New(42, "payload", nil, 3, done)

	if j.ID != 42 {
		t.Errorf("ID: want 42, got %d", j.ID)
	}
	if j.Payload != "payload" {
		t.Errorf("Payload: want 'payload', got %v", j.Payload)
	}
	if j.MaxRetry != 3 {
		t.Errorf("MaxRetry: want 3, got %d", j.MaxRetry)
	}
	if j.Attempt != 0 {
		t.Errorf("Attempt: want 0 initially, got %d", j.Attempt)
	}
}

func TestResolve_CallsDoneExactlyOnce(t *testing.T) {
	called := atomic.Int32{}
	j := job.New(1, nil, nil, 0, func() { called.Add(1) })

	j.Resolve()

	if n := called.Load(); n != 1 {
		t.Errorf("done called %d times, want 1", n)
	}
}

func TestResolve_NilDone_NoPanic(t *testing.T) {
	// Jobs submitted via pool.Submit() get a done callback injected,
	// but a zero-value Job (done=nil) must not panic on Resolve().
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Resolve() panicked with nil done: %v", r)
		}
	}()

	j := job.Job{ID: 1}
	j.Resolve() // must not panic
}

func TestJob_TaskIsCallable(t *testing.T) {
	j := job.New(1, nil, func() (any, error) {
		return "result", nil
	}, 0, func() {})

	val, err := j.Task()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if val != "result" {
		t.Errorf("want 'result', got %v", val)
	}
}
