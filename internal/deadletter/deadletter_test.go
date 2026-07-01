package deadletter_test

import (
	"errors"
	"sync"
	"testing"

	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/job"
)

func TestNew_EmptyOnCreate(t *testing.T) {
	q := deadletter.New()

	if q.Len() != 0 {
		t.Errorf("want 0 length on new queue, got %d", q.Len())
	}
	if all := q.All(); len(all) != 0 {
		t.Errorf("want empty slice from All(), got %v", all)
	}
}

func TestAdd_SingleJob(t *testing.T) {
	q := deadletter.New()
	j := job.Job{ID: 42}

	q.Add(j, errors.New("boom"))

	if q.Len() != 1 {
		t.Errorf("want Len()=1, got %d", q.Len())
	}
	all := q.All()
	if len(all) != 1 {
		t.Fatalf("want 1 job in All(), got %d", len(all))
	}
	if all[0].ID != 42 {
		t.Errorf("want job ID=42, got %d", all[0].ID)
	}
}

func TestAdd_MultipleJobs(t *testing.T) {
	q := deadletter.New()

	for i := range 5 {
		q.Add(job.Job{ID: i}, errors.New("fail"))
	}

	if q.Len() != 5 {
		t.Errorf("want Len()=5, got %d", q.Len())
	}
	if len(q.All()) != 5 {
		t.Errorf("want 5 jobs from All(), got %d", len(q.All()))
	}
}

func TestAll_ReturnsCopy(t *testing.T) {
	q := deadletter.New()
	q.Add(job.Job{ID: 1}, errors.New("fail"))

	// mutating the returned slice must not affect the queue
	all := q.All()
	all[0] = job.Job{ID: 999}

	original := q.All()
	if original[0].ID == 999 {
		t.Error("All() returned a reference to internal slice, not a copy")
	}
}

func TestAdd_ConcurrentSafe(t *testing.T) {
	// run with -race to verify no data races
	q := deadletter.New()
	const goroutines = 50

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			q.Add(job.Job{ID: id}, errors.New("concurrent fail"))
		}()
	}
	wg.Wait()

	if q.Len() != goroutines {
		t.Errorf("want %d dead letters, got %d", goroutines, q.Len())
	}
}

func TestLen_ConcurrentSafe(t *testing.T) {
	q := deadletter.New()
	var wg sync.WaitGroup

	// concurrent adds and reads
	for i := range 30 {
		wg.Add(2)
		id := i
		go func() {
			defer wg.Done()
			q.Add(job.Job{ID: id}, errors.New("fail"))
		}()
		go func() {
			defer wg.Done()
			_ = q.Len() // just must not race
		}()
	}
	wg.Wait()
}
