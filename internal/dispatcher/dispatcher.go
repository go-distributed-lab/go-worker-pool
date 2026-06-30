package dispatcher

import (
	"context"
	"sync"

	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
	"go-worker-pool/internal/worker"
)

func Spawn(ctx context.Context, count int, jobs <-chan job.Job, retry chan<- job.Job, results chan<- result.Result, dlq *deadletter.Queue, delayMs int, workerWg, jobWg *sync.WaitGroup) {
	for i := range count {
		w := worker.New(i, jobs, retry, results, dlq, delayMs, jobWg)
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			w.Start(ctx)
		}()
	}
}
