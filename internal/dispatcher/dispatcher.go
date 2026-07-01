package dispatcher

import (
	"sync"

	"go-worker-pool/internal/deadletter"
	"go-worker-pool/internal/job"
	"go-worker-pool/internal/result"
	"go-worker-pool/internal/worker"
)

func Spawn(count int, jobs chan job.Job, results chan<- result.Result, dlq *deadletter.Queue, delayMs int, workerWg *sync.WaitGroup) {
	for i := range count {
		w := worker.New(i, jobs, results, dlq, delayMs)
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			w.Start()
		}()
	}
}
