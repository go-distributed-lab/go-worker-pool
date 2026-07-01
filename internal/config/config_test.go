package config_test

import (
	"os"
	"testing"

	"go-worker-pool/internal/config"
)

func TestLoad_Defaults(t *testing.T) {
	// clear any env vars that might be set in the environment
	os.Unsetenv("WORKER_COUNT")
	os.Unsetenv("JOB_BUF_SIZE")
	os.Unsetenv("RES_BUF_SIZE")
	os.Unsetenv("JOB_DELAY_MS")

	cfg := config.Load()

	if cfg.WorkerCount != 4 {
		t.Errorf("WorkerCount: want 4, got %d", cfg.WorkerCount)
	}
	if cfg.JobBufSize != 100 {
		t.Errorf("JobBufSize: want 100, got %d", cfg.JobBufSize)
	}
	if cfg.ResBufSize != 100 {
		t.Errorf("ResBufSize: want 100, got %d", cfg.ResBufSize)
	}
	if cfg.JobDelayMs != 0 {
		t.Errorf("JobDelayMs: want 0, got %d", cfg.JobDelayMs)
	}
}

func TestLoad_FromEnv(t *testing.T) {
	os.Setenv("WORKER_COUNT", "8")
	os.Setenv("JOB_BUF_SIZE", "200")
	os.Setenv("RES_BUF_SIZE", "150")
	os.Setenv("JOB_DELAY_MS", "50")
	defer func() {
		os.Unsetenv("WORKER_COUNT")
		os.Unsetenv("JOB_BUF_SIZE")
		os.Unsetenv("RES_BUF_SIZE")
		os.Unsetenv("JOB_DELAY_MS")
	}()

	cfg := config.Load()

	if cfg.WorkerCount != 8 {
		t.Errorf("WorkerCount: want 8, got %d", cfg.WorkerCount)
	}
	if cfg.JobBufSize != 200 {
		t.Errorf("JobBufSize: want 200, got %d", cfg.JobBufSize)
	}
	if cfg.ResBufSize != 150 {
		t.Errorf("ResBufSize: want 150, got %d", cfg.ResBufSize)
	}
	if cfg.JobDelayMs != 50 {
		t.Errorf("JobDelayMs: want 50, got %d", cfg.JobDelayMs)
	}
}

func TestLoad_InvalidEnvFallsBackToDefault(t *testing.T) {
	os.Setenv("WORKER_COUNT", "not-a-number")
	defer os.Unsetenv("WORKER_COUNT")

	cfg := config.Load()

	// invalid value must fall back to default, not panic or return 0
	if cfg.WorkerCount != 4 {
		t.Errorf("WorkerCount: want default 4 on invalid input, got %d", cfg.WorkerCount)
	}
}

func TestLoad_ZeroValueEnv(t *testing.T) {
	os.Setenv("WORKER_COUNT", "0")
	defer os.Unsetenv("WORKER_COUNT")

	cfg := config.Load()

	// 0 is a valid integer parse — it should come through as 0, not default
	if cfg.WorkerCount != 0 {
		t.Errorf("WorkerCount: want 0 for explicit zero, got %d", cfg.WorkerCount)
	}
}
