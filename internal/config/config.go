package config

import (
	"os"
	"strconv"
)

type Config struct {
	WorkerCount int
	JobBufSize  int
	ResBufSize  int
	JobDelayMs  int
}

func Load() Config {
	return Config{
		WorkerCount: getEnvInt("WORKER_COUNT", 4),
		JobBufSize:  getEnvInt("JOB_BUF_SIZE", 100),
		ResBufSize:  getEnvInt("RES_BUF_SIZE", 100),
		JobDelayMs:  getEnvInt("JOB_DELAY_MS", 0),
	}
}

func getEnvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
