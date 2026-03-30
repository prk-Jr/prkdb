package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"benchclient/client_go"
)

const (
	defaultServer     = "http://127.0.0.1:8080"
	defaultRecords    = 10000
	defaultCollection = "benchmark"
	defaultIDPrefix   = "bench_go"
	batchSize         = 100
	payload           = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

type benchmarkConfig struct {
	ServerURL  string
	NumRecords int
	Collection string
	IDPrefix   string
}

func getenvString(key string, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	if value == "" {
		fmt.Fprintf(os.Stderr, "❌ Configuration error: %s must not be empty\n", key)
		os.Exit(2)
	}
	return value
}

func getenvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Configuration error: %s must be an integer, got %q\n", key, value)
		os.Exit(2)
	}
	if parsed <= 0 {
		fmt.Fprintf(os.Stderr, "❌ Configuration error: %s must be greater than 0, got %d\n", key, parsed)
		os.Exit(2)
	}

	return parsed
}

func loadConfig() benchmarkConfig {
	return benchmarkConfig{
		ServerURL:  getenvString("PRKDB_SERVER", defaultServer),
		NumRecords: getenvInt("NUM_RECORDS", defaultRecords),
		Collection: getenvString("PRKDB_COLLECTION", defaultCollection),
		IDPrefix:   getenvString("PRKDB_ID_PREFIX", defaultIDPrefix),
	}
}

func buildRecord(index int, idPrefix string) models.Benchmark {
	return models.Benchmark{
		Id:        fmt.Sprintf("%s_%d", idPrefix, index),
		Payload:   payload,
		Timestamp: time.Now().UnixMilli(),
	}
}

func main() {
	config := loadConfig()

	fmt.Printf("🚀 Connecting to %s...\n", config.ServerURL)
	client := models.NewPrkDbClient(config.ServerURL)

	fmt.Printf("  📤 Starting Producer: %d records...\n", config.NumRecords)
	start := time.Now()
	var successCount int64
	var failureCount int64

	// This file is copied into a temporary Go module where the generated client
	// lives at benchclient/client_go and keeps package name models.
	for batchStart := 0; batchStart < config.NumRecords; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > config.NumRecords {
			batchEnd = config.NumRecords
		}

		var waitGroup sync.WaitGroup
		for index := batchStart; index < batchEnd; index++ {
			waitGroup.Add(1)
			go func(recordIndex int) {
				defer waitGroup.Done()

				if err := client.Put(config.Collection, buildRecord(recordIndex, config.IDPrefix)); err != nil {
					atomic.AddInt64(&failureCount, 1)
					fmt.Printf("Error: %v\n", err)
					return
				}

				atomic.AddInt64(&successCount, 1)
			}(index)
		}
		waitGroup.Wait()
	}

	duration := time.Since(start).Seconds()
	mbps := (float64(successCount) * float64(len(payload))) / duration / 1024 / 1024

	fmt.Printf("✅ Producer Finished: %d/%d records\n", successCount, config.NumRecords)
	if failureCount > 0 {
		fmt.Printf("❌ Failed Writes: %d\n", failureCount)
	}
	fmt.Printf("⏱️  Duration: %.2fs\n", duration)
	fmt.Printf("📈 Throughput: %.2f MB/s\n", mbps)

	if failureCount > 0 {
		fmt.Fprintf(os.Stderr, "❌ benchmark failed with %d write errors\n", failureCount)
		os.Exit(1)
	}
}
