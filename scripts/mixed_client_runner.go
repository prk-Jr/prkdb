package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	models "mixedclient/client_go"
)

const (
	defaultServer     = "http://127.0.0.1:8080"
	defaultCollection = "benchmark"
	defaultRecords    = 1000
	defaultIDPrefix   = "go"
)

type sampleIDList []string

func (s *sampleIDList) String() string {
	return strings.Join(*s, ",")
}

func (s *sampleIDList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("sample id must not be empty")
	}
	*s = append(*s, value)
	return nil
}

type runnerConfig struct {
	Mode       string
	ServerURL  string
	Collection string
	Records    int
	IDPrefix   string
	ClientDir  string
	SampleIDs  []string
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

func parseArgs() runnerConfig {
	mode := flag.String("mode", getenvString("PRKDB_MODE", "write"), "Mode: write or read")
	server := flag.String("server", getenvString("PRKDB_SERVER", defaultServer), "PrkDB server URL")
	collection := flag.String("collection", getenvString("PRKDB_COLLECTION", defaultCollection), "Collection name")
	records := flag.Int("records", getenvInt("NUM_RECORDS", defaultRecords), "Number of records to write")
	idPrefix := flag.String("id-prefix", getenvString("PRKDB_ID_PREFIX", defaultIDPrefix), "Deterministic id prefix")
	clientDir := flag.String("client-dir", getenvString("PRKDB_CLIENT_DIR", "."), "Generated Go client directory")

	var sampleIDs sampleIDList
	flag.Var(&sampleIDs, "sample-id", "Sample id to verify; repeatable")
	flag.Parse()

	if *mode != "write" && *mode != "read" {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --mode must be write or read")
		os.Exit(2)
	}
	if *server == "" {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --server must not be empty")
		os.Exit(2)
	}
	if *collection == "" {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --collection must not be empty")
		os.Exit(2)
	}
	if *idPrefix == "" {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --id-prefix must not be empty")
		os.Exit(2)
	}
	if *clientDir == "" {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --client-dir must not be empty")
		os.Exit(2)
	}
	if *records <= 0 {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: --records must be greater than 0")
		os.Exit(2)
	}
	if *mode == "read" && len(sampleIDs) == 0 {
		fmt.Fprintln(os.Stderr, "❌ Configuration error: at least one --sample-id is required")
		os.Exit(2)
	}

	return runnerConfig{
		Mode:       *mode,
		ServerURL:  *server,
		Collection: *collection,
		Records:    *records,
		IDPrefix:   *idPrefix,
		ClientDir:  *clientDir,
		SampleIDs:  sampleIDs,
	}
}

func buildRecord(index int, idPrefix string) models.Benchmark {
	recordID := fmt.Sprintf("%s-%06d", idPrefix, index+1)
	return models.Benchmark{
		Id:        recordID,
		Payload:   recordID,
		Timestamp: time.Now().UnixMilli(),
	}
}

func ensureClientDir(clientDir string) {
	if info, err := os.Stat(clientDir); err != nil || !info.IsDir() {
		fmt.Fprintf(os.Stderr, "❌ Configuration error: client dir %q is not available\n", clientDir)
		os.Exit(2)
	}
}

func runWrite(config runnerConfig) {
	client := models.NewPrkDbClientWithCredential(config.ServerURL, os.Getenv("PRKDB_CREDENTIAL"))

	for index := 0; index < config.Records; index++ {
		if err := client.Put(config.Collection, buildRecord(index, config.IDPrefix)); err != nil {
			fmt.Fprintf(os.Stderr, "❌ write failed for record %s-%06d: %v\n", config.IDPrefix, index+1, err)
			os.Exit(1)
		}
	}

	fmt.Printf(
		"✅ Go mixed-client write: collection=%s records=%d range=%s-000001..%s-%06d\n",
		config.Collection,
		config.Records,
		config.IDPrefix,
		config.IDPrefix,
		config.Records,
	)
}

func runRead(config runnerConfig) {
	client := models.NewPrkDbClientWithCredential(config.ServerURL, os.Getenv("PRKDB_CREDENTIAL"))
	rows, err := client.ListRaw(config.Collection, models.ListOptions{Limit: 10000})
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ read failed: %v\n", err)
		os.Exit(1)
	}

	rowsByID := make(map[string]map[string]any, len(rows))
	for _, row := range rows {
		idValue, ok := row["id"].(string)
		if ok {
			rowsByID[idValue] = row
		}
	}

	for _, sampleID := range config.SampleIDs {
		if _, ok := rowsByID[sampleID]; !ok {
			fmt.Fprintf(os.Stderr, "❌ missing expected sample id: %s\n", sampleID)
			os.Exit(1)
		}
	}

	fmt.Printf(
		"✅ Go mixed-client read: collection=%s sample_ids=%d fetched_rows=%d\n",
		config.Collection,
		len(config.SampleIDs),
		len(rowsByID),
	)
}

func main() {
	config := parseArgs()
	ensureClientDir(config.ClientDir)

	switch config.Mode {
	case "write":
		runWrite(config)
	case "read":
		if len(config.SampleIDs) == 0 {
			fmt.Fprintln(os.Stderr, "❌ Configuration error: at least one --sample-id is required")
			os.Exit(2)
		}
		runRead(config)
	default:
		fmt.Fprintf(os.Stderr, "❌ unsupported mode: %s\n", config.Mode)
		os.Exit(1)
	}
}
