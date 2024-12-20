package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	_ "github.com/marcboeker/go-duckdb"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	var modes []string
	validModes := map[string]bool{
		"table_ingest":  true,
		"table_samples": true,
		"table_sizes":   true,
		"query_list":    true,
		"process_logs":  true,
	}
	for mode := range validModes {
		modes = append(modes, mode)
	}

	projectID := flag.String("project", "", "Comma separated list of Google Cloud Project IDs")
	datasetID := flag.String("dataset", "", "BigQuery Dataset ID")
	sampleRate := flag.Float64("sample_rate", 0.01, "Sample rate for downloading data")
	mode := flag.String("mode", "", "Mode of operation: "+strings.Join(modes, ", "))
	concurrency := flag.Int("concurrency", 10, "Number of concurrent requests")
	dbFile := flag.String("db", "logs.db", "DuckDB database file")
	threshold := flag.Float64("threshold", 0.6, "Similarity threshold")
	flag.Parse()

	if *mode == "table_samples" && (*projectID == "" || *datasetID == "") {
		log.Fatalf("Both project and dataset flags are required when mode=table_samples")
	}

	if !validModes[*mode] {
		log.Fatalf("Invalid mode: %s. Valid modes are: table_ingest, table_samples, table_sizes, query_list, process_logs", *mode)
	}

	ctx := context.Background()

	var projects []string
	if *projectID != "" {
		projects = append(projects, strings.Split(*projectID, ",")...)
	} else {
		projectList, err := listProjects(ctx)
		if err != nil {
			log.Fatalf("Failed to list projects: %v", err)
		}
		projects = append(projects, projectList...)
	}

	db, err := sql.Open("duckdb", *dbFile)
	if err != nil {
		log.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer db.Close()

	initializeDuckDB(db)

	switch *mode {
	case "table_samples":
		handleTableSamples(ctx, *projectID, *datasetID, db, *sampleRate)
	case "query_list":
		handleQueryList(ctx, projects, db, *concurrency)
	case "table_sizes":
		handleTableSizes(ctx, projects, db, *concurrency)
	case "table_ingest":
		handleTableIngest(ctx, projects, db, *concurrency)
	case "process_logs":
		handleProcessLogs(db, *threshold)
	}
}

func handleTableSamples(ctx context.Context, projectID, datasetID string, db *sql.DB, sampleRate float64) {
	fmt.Println("Connecting to BigQuery")
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	tables, err := listTables(ctx, client, datasetID)
	if err != nil {
		log.Fatalf("Failed to list tables: %v", err)
	}

	for _, table := range tables {
		if err := downloadTableRows(ctx, client, datasetID, table, db, &sampleRate); err != nil {
			log.Printf("Failed to download data for table %s: %v", table, err)
		}
	}
}

func handleQueryList(ctx context.Context, projects []string, db *sql.DB, concurrency int) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, project := range projects {
		wg.Add(1)
		go func(project string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fmt.Println("Connecting to BigQuery project: ", project)
			client, err := bigquery.NewClient(ctx, project)
			if err != nil {
				log.Fatalf("Failed to create BigQuery client: %v", err)
			}
			if err := downloadQueryList(ctx, client, db); err != nil {
				log.Printf("Failed to download query list: %v", err)
			}
		}(project)
	}

	wg.Wait()
}

func handleTableSizes(ctx context.Context, projects []string, db *sql.DB, concurrency int) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, project := range projects {
		wg.Add(1)
		go func(project string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fmt.Println("Connecting to BigQuery project: ", project)
			client, err := bigquery.NewClient(ctx, project)
			if err != nil {
				log.Fatalf("Failed to create BigQuery client: %v", err)
			}
			if err := downloadTableSizes(ctx, client, db); err != nil {
				log.Printf("Failed to download table size for %s: %v", project, err)
			}
		}(project)
	}

	wg.Wait()
}

func handleTableIngest(ctx context.Context, projects []string, db *sql.DB, concurrency int) {
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, project := range projects {
		wg.Add(1)
		go func(project string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			fmt.Println("Connecting to BigQuery project: ", project)
			client, err := bigquery.NewClient(ctx, project)
			if err != nil {
				log.Fatalf("Failed to create BigQuery client: %v", err)
			}
			if err := downloadTableIngestion(ctx, client, db); err != nil {
				log.Printf("Failed to download table ingestion data: %v", err)
			}
		}(project)
	}

	wg.Wait()
}
