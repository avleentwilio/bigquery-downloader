package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	_ "github.com/marcboeker/go-duckdb"
	"google.golang.org/api/iterator"
)

func main() {
	var modes []string
	validModes := map[string]bool{
		"table_ingest":  true,
		"table_samples": true,
		"table_sizes":   true,
		"query_list":    true,
	}
	for mode := range validModes {
		modes = append(modes, mode)
	}

	projectID := flag.String("project", "", "Comma separated list of Google Cloud Project IDs")
	datasetID := flag.String("dataset", "", "BigQuery Dataset ID")
	sampleRate := flag.Float64("sample_rate", 0.01, "Sample rate for downloading data")
	mode := flag.String("mode", "", "Mode of operation: "+strings.Join(modes, ", "))
	concurrency := flag.Int("concurrency", 10, "Number of concurrent requests")
	flag.Parse()

	if *mode == "table_samples" && (*projectID == "" || *datasetID == "") {
		log.Fatalf("Both project and dataset flags are required when mode=table_samples")
	}

	// mode should be in the list of validModes
	if !validModes[*mode] {
		log.Fatalf("Invalid mode: %s. Valid modes are: table_ingest, table_samples, table_sizes, query_list", *mode)
	}

	ctx := context.Background()

	// Store the project ID in a slice
	var projects []string
	if *projectID != "" {
		projects = append(projects, strings.Split(*projectID, ",")...)
	} else {
		// Get a list of all BigQuery projects
		fmt.Println("Getting a list of all Quantico projects in GCP")
		projectList, err := listProjects(ctx)
		if err != nil {
			log.Fatalf("Failed to list projects: %v", err)
		}
		projects = append(projects, projectList...)

	}

	// Initialize DuckDB connection
	db, err := sql.Open("duckdb", "results.duckdb")
	if err != nil {
		log.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer db.Close()

	// Create table in DuckDB
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS samples (table_name TEXT, message TEXT)")
	if err != nil {
		log.Fatalf("Failed to create table samples in DuckDB: %v", err)
	}
	// Create a sequence to generate unique IDs for each row
	_, err = db.Exec("CREATE SEQUENCE IF NOT EXISTS queries_id_seq START 1")
	if err != nil {
		log.Fatalf("Failed to create sequence queries_id_seq in DuckDB: %v", err)
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS queries (id INTEGER DEFAULT nextval('queries_id_seq'), creation_time TIMESTAMP, user_email TEXT, total_bytes_processed BIGINT, query TEXT, referenced_tables STRUCT(project_id TEXT, dataset_id TEXT, tables TEXT)[])")
	if err != nil {
		log.Fatalf("Failed to create table queries in DuckDB: %v", err)
	}
	// Create a table to store the size of each table in each dataset in each project
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS table_sizes (project_id TEXT, table_name TEXT, active_logical_bytes BIGINT, long_term_logical_bytes BIGINT, active_physical_bytes BIGINT, long_term_physical_bytes BIGINT, time_travel_physical_bytes BIGINT, fail_safe_physical_bytes BIGINT)")
	if err != nil {
		log.Fatalf("Failed to create table table_sizes in DuckDB: %v", err)
	}
	// Create a table to store the amount of data ingested into each table in each dataset in each project by month
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS table_ingestion (project_id TEXT, dataset_id TEXT, table_id TEXT, month DATE, bytes_ingested BIGINT)")
	if err != nil {
		log.Fatalf("Failed to create table table_ingestion in DuckDB: %v", err)
	}

	switch *mode {
	case "table_samples":
		fmt.Println("Connecting to BigQuery")
		client, err := bigquery.NewClient(ctx, *projectID)
		if err != nil {
			log.Fatalf("Failed to create BigQuery client: %v", err)
		}

		tables, err := listTables(ctx, client, *datasetID)
		if err != nil {
			log.Fatalf("Failed to list tables: %v", err)
		}

		for _, table := range tables {
			if err := downloadTableRows(ctx, client, *datasetID, table, db, sampleRate); err != nil {
				log.Printf("Failed to download data for table %s: %v", table, err)
			}
		}
	case "query_list":
		// Iterate over each project and download the query list
		for _, project := range projects {
			fmt.Println("Connecting to BigQuery project: ", project)
			client, err := bigquery.NewClient(ctx, project)
			if err != nil {
				log.Fatalf("Failed to create BigQuery client: %v", err)
			}
			if err := downloadQueryList(ctx, client, db); err != nil {
				log.Printf("Failed to download query list: %v", err)
			}
		}
	case "table_sizes":
		// Create a buffered channel to limit concurrency to 10
		sem := make(chan struct{}, *concurrency)
		var wg sync.WaitGroup

		// Iterate over each project and download the current table sizes concurrently
		for _, project := range projects {
			wg.Add(1)
			go func(project string) {
				defer wg.Done()
				sem <- struct{}{}        // Acquire a token
				defer func() { <-sem }() // Release the token

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

		// Wait for all goroutines to complete
		wg.Wait()
	case "table_ingest":
		// Create a buffered channel to limit concurrency to 10
		sem := make(chan struct{}, *concurrency)
		var wg sync.WaitGroup

		// Iterate over each project and download the amount of data ingested into each table by month concurrently
		for _, project := range projects {
			wg.Add(1)
			go func(project string) {
				defer wg.Done()
				sem <- struct{}{}        // Acquire a token
				defer func() { <-sem }() // Release the token

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

		// Wait for all goroutines to complete
		wg.Wait()
	}
}

func listProjects(ctx context.Context) ([]string, error) {
	fmt.Println("Listing projects")
	var projects []string
	client, err := resourcemanager.NewProjectsClient(ctx)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	// Specify the parent resource name
	parent := "folders/359975919687" // Replace with your organization ID or folder ID

	req := &resourcemanagerpb.ListProjectsRequest{
		Parent: parent,
	}
	it := client.ListProjects(ctx, req)
	for {
		project, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		projects = append(projects, project.ProjectId)
	}
	// Sort the projects
	sort.Strings(projects)
	return projects, nil
}

func listDatasets(ctx context.Context, client *bigquery.Client) ([]string, error) {
	fmt.Println("Listing datasets in project")
	var datasets []string
	it := client.Datasets(ctx)
	for {
		dataset, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		datasets = append(datasets, dataset.DatasetID)
	}
	return datasets, nil
}

func listTables(ctx context.Context, client *bigquery.Client, datasetID string) ([]string, error) {
	fmt.Println("Listing tables in dataset")
	var tables []string
	it := client.Dataset(datasetID).Tables(ctx)
	for {
		table, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		tables = append(tables, table.TableID)
	}
	return tables, nil
}

func downloadTableSizes(ctx context.Context, client *bigquery.Client, db *sql.DB) error {
	fmt.Println("Downloading table sizes")
	query := `
		SELECT 
			project_id,
			table_name,
			sum(active_logical_bytes) as active_logical_bytes,
			sum(long_term_logical_bytes) as long_term_logical_bytes,
			sum(active_physical_bytes) as active_physical_bytes,
			sum(long_term_physical_bytes) as long_term_physical_bytes,
			sum(time_travel_physical_bytes) as time_travel_physical_bytes,
			sum(fail_safe_physical_bytes) as fail_safe_physical_bytes
		FROM 
			region-us.INFORMATION_SCHEMA.TABLE_STORAGE
		WHERE total_logical_bytes > 0
		AND table_schema != 'expired_tables'
		GROUP BY project_id, table_name
	`
	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Saving table sizes to DuckDB")
	batch := make([][]bigquery.Value, 0, 1000)
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		batch = append(batch, values)

		if len(batch) >= 1000 {
			if err := saveTableSizesBatchToDuckDB(db, batch); err != nil {
				return err
			}
			batch = batch[:0] // Reset the batch
		}
	}

	// Save any remaining records
	if len(batch) > 0 {
		if err := saveTableSizesBatchToDuckDB(db, batch); err != nil {
			return err
		}
	}

	return nil
}

func downloadTableIngestion(ctx context.Context, client *bigquery.Client, db *sql.DB) error {
	fmt.Println("Downloading table ingestion data")
	query := `
		SELECT
			project_id,
			dataset_id,
			table_id,
			DATE_TRUNC(creation_time, MONTH) AS month,
			SUM(total_bytes_processed) AS bytes_ingested
		FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
		WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 YEAR)
		AND job_type = "QUERY"
		GROUP BY project_id, dataset_id, table_id, month
	`
	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Saving table ingestion data to DuckDB")
	batch := make([][]bigquery.Value, 0, 1000)
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		batch = append(batch, values)

		if len(batch) >= 1000 {
			if err := saveTableIngestionBatchToDuckDB(db, batch); err != nil {
				return err
			}
			batch = batch[:0] // Reset the batch
		}
	}

	// Save any remaining records
	if len(batch) > 0 {
		if err := saveTableIngestionBatchToDuckDB(db, batch); err != nil {
			return err
		}
	}

	return nil
}

func downloadTableRows(ctx context.Context, client *bigquery.Client, datasetID, tableID string, db *sql.DB, sampleRate *float64) error {
	rowCount, err := countRows(ctx, client, datasetID, tableID)
	if err != nil {
		return err
	}
	if rowCount == 0 {
		fmt.Printf("No data found for table %s\n", tableID)
		return nil
	}
	fmt.Printf("Table %s has %d rows\n", tableID, rowCount)

	// Calculate 1% of the number of rows, rounded up to the nearest integer
	sampleSize := int(math.Ceil(float64(rowCount) * *sampleRate))

	fmt.Printf("Downloading %d rows for table %s\n", sampleSize, tableID)
	query := fmt.Sprintf("SELECT message FROM `%s.%s.%s` LIMIT %d", client.Project(), datasetID, tableID, sampleSize)
	q := client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Saving data to DuckDB")
	var batch []string
	var count int
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		message := fmt.Sprintf("%v", values)
		batch = append(batch, message)

		if len(batch) >= 1000 {
			// Print the number of records saved. This line will be overwritten by the next batch
			count += len(batch)
			fmt.Printf("\rSaved %d records", count)
			if err := saveSamplesBatchToDuckDB(db, tableID, batch); err != nil {
				return err
			}
			batch = batch[:0] // Reset the batch
		}
	}

	// Save any remaining records
	if len(batch) > 0 {
		count += len(batch)
		fmt.Printf("\rSaved %d records", count)
		if err := saveSamplesBatchToDuckDB(db, tableID, batch); err != nil {
			return err
		}
	}

	return nil
}

func downloadQueryList(ctx context.Context, client *bigquery.Client, db *sql.DB) error {
	query := `
        SELECT
			creation_time,
            user_email,
            total_bytes_processed,
            query,
            referenced_tables
        FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 360 DAY) AND CURRENT_TIMESTAMP()
        AND job_type = "QUERY"
        ORDER BY total_bytes_processed DESC
    `
	q := client.Query(query)

	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Saving query list to DuckDB")
	batch := make([][]bigquery.Value, 0, 1000)
	for {
		var values []bigquery.Value
		var tableRefs []map[string]string
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		// Convert the referenced tables to a slice of maps
		for i := 0; i < len(values[4].([]bigquery.Value)); i += 1 {
			tableRefs = append(tableRefs, map[string]string{
				"project_id": values[4].([]bigquery.Value)[i].([]bigquery.Value)[0].(string),
				"dataset_id": values[4].([]bigquery.Value)[i].([]bigquery.Value)[1].(string),
				"tables":     values[4].([]bigquery.Value)[i].([]bigquery.Value)[2].(string),
			})
		}
		// Serialize the referenced tables to a JSON string
		tableRefsJSON, err := json.MarshalIndent(tableRefs, "", "   ")
		if err != nil {
			return err
		}
		tableRefsString := string(tableRefsJSON)
		if tableRefsString == "null" {
			// Skip this record
			continue
		}
		values[4] = tableRefsString

		// Replace the \n in the query with a space
		values[3] = strings.Replace(values[3].(string), "\n", " ", -1)
		batch = append(batch, values)

		if len(batch) >= 1000 {
			if err := saveQueryBatchToDuckDB(db, batch); err != nil {
				return err
			}
			batch = batch[:0] // Reset the batch
		}
	}

	// Save any remaining records
	if len(batch) > 0 {
		if err := saveQueryBatchToDuckDB(db, batch); err != nil {
			return err
		}
	}

	return nil
}

func countRows(ctx context.Context, client *bigquery.Client, datasetID string, tableID string) (int, error) {
	fmt.Println("Counting rows in the last 24 hours")
	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM `%s.%s.%s` WHERE message IS NOT NULL AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)",
		client.Project(),
		datasetID,
		tableID,
	)
	q := client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return 0, err
	}
	// Return the number of rows
	var count int
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return 0, err
		}
		count = int(values[0].(int64))
	}
	return count, nil
}

func saveToDuckDB(db *sql.DB, tableName, message string) error {
	_, err := db.Exec("INSERT INTO results (table_name, message) VALUES (?, ?)", tableName, message)
	return err
}

func saveSamplesBatchToDuckDB(db *sql.DB, tableName string, batch []string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO results (table_name, message) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, message := range batch {
		if _, err := stmt.Exec(tableName, message); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func saveQueryBatchToDuckDB(db *sql.DB, batch [][]bigquery.Value) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO queries (creation_time, user_email, total_bytes_processed, query, referenced_tables) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range batch {
		if _, err := stmt.Exec(row[0], row[1], row[2], row[3], row[4]); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func saveTableSizesBatchToDuckDB(db *sql.DB, batch [][]bigquery.Value) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO table_sizes (project_id, table_name, active_logical_bytes, long_term_logical_bytes, active_physical_bytes, long_term_physical_bytes, time_travel_physical_bytes, fail_safe_physical_bytes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range batch {
		if _, err := stmt.Exec(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func saveTableIngestionBatchToDuckDB(db *sql.DB, batch [][]bigquery.Value) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("INSERT INTO table_ingestion (project_id, dataset_id, table_id, month, bytes_ingested) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, row := range batch {
		if _, err := stmt.Exec(row[0], row[1], row[2], row[3], row[4]); err != nil {
			return err
		}
	}

	return tx.Commit()
}
