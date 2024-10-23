package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"

	"cloud.google.com/go/bigquery"
	_ "github.com/marcboeker/go-duckdb"
	"google.golang.org/api/iterator"
)

func main() {
	projectID := flag.String("project", "", "Google Cloud Project ID")
	datasetID := flag.String("dataset", "", "BigQuery Dataset ID")
	flag.Parse()

	if *projectID == "" || *datasetID == "" {
		log.Fatalf("Both project and dataset flags are required")
	}

	ctx := context.Background()
	fmt.Println("Connecting to BigQuery")
	client, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Initialize DuckDB connection
	db, err := sql.Open("duckdb", "results.duckdb")
	if err != nil {
		log.Fatalf("Failed to connect to DuckDB: %v", err)
	}
	defer db.Close()

	// Create table in DuckDB
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS results (table_name TEXT, message TEXT)")
	if err != nil {
		log.Fatalf("Failed to create table in DuckDB: %v", err)
	}

	tables, err := listTables(ctx, client, *datasetID)
	if err != nil {
		log.Fatalf("Failed to list tables: %v", err)
	}

	for _, table := range tables {
		if err := downloadTableData(ctx, client, *datasetID, table, db); err != nil {
			log.Printf("Failed to download data for table %s: %v", table, err)
		}
	}
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

func downloadTableData(ctx context.Context, client *bigquery.Client, datasetID, tableID string, db *sql.DB) error {
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
	sampleSize := int(math.Ceil(float64(rowCount) * 0.01))

	fmt.Printf("Downloading %d rows for table %s\n", sampleSize, tableID)
	query := fmt.Sprintf("SELECT message FROM `%s.%s.%s` LIMIT %d", client.Project(), datasetID, tableID, sampleSize)
	q := client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return err
	}

	fmt.Println("Saving data to DuckDB")
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
		if err := saveToDuckDB(db, tableID, message); err != nil {
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
