package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"

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
	fmt.Printf("Downloading data for table %s\n", tableID)
	query := fmt.Sprintf("SELECT message FROM `%s.%s.%s` LIMIT 100000", client.Project(), datasetID, tableID)
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

func saveToDuckDB(db *sql.DB, tableName, message string) error {
	_, err := db.Exec("INSERT INTO results (table_name, message) VALUES (?, ?)", tableName, message)
	return err
}
