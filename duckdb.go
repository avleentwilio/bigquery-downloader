package main

import (
	"database/sql"
	"log"
	"sync"

	"cloud.google.com/go/bigquery"
)

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

func insertWorker(db *sql.DB, jobs <-chan []SimilarLine, wg *sync.WaitGroup) {
	defer wg.Done()
	for batch := range jobs {
		insertBatch(db, batch)
	}
}

func insertBatch(db *sql.DB, batch []SimilarLine) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare("INSERT INTO similar_lines (pattern, count) VALUES (?, ?)")
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for _, line := range batch {
		_, err := stmt.Exec(line.Pattern, line.Count)
		if err != nil {
			log.Fatal(err)
		}
	}
	tx.Commit()
}

func initializeDuckDB(db *sql.DB) {
	// Create table in DuckDB
	_, err := db.Exec("CREATE TABLE IF NOT EXISTS samples (table_name TEXT, message TEXT)")
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
	// Create a table to store similar log lines
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS similar_lines (pattern TEXT, count INTEGER)")
	if err != nil {
		log.Fatalf("Failed to create table similar_lines in DuckDB: %v", err)
	}
}
