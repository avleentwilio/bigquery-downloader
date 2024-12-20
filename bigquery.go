package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"cloud.google.com/go/bigquery"
	resourcemanager "cloud.google.com/go/resourcemanager/apiv3"
	resourcemanagerpb "cloud.google.com/go/resourcemanager/apiv3/resourcemanagerpb"
	"google.golang.org/api/iterator"
)

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
