package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
)

// LogLine represents a log line with its table and tokens
type LogLine struct {
	Table   string
	Message string
	Tokens  []string
}

// SimilarLine represents a line with a common pattern and its count
type SimilarLine struct {
	Pattern string
	Count   int
}

func handleProcessLogs(db *sql.DB, threshold float64) {
	rows, err := db.Query("SELECT table_name, message FROM samples")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	logs := make(map[string]map[int][]LogLine)
	for rows.Next() {
		var table, message string
		if err := rows.Scan(&table, &message); err != nil {
			log.Fatal(err)
		}

		tokens := Tokenize(message)
		tokenCount := len(tokens)
		if logs[table] == nil {
			logs[table] = make(map[int][]LogLine)
		}
		logs[table][tokenCount] = append(logs[table][tokenCount], LogLine{Table: table, Message: message, Tokens: tokens})
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make(chan SimilarLine, 100)
	batchSize := 10000
	batchJobs := make(chan []SimilarLine, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go insertWorker(db, batchJobs, &wg)
	}

	go func() {
		var batch []SimilarLine
		for result := range results {
			batch = append(batch, result)
			if len(batch) >= batchSize {
				batchJobs <- batch
				batch = []SimilarLine{}
			}
		}
		if len(batch) > 0 {
			batchJobs <- batch
		}
		close(batchJobs)
	}()

	for _, groups := range logs {
		for _, group := range groups {
			wg.Add(1)
			go FindSimilarLogs(group, threshold, &wg, results)
		}
	}

	wg.Wait()
	close(results)
	wg.Add(5)
	wg.Wait()

	fmt.Println("Similar log patterns have been stored in results.db")
}

// Tokenize splits a log line into tokens
func Tokenize(line string) []string {
	return strings.Fields(line)
}

// CalculateSimilarity calculates the similarity between two slices of tokens and returns the masked pattern
func CalculateSimilarity(tokens1, tokens2 []string) (float64, string) {
	minLen := min(len(tokens1), len(tokens2))
	matches := 0
	pattern := make([]string, max(len(tokens1), len(tokens2)))

	for i := 0; i < minLen; i++ {
		if tokens1[i] == tokens2[i] {
			matches++
			pattern[i] = tokens1[i]
		} else {
			pattern[i] = "?"
		}
	}

	for i := minLen; i < len(tokens1); i++ {
		pattern[i] = "?"
	}

	for i := minLen; i < len(tokens2); i++ {
		pattern[i] = "?"
	}

	return float64(matches) / float64(max(len(tokens1), len(tokens2))), strings.Join(pattern, " ")
}

// FindSimilarLogs finds similar log lines within a group
func FindSimilarLogs(logs []LogLine, threshold float64, wg *sync.WaitGroup, results chan<- SimilarLine) {
	defer wg.Done()
	patternCount := make(map[string]int)

	for i := 0; i < len(logs); i++ {
		for j := i + 1; j < len(logs); j++ {
			similarity, pattern := CalculateSimilarity(logs[i].Tokens, logs[j].Tokens)
			if similarity > threshold {
				patternCount[pattern]++
			}
		}
	}

	for pattern, count := range patternCount {
		results <- SimilarLine{Pattern: pattern, Count: count}
	}
}
