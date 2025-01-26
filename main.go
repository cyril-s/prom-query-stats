package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
)

var (
	argFile = flag.String("f", "-", "path to query log file. Pass '-' to read from stdin")
)

type LogEntry struct {
	Params struct {
		Query string `json:"query"`
		Start string `json:"start"`
		End   string `json:"end"`
		Step  int	 `json:"step"`
	} `json:"params"`
	Stats struct {
		Timings struct {
			EvalTotalTime		 float64 `json:"evalTotalTime"`
			ExecQueueTime		 float64 `json:"execQueueTime"`
			ExecTotalTime		 float64 `json:"execTotalTime"`
			InnerEvalTime		 float64 `json:"innerEvalTime"`
			QueryPreparationTime float64 `json:"queryPreparationTime"`
			ResultSortTime		 float64 `json:"resultSortTime"`
		} `json:"timings"`
		Samples struct {
			TotalQueryableSamples int `json:"totalQueryableSamples"`
			PeakSamples           int `json:"peakSamples"`
		} `json:"samples,omitempty"`
	} `json:"stats"`
	TS string `json:"ts"`
}

func avg[T int | float64](nums []T) float64 {
	var sum T
	for _, num := range nums {
		sum += num
	}
	return float64(sum) / float64(len(nums))
}

type Query struct {
	Query string
	Logs []LogEntry
	AvgExecTotalTime float64
	AvgTotalQueryableSamples float64
	AvgPeakSamples float64
}

func NewQuery(query string, logs []LogEntry) (*Query, error) {
	if query == "" {
		return nil, fmt.Errorf("a query cannot be empty")
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("a number of log entries must be greater than zero")
	}

	execTotalTimeVals := make([]float64, 0, len(logs))
	totalQueryableSamplesVals := make([]int, 0, len(logs))
	peakSamplesVals := make([]int, 0, len(logs))
	for _, log := range logs {
		execTotalTimeVals = append(execTotalTimeVals, log.Stats.Timings.ExecTotalTime)
		totalQueryableSamplesVals = append(totalQueryableSamplesVals, log.Stats.Samples.TotalQueryableSamples)
		peakSamplesVals = append(peakSamplesVals, log.Stats.Samples.PeakSamples)
	}

	q := Query{
		query,
		logs,
		avg(execTotalTimeVals),
		avg(totalQueryableSamplesVals),
		avg(peakSamplesVals),
	}
	return &q, nil
}

type Queries []*Query

func (q Queries) Len() int { return len(q) }
func (q Queries) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

type ByAvgExecTotalTime struct {Queries}

func (q ByAvgExecTotalTime) Less(i, j int) bool {
	return q.Queries[i].AvgExecTotalTime < q.Queries[j].AvgExecTotalTime
}

type ByAvgTotalQueryableSamples struct {Queries}

func (q ByAvgTotalQueryableSamples) Less(i, j int) bool {
	return q.Queries[i].AvgTotalQueryableSamples < q.Queries[j].AvgTotalQueryableSamples
}

type ByAvgPeakSamples struct {Queries}

func (q ByAvgPeakSamples) Less(i, j int) bool {
	return q.Queries[i].AvgPeakSamples < q.Queries[j].AvgPeakSamples
}

func loadQueriesFromLog(file *os.File) ([]*Query, error) {
	qMap := make(map[string][]LogEntry)
	scanner := bufio.NewScanner(file)
	for lineNum := 0; scanner.Scan(); lineNum++ {
		line := scanner.Bytes()
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("Failed to parse line %d: %w", lineNum, err)
		}
		if entry.Params.Query == "" {
			log.Printf("Failed to parse line %d: invalid query \"%s\"", lineNum, entry.Params.Query)
			continue
		}
		qMap[entry.Params.Query] = append(qMap[entry.Params.Query], entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	queries := make([]*Query, 0, len(qMap))
	for query, logs := range qMap {
		if q, err := NewQuery(query, logs); err != nil {
			return nil, fmt.Errorf("Failed to create Query: %w", err)
		} else {
			queries = append(queries, q)
		}
	}

	return queries, nil
}

func main() {
	flag.Parse()

	input := os.Stdin
	if *argFile != "-" {
		log.Printf("Reading the query log from %s", *argFile)
		var err error
		input, err = os.Open(*argFile)
		if err != nil {
			log.Fatalf("Failed to read the query log file: %s", err)
		}
		defer input.Close()
	} else {
		log.Print("Reading the query log from stdin")
	}

	queries, err := loadQueriesFromLog(input)
	if err != nil {
		log.Fatalf("Failed to parse the query log file: %s", err)
	}

	top := 10
	if top > len(queries) {
		top = len(queries)
	}

	sort.Sort(sort.Reverse(ByAvgExecTotalTime{queries}))
	fmt.Printf("Top %d queries by average execution time:\n", top)
	for i, query := range queries[:top] {
		fmt.Printf(
			"[%2d] n: %-6d %.3fs %s\n",
			i+1,
			len(query.Logs),
			query.AvgExecTotalTime,
			query.Query,
		)
	}

	sort.Sort(sort.Reverse(ByAvgTotalQueryableSamples{queries}))
	fmt.Println()
	fmt.Printf("Top %d queries by average total queryable samples:\n", top)
	for i, query := range queries[:top] {
		fmt.Printf(
			"[%2d] n: %-6d %.3f %s\n",
			i+1,
			len(query.Logs),
			query.AvgTotalQueryableSamples,
			query.Query,
		)
	}

	sort.Sort(sort.Reverse(ByAvgPeakSamples{queries}))
	fmt.Println()
	fmt.Printf("Top %d queries by average peak samples:\n", top)
	for i, query := range queries[:top] {
		fmt.Printf(
			"[%2d] n: %-6d %.3f %s\n",
			i+1,
			len(query.Logs),
			query.AvgPeakSamples,
			query.Query,
		)
	}
}
