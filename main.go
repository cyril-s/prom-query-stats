package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"time"
)

type timeFlag struct {
	*time.Time
}

func (t *timeFlag) String() string {
	if t == nil || t.Time == nil {
		return ""
	}
	return t.Time.Format(time.RFC3339)
}

func (t *timeFlag) Set(value string) error {
	time, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return err
	}
	t.Time = &time
	return nil
}

var (
	now = time.Now()
	argFile = flag.String("f", "-", "path to the query log file. Pass '-' to read from stdin")
	argFrom timeFlag
	argTo timeFlag
	argTop = flag.Int("top", 10, "number of queries to display")
	argVer = flag.Bool("version", false, "show version")
)

func init() {
	flag.Var(&argFrom, "from", "load log entries afer this time. Accepts RFC3339 format, e.g. " + now.UTC().Format(time.RFC3339))
	flag.Var(&argTo, "to", "load log entries until this time. Accepts RFC3339 format, e.g. " + now.UTC().Format(time.RFC3339))
}

type LogEntry struct {
	Params struct {
		Query string `json:"query"`
		Start *time.Time `json:"start"`
		End   *time.Time `json:"end"`
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
	RuleGroup *struct {
		Name string `json:"name,omitempty"`
		File string `json:"file,omitempty"`
	} `json:"ruleGroup,omitempty"`
	TS *time.Time `json:"ts"`
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
	MaxExecTotalTimeEntry *LogEntry
	MaxTotalQueryableSamplesEntry *LogEntry
	MaxPeakSamplesEntry *LogEntry
}

func NewQuery(query string, logs []LogEntry) (*Query, error) {
	if query == "" {
		return nil, fmt.Errorf("a query cannot be empty")
	}
	if len(logs) == 0 {
		return nil, fmt.Errorf("a number of log entries must be greater than zero")
	}

	maxExecTotalTimeEntry := &logs[0]
	maxTotalQueryableSamplesEntry := &logs[0]
	maxPeakSamplesEntry := &logs[0]
	execTotalTimeVals := make([]float64, 0, len(logs))
	totalQueryableSamplesVals := make([]int, 0, len(logs))
	peakSamplesVals := make([]int, 0, len(logs))
	for _, log := range logs {
		execTotalTimeVals = append(execTotalTimeVals, log.Stats.Timings.ExecTotalTime)
		totalQueryableSamplesVals = append(totalQueryableSamplesVals, log.Stats.Samples.TotalQueryableSamples)
		peakSamplesVals = append(peakSamplesVals, log.Stats.Samples.PeakSamples)
		if log.Stats.Timings.ExecTotalTime > maxExecTotalTimeEntry.Stats.Timings.ExecTotalTime {
			maxExecTotalTimeEntry = &log
		}
		if log.Stats.Samples.TotalQueryableSamples > maxTotalQueryableSamplesEntry.Stats.Samples.TotalQueryableSamples {
			maxTotalQueryableSamplesEntry = &log
		}
		if log.Stats.Samples.PeakSamples > maxPeakSamplesEntry.Stats.Samples.PeakSamples {
			maxPeakSamplesEntry = &log
		}
	}

	q := Query{
		query,
		logs,
		avg(execTotalTimeVals),
		avg(totalQueryableSamplesVals),
		avg(peakSamplesVals),
		maxExecTotalTimeEntry,
		maxTotalQueryableSamplesEntry,
		maxPeakSamplesEntry,
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

type ByMaxExecTotalTime struct {Queries}

func (q ByMaxExecTotalTime) Less(i, j int) bool {
	return q.Queries[i].MaxExecTotalTimeEntry.Stats.Timings.ExecTotalTime < q.Queries[j].MaxExecTotalTimeEntry.Stats.Timings.ExecTotalTime
}

type ByAvgTotalQueryableSamples struct {Queries}

func (q ByAvgTotalQueryableSamples) Less(i, j int) bool {
	return q.Queries[i].AvgTotalQueryableSamples < q.Queries[j].AvgTotalQueryableSamples
}

type ByMaxTotalQueryableSamples struct {Queries}

func (q ByMaxTotalQueryableSamples) Less(i, j int) bool {
	return q.Queries[i].MaxTotalQueryableSamplesEntry.Stats.Samples.TotalQueryableSamples < q.Queries[j].MaxTotalQueryableSamplesEntry.Stats.Samples.TotalQueryableSamples
}

type ByAvgPeakSamples struct {Queries}

func (q ByAvgPeakSamples) Less(i, j int) bool {
	return q.Queries[i].AvgPeakSamples < q.Queries[j].AvgPeakSamples
}

type ByMaxPeakSamples struct {Queries}

func (q ByMaxPeakSamples) Less(i, j int) bool {
	return q.Queries[i].MaxPeakSamplesEntry.Stats.Samples.PeakSamples < q.Queries[j].MaxPeakSamplesEntry.Stats.Samples.PeakSamples
}

type LoadStats struct {
	Num int
	From time.Time
	To time.Time
}

func LoadQueriesFromLog(file *os.File, from *time.Time, to *time.Time) ([]*Query, LoadStats, error) {
	qMap := make(map[string][]LogEntry)
	stats := LoadStats{0, time.Now(), time.Time{}}
	scanner := bufio.NewScanner(file)
	for lineNum := 0; scanner.Scan(); lineNum++ {
		line := scanner.Bytes()
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, stats, fmt.Errorf("Failed to parse line %d: %w", lineNum, err)
		}
		if entry.Params.Query == "" {
			log.Printf("Failed to parse line %d: empty query", lineNum)
			continue
		}
		if from != nil && entry.TS.Before(*from) {
			continue
		}
		if to != nil && entry.TS.After(*to) {
			continue
		}

		qMap[entry.Params.Query] = append(qMap[entry.Params.Query], entry)

		stats.Num++
		if entry.TS.Before(stats.From) {
			stats.From = *entry.TS
		}
		if entry.TS.After(stats.To) {
			stats.To = *entry.TS
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, stats, err
	}

	queries := make([]*Query, 0, len(qMap))
	for query, logs := range qMap {
		if q, err := NewQuery(query, logs); err != nil {
			return nil, stats, fmt.Errorf("Failed to create Query: %w", err)
		} else {
			queries = append(queries, q)
		}
	}

	return queries, stats, nil
}

func removeNL(str string) string {
	re := regexp.MustCompile(`\n\s*`)
	return re.ReplaceAllString(str, "")
}

func main() {
	flag.Parse()

	if *argVer {
		if buildInfo, ok := debug.ReadBuildInfo(); ok {
			fmt.Println(buildInfo.Main.Version)
			fmt.Println(buildInfo)
			os.Exit(0)
		} else {
			fmt.Println("Failed to get build info")
			os.Exit(13)
		}
	}

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

	queries, loadStats, err := LoadQueriesFromLog(input, argFrom.Time, argTo.Time)
	if err != nil {
		log.Fatalf("Failed to parse the query log file: %s", err)
	}
	if len(queries) == 0 {
		log.Fatalln("Loaded 0 queries")
	}
	log.Printf("Loaded %d entries from [%v] to [%v]", loadStats.Num, loadStats.From, loadStats.To)

	if *argTop > len(queries) {
		*argTop = len(queries)
	}

	printAvgTable := func (title, unit string, getter func(q *Query) float64) {
		fmt.Printf("\nTop %d queries by %s:\n", *argTop, title)
		for i, query := range queries[:*argTop] {
			fmt.Printf(
				"%2d) n=%-6d %.3f%s %s",
				i+1,
				len(query.Logs),
				getter(query),
				unit,
				removeNL(query.Query),
			)
			if query.Logs[0].RuleGroup != nil {
				fmt.Printf(" | ruleName=\"%s\"", query.Logs[0].RuleGroup.Name)
			}
			fmt.Println()
		}
	}

	printMaxTable := func (title, unit string, valueGetter func(q *Query) interface{}, tsGetter func(q *Query) *time.Time) {
		fmt.Printf("\nTop %d queries by %s:\n", *argTop, title)
		for i, query := range queries[:*argTop] {
			valueOut := ""
			switch value := valueGetter(query).(type) {
			case int:
				valueOut = fmt.Sprintf("%d", value)
			case float64:
				valueOut = fmt.Sprintf("%.3f", value)
			default:
				panic("unsupported type")
			}
			fmt.Printf(
				"%2d) t=%s %s%s %s",
				i+1,
				tsGetter(query).Format(time.RFC3339),
				valueOut,
				unit,
				removeNL(query.Query),
			)
			if query.Logs[0].RuleGroup != nil {
				fmt.Printf(" | ruleName=\"%s\"", query.Logs[0].RuleGroup.Name)
			}
			fmt.Println()
		}
	}

	sort.Sort(sort.Reverse(ByAvgExecTotalTime{queries}))
	printAvgTable("average execution time", "s", func(q *Query) float64 { return q.AvgExecTotalTime })

	sort.Sort(sort.Reverse(ByMaxExecTotalTime{queries}))
	printMaxTable("max execution time", "s", func(q *Query) interface{} { return q.MaxExecTotalTimeEntry.Stats.Timings.ExecTotalTime }, func(q *Query) *time.Time { return q.MaxExecTotalTimeEntry.TS })

	sort.Sort(sort.Reverse(ByAvgTotalQueryableSamples{queries}))
	printAvgTable("average total queryable samples", "", func(q *Query) float64 { return q.AvgTotalQueryableSamples })

	sort.Sort(sort.Reverse(ByMaxTotalQueryableSamples{queries}))
	printMaxTable("max total queryable samples", "", func(q *Query) interface{} { return q.MaxTotalQueryableSamplesEntry.Stats.Samples.TotalQueryableSamples }, func(q *Query) *time.Time { return q.MaxTotalQueryableSamplesEntry.TS })

	sort.Sort(sort.Reverse(ByAvgPeakSamples{queries}))
	printAvgTable("average peak samples", "", func(q *Query) float64 { return q.AvgPeakSamples })

	sort.Sort(sort.Reverse(ByMaxPeakSamples{queries}))
	printMaxTable("max peak samples", "", func(q *Query) interface{} { return q.MaxPeakSamplesEntry.Stats.Samples.PeakSamples }, func(q *Query) *time.Time { return q.MaxPeakSamplesEntry.TS })
}
