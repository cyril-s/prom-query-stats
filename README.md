# prom-query-stats

__prom-query-stats__ is a tool for analyzing [Prometheus query logs](https://prometheus.io/docs/guides/query-log/).

## Installation
```bash
go install github.com/cyril-s/prom-query-stats@latest
```

## Usage
```
Usage of ./prom-query-stats:
  -f string
    	path to the query log file. Pass '-' to read from stdin (default "-")
  -from value
    	load log entries afer this time. Accepts RFC3339 format, e.g. 2025-01-30T22:09:27Z
  -p int
    	percentile rank (default 95)
  -to value
    	load log entries until this time. Accepts RFC3339 format, e.g. 2025-01-30T22:09:27Z
  -top int
    	number of top queries to display (default 10)
  -version
    	show version
```
