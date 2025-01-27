# prom-query-stats

__prom-query-stats__ is a tool for analyzing [Prometheus query logs](https://prometheus.io/docs/guides/query-log/).

## Installation
```bash
go install github.com/cyril-s/prom-query-stats@latest
```

## Usage
```
Usage of prom-query-stats:
  -f string
    	path to the query log file. Pass '-' to read from stdin (default "-")
  -from value
    	load log entries afer this time. Accepts RFC3339 format, e.g. 2025-01-27T17:21:44Z
  -to value
    	load log entries until this time. Accepts RFC3339 format, e.g. 2025-01-27T17:21:44Z
  -top int
    	number of queries to display (default 10)
  -version
    	show version
```
