### Random Notes
```
Hub
    - DataCoordinator
    - Clock
    - []Client
    - []TickerThread

DataCoordinator
```

### OHLC bar insertion

Most likely need to use a: https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool

pgxpool is a concurrency-safe connection pool for pgc.

pgxpool implements a nearly identical interAface to pgc connections.


**pgx CopyFrom**

Use CopyFrom to efficiently insert multople rows at a time using ...

Accepts a CopyFromSource interface.

If the data is already in a [][]any use CopyFromRows to wrap it in a CopyFromSource interface. Or implement CopyFromSource to avoid buffering the entire data set in memory.



