# Data Warehouse

A small client that allows for the fetching of data from the Massive API
and its storage as a Golang Binary (.gob) in a specified location.

Created s.t. data can be persisted and used as test data.

Example use from project root:
```
go run components/datawarehouse/cmd/main.go -symbol QQQ -date 2025-03-20 -storagedir components/replayengine_test/testdata
```