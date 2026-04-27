package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/common"
	"github.com/mcudby/mwat/components/datawarehouse"
	enginecommon "github.com/mcudby/mwat/components/replayengine/common"
)

func main() {
	ctx := context.Background()

	_dir := flag.String("storagedir", "", "directory in which to store fetched data")
	_date := flag.String("date", "", "target date")
	_symbol := flag.String("symbol", "", "target stock market symbol")

	flag.Parse()

	dir := *_dir
	if !common.DirExists(dir) {
		log.Fatalf("specified directory does not exist")
	}

	date, err := civil.ParseDate(*_date)
	if err != nil {
		log.Fatalf("failed to parse date: %s", err)
	}

	symbol := enginecommon.Symbol(*_symbol)

	// Initialise warehouse
	warehouse := datawarehouse.NewDataWarehouse(dir)

	// Fetch and store the data
	path, err := warehouse.FetchAndStoreSeries(ctx, date, symbol)
	if err != nil {
		log.Fatalf("error: %s", err)
	}
	fmt.Printf("written fetched data to '%s'\n", path)
}
