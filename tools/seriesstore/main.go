package main

import (
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/common"

	"github.com/mcudby/mwat/components/replayengine/hydrationmanager/massiveclient"
	enginecommon "github.com/mcudby/mwat/components/replayengine/common"
)

func fetchSeries(date civil.Date, symbol string) ([]enginecommon.Bar, error) {
	ctx := context.Background()

	client := massiveclient.NewMassiveClient()

	series, err := client.FetchTradingSession(ctx, date, symbol)
	if err != nil {
		return []enginecommon.Bar{}, err
	}
	return []enginecommon.Bar(series), nil
}

func storeSeries(path string, series []enginecommon.Bar) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to open file at `%s`: %w", path, err)
	}

	encoder := gob.NewEncoder(f)
	err = encoder.Encode(series)
	if err != nil {
		return fmt.Errorf("failed to write series: %w", err)
	}

	return nil
}

func fmtFileName(date civil.Date, symbol string) string {
	return fmt.Sprintf("%s_%s.gob", date.String(), symbol)
}

func fail(err error) {
	log.Fatalf("%s", err)
}

func main() {

	_dir := flag.String("storagedir", "", "directory in which to store fetched data")
	_date := flag.String("date", "", "target date")
	_symbol := flag.String("symbol", "", "target stock market symbol")

	flag.Parse()

	dir := *_dir
	date, err := civil.ParseDate(*_date)
	symbol := string(*_symbol)

	// basic validaton
	if !common.DirExists(dir) {
		log.Fatalf("specified directory does not exist")
	}
	if err != nil {
		log.Fatalf("failed to parse date: %s", err)
	}

	// check if file already exists
	path := filepath.Join(dir, fmtFileName(date, symbol))
	if common.FileExists(path) {
		fail(fmt.Errorf("file `%s` already exists", path))
	}

	// fetch series
	series, err := fetchSeries(date, symbol)
	if err != nil {
		fail(err)
	}

	// store as a go binary (.gob)
	err = storeSeries(path, series)
	if err != nil {
		fail(err)
	}

	nBars := len(series)
	fileInfo, err := os.Stat(path)

	fmt.Printf("Successfully stored market data:\n")
	fmt.Printf("  path:      %s\n", path)
	fmt.Printf("  num bars:  %d \n", nBars)
	fmt.Printf("  file size: %.2f KB (%d bytes)\n", float64(fileInfo.Size())/1024, fileInfo.Size())
}
