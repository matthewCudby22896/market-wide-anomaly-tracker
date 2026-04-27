package datawarehouse

import (
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"

	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/common"
	"github.com/mcudby/mwat/components/replayengine"
	enginecommon "github.com/mcudby/mwat/components/replayengine/common"
)

type dataWarehouse struct {
	datastoredir string
	client       replayengine.MassiveClient
}

func NewDataWarehouse(datastoredir string) *dataWarehouse {
	client := replayengine.NewMassiveClient()

	return &dataWarehouse{
		datastoredir: datastoredir,
		client:       client,
	}
}

func (w *dataWarehouse) FetchAndStoreSeries(ctx context.Context, date civil.Date, symbol enginecommon.Symbol) (string, error) {
	file := fmt.Sprintf("%s_%s", symbol, date.String())
	path := filepath.Join(w.datastoredir, file)

	if common.FileExists(path) {
		return "", fmt.Errorf("file '%s' already exists", path)
	}

	series, err := w.client.FetchDayData(ctx, date, symbol)
	if err != nil {
		return "", err
	}

	f, err := os.Create(path)
	if err != nil {
		return "", fmt.Errorf("failed to open file at `%s`: %w", path, err)
	}

	encoder := gob.NewEncoder(f)

	err = encoder.Encode(series)
	if err != nil {
		return "", err
	}

	fmt.Printf("%d ohlc bars stored\n", len(series))

	return path, nil
}
