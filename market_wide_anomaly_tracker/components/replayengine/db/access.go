package db

import (
	"context"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v5/pgxpool"

	// TODO: Fix
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"

)

type Database interface {
	BatchStoreOHLC(bars []common.OHLC) error
	GetCompleteTradingDay(day civil.Date, symbol string) ([]common.OHLC, error)
}

// Implements the Database interface
type database struct {
	connPool *pgxpool.Pool
}

var once sync.Once

func NewDatabase() Database {
	var db *database
	var err error
	once.Do(func() {
		pool, _err := pgxpool.New(context.Background(), os.Getenv(DB_URL))
		err = _err

		db = &database{
			connPool: pool,
		}
	})
	if err != nil {
		log.Fatalf("Failed to init database: %#v", err)
	}

	return db
}

func (d *database) BatchStoreOHLC(bars []common.OHLC) error {
	// TODO
	return nil
}

func (d *database) GetCompleteTradingDay(day civil.Date, symbol string) ([]common.OHLC, error) {
	// TODO
	return nil, nil
}
