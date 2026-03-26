package db

import (
	"context"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine"
)

/*
Need a thread safe access layer.

AIM:
	- Provide an interface
*/

type Database interface {
	BatchStoreOHLC(bars []replayengine.OHLC) error
	GetCompleteTradingDay(day civil.Date, symbol string) ([]replayengine.OHLC, error)
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
		pool, _err := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))
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

func (d *database) BatchStoreOHLC(bars []replayengine.OHLC) error {
	// TODO
	return nil
}

func (d *database) GetCompleteTradingDay(day civil.Date, symbol string) ([]replayengine.OHLC, error) {
	// TODO
	return nil, nil
}
