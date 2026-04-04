package db

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

type Database interface {
	BatchStoreBars(ctx context.Context, bars common.Series) error
	GetCompleteTradingDay(ctx context.Context, day civil.Date, symbol string) ([]common.Bar, error)
}

// Implements the Database interface
type database struct {
	connPool *pgxpool.Pool
}

var once sync.Once

func NewDatabase(dbUrl string) *database {
	var db *database
	var err error
	once.Do(func() {
		pool, _err := pgxpool.New(context.Background(), dbUrl)
		err = _err

		db = &database{
			connPool: pool,
		}
	})
	if err != nil {
		log.Fatalf("Failed to init database: %#v", err)
	}
	if db == nil {
		log.Fatalf("NewDatabase() called > 1 times")
	}

	return db
}

func (db *database) getConn(ctx context.Context) (*pgxpool.Conn, error) {
	conn, err := db.connPool.Acquire(ctx)
	return conn, err
}

// Note - future optimisation: This could likely be quicker if I implement the
// CopyFromSource interface (to avoid buffering in memory)
func (db *database) BatchStoreBars(ctx context.Context, bars common.Series) error {
	conn, err := db.getConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"ohlc_bars"},
		bars.ColNames(),
		pgx.CopyFromRows(bars.ToRows()),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk insert: %w", err)
	}
	if n != int64(len(bars)) {
		return fmt.Errorf("unexpected copy count `%d` expected `%d`", n, len(bars))
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}
	return nil
}

func (db *database) GetCompleteTradingDay(ctx context.Context, day civil.Date, symbol string) (common.Series, error) {
	conn, err := db.getConn(ctx)
	defer conn.Release()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	stmt := `
		SELECT symbol, t, o, h, l, c, n, v, vw
		FROM ohlc_bars
		WHERE t >= $1
		AND t <= $2
		AND symbol = $3
	`
	rows, err := conn.Query(
		ctx,
		stmt,
		common.GetMarketOpenUnixMilli(day),
		common.GetMarketCloseUnixMilli(day),
		symbol,
	)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	var fn pgx.RowToFunc[common.Bar] = func(row pgx.CollectableRow) (common.Bar, error) {
		var bar common.Bar
		err := row.Scan(&bar.Symbol, &bar.T, &bar.O, &bar.H, &bar.L, &bar.C, &bar.N, &bar.V, &bar.VW)
		if err != nil {
			return common.Bar{}, err
		}
		return bar, nil
	}
	var bars common.Series
	bars, err = pgx.CollectRows(rows, fn)
	if err != nil {
		return nil, fmt.Errorf("failed to collect rows: %w", err)
	}

	return bars, nil
}
