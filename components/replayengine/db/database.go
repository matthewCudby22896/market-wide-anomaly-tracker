package db

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mcudby/mwat/components/replayengine/common"
)

// Implements the Database interface
type ReplayEngineDB struct {
	connPool *pgxpool.Pool
}

var once sync.Once

func EstablishDBConnection(ctx context.Context, connectionURI string) *ReplayEngineDB {
	pool, err := pgxpool.New(ctx, connectionURI)
	if err != nil {
		log.Fatalf("failed to instantiate connection pool")
	}

	timeout := time.After(20 * time.Second)
	for {
		_, err := pool.Acquire(ctx)
		if err == nil {
			break
		}
		select {
		default:
			time.Sleep(500 * time.Millisecond)
		case <-timeout:
			log.Fatalf("failed to ping database within allotted time")
		}
	}

	return &ReplayEngineDB{
		connPool: pool,
	}
}

func RequireNewDatabase(connectionURI string) *ReplayEngineDB {
	var db *ReplayEngineDB
	once.Do(func() {
		/* A pool returns without waiting for any connections to be established
		 */
		pool, err := pgxpool.New(context.Background(), connectionURI)
		if err != nil {
			log.Fatalf("Failed to init database: %#v", err)
		}

		db = &ReplayEngineDB{
			connPool: pool,
		}
	})
	if db == nil {
		log.Fatalf("NewDatabase() called > 1 times")
	}

	return db
}

func (db *ReplayEngineDB) GetConn(ctx context.Context) (*pgxpool.Conn, error) {
	conn, err := db.connPool.Acquire(ctx)
	return conn, err
}

// Note - future optimisation: This could likely be quicker if I implement the
// CopyFromSource interface (to avoid buffering in memory)
func (db *ReplayEngineDB) StoreSeries(ctx context.Context, series common.Series, symbol common.Symbol, date civil.Date) error {
	conn, err := db.GetConn(ctx)
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
		pgx.Identifier{"bars_1sec"},
		series.ColNames(),
		pgx.CopyFromRows(series.ToRows()),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk insert: %w", err)
	}
	if n != int64(len(series)) {
		return fmt.Errorf("unexpected copy count `%d` expected `%d`", n, len(series))
	}

	db.updateHydrationStateTableInTx(ctx, tx, symbol, date)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}
	return nil
}

func (db *ReplayEngineDB) updateHydrationStateTableInTx(ctx context.Context, tx pgx.Tx, symbol common.Symbol, date civil.Date) error {
	stmt := "INSERT INTO hydration_state_1sec (symbol, date) VALUES ($1, $2)"
	_, err := tx.Exec(ctx, stmt, symbol, date.String())
	if err != nil {
		return err
	}
	return nil
}

func (db *ReplayEngineDB) GetSeries(ctx context.Context, symbol common.Symbol, date civil.Date) (common.Series, error) {
	conn, err := db.GetConn(ctx)
	defer conn.Release()
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	stmt := `
		SELECT symbol, t, o, h, l, c, n, v, vw
		FROM bars_1sec
		WHERE t >= $1
		AND t <= $2
		AND symbol = $3
		ORDER BY t DESC
	`
	rows, err := conn.Query(
		ctx,
		stmt,
		common.NYSEOpenUnixMilli(date),
		common.NYSECloseUnixMilli(date),
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

func (db *ReplayEngineDB) LoadHydrationState(ctx context.Context) ([]common.HydrationStatusRow, error) {
	conn, err := db.GetConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}

	stmt := `SELECT symbol, date::TEXT FROM hydration_state_1sec`

	rows, err := conn.Query(ctx, stmt)
	if err != nil {
		return nil, fmt.Errorf("failed to load hydration state: %w", err)
	}

	res, err := pgx.CollectRows(
		rows,
		func(row pgx.CollectableRow) (common.HydrationStatusRow, error) {
			var v common.HydrationStatusRow
			err := row.Scan(&v.Symbol, &v.Date)
			if err != nil {
				return common.HydrationStatusRow{}, err
			}
			return v, nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to collect rows: %w", err)
	}

	return res, nil
}
