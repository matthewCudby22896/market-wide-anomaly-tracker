package db

import (
	"context"
	"fmt"
	"log"
	"sync"

	"cloud.google.com/go/civil"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mcudby/mwat/components/replayengine/common"
)

type Database interface {
	BatchStoreBars(ctx context.Context, bars common.Series, symbol common.Symbol, date civil.Date) error
	GetCompleteTradingDay(ctx context.Context, symbol common.Symbol, date civil.Date) (common.Series, error)
	LoadHydrationState(ctx context.Context) ([]common.HydrationStatusRow, error)
}

// Implements the Database interface
type database struct {
	connPool *pgxpool.Pool
}

var once sync.Once

func RequireNewDatabase(connString string) *database {
	var db *database
	once.Do(func() {
		pool, err := pgxpool.New(context.Background(), connString)
		if err != nil {
			log.Fatalf("Failed to init database: %#v", err)
		}

		db = &database{
			connPool: pool,
		}
	})
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
func (db *database) BatchStoreBars(ctx context.Context, bars common.Series, symbol common.Symbol, date civil.Date) error {
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
		pgx.Identifier{"bars_1sec"},
		bars.ColNames(),
		pgx.CopyFromRows(bars.ToRows()),
	)
	if err != nil {
		return fmt.Errorf("failed to bulk insert: %w", err)
	}
	if n != int64(len(bars)) {
		return fmt.Errorf("unexpected copy count `%d` expected `%d`", n, len(bars))
	}

	db.updateHydrationStateTableInTx(ctx, tx, symbol, date)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}
	return nil
}

func (db *database) updateHydrationStateTableInTx(ctx context.Context, tx pgx.Tx, symbol common.Symbol, date civil.Date) error {
	stmt := "INSERT INTO hydration_state_1sec (symbol, date) VALUES ($1, $2)"
	_, err := tx.Exec(ctx, stmt, symbol, date.String())
	if err != nil {
		return err
	}
	return nil
}

// TODO: Rename
func (db *database) GetCompleteTradingDay(ctx context.Context, symbol common.Symbol, date civil.Date) (common.Series, error) {
	conn, err := db.getConn(ctx)
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

func (db *database) LoadHydrationState(ctx context.Context) ([]common.HydrationStatusRow, error) {
	conn, err := db.getConn(ctx)
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
