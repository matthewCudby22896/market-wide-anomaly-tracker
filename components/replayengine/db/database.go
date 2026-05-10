package db

import (
	"context"
	"errors"
	"fmt"
	"log"
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

const BaseTableName = "bars_1sec"

var ColNames = []string{
	"symbol",
	"t",
	"o",
	"h",
	"l",
	"c",
	"n",
	"v",
	"vw",
}

func EstablishDBConnection(ctx context.Context, connectionURI string) *ReplayEngineDB {
	pool, err := pgxpool.New(ctx, connectionURI)
	if err != nil {
		log.Fatalf("failed to instantiate connection pool")
	}

	timeout := time.After(20 * time.Second)
	for {
		fmt.Print("attempting to establish connection to database...\n")
		_, err := pool.Acquire(ctx)
		if err == nil {
			break
		}
		select {
		default:
			time.Sleep(500 * time.Millisecond)
		case <-ctx.Done():
			return nil
		case <-timeout:
			log.Fatalf("failed to ping database within allotted time")
		}
	}

	return &ReplayEngineDB{
		connPool: pool,
	}
}

func (db *ReplayEngineDB) GetConn(ctx context.Context) (*pgxpool.Conn, error) {
	return db.connPool.Acquire(ctx)
}

func (db *ReplayEngineDB) InsertFullSession(ctx context.Context, series []common.Bar, symbol, date string) error {
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

	// Insert series
	err = db.insertSeriesInTx(ctx, tx, series)
	if err != nil {
		return fmt.Errorf("failed to insert series: %w", err)
	}
	// Record hydration for symbol-date combo
	if err := db.recordHydrationInTx(ctx, tx, symbol, date); err != nil {
		return fmt.Errorf("failed to record hydration: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}
	return nil
}

func (db *ReplayEngineDB) insertSeriesInTx(ctx context.Context, tx pgx.Tx, series []common.Bar) error {
	n, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{BaseTableName},
		ColNames,
		pgx.CopyFromSlice(
			len(series),
			func(i int) ([]any, error) {
				return []any{
					series[i].Symbol,
					series[i].T,
					series[i].O,
					series[i].H,
					series[i].L,
					series[i].C,
					series[i].N,
					series[i].V,
					series[i].VW,
				}, nil
			},
		),
	)
	if err != nil {
		return err
	}
	if n != int64(len(series)) {
		return fmt.Errorf("no. rows copied != no. bars in series")
	}
	return err
}

func (db *ReplayEngineDB) recordHydrationInTx(ctx context.Context, tx pgx.Tx, symbol, date string) error {
	stmt := "INSERT INTO hydration_state_1sec (symbol, date) VALUES ($1, $2)"
	_, err := tx.Exec(ctx, stmt, symbol, date)
	if err != nil {
		return err
	}
	return nil
}

// Convenience method for fetching the complete trading day for a specified date & symbol
func (db *ReplayEngineDB) GetFullSession(
	ctx context.Context,
	symbol string,
	date civil.Date,
) ([]common.Bar, error) {
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
		ORDER BY t ASC
	`
	t1 := common.NYSEOpenUnixMilli(date)
	t2 := common.NYSECloseUnixMilli(date)
	rows, err := conn.Query(
		ctx,
		stmt,
		t1,
		t2,
		symbol,
	)
	series := make([]common.Bar, 0)
	var bar common.Bar
	for rows.Next() {
		rows.Scan(
			&bar.Symbol,
			&bar.T,
			&bar.O,
			&bar.H,
			&bar.L,
			&bar.C,
			&bar.N,
			&bar.V,
			&bar.VW,
		)
		series = append(series, bar)
	}
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("result set reading ended prematurely due to err: %w", err)
	}

	return series, nil
}

var BufferTooSmallErr = errors.New("buffer too small")

// Populates the buffer for the range timestamp [t1, t2), returns the no. bars copied into the buffer
// - Errors if the no. rows returned in the query exceed the capacity of the buffer
func (db *ReplayEngineDB) GetSeries(
	ctx context.Context,
	symbol string,
	date string,
	t1 int64,
	t2 int64,
	buffer []common.Bar,
) (int, error) {
	conn, err := db.GetConn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Release()

	stmt := `
		SELECT symbol, t, o, h, l, c, n, v, vw
		FROM bars_1sec
		WHERE t >= $1
		AND t < $2
		AND symbol = $3
		ORDER BY t ASC
	`
	rows, err := conn.Query(
		ctx,
		stmt,
		t1,
		t2,
		symbol,
	)
	if err != nil {
		return 0, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	n := 0
	for i := range cap(buffer) {
		if rows.Next() {
			rows.Scan(
				&buffer[i].Symbol,
				&buffer[i].T,
				&buffer[i].O,
				&buffer[i].H,
				&buffer[i].L,
				&buffer[i].C,
				&buffer[i].N,
				&buffer[i].V,
				&buffer[i].VW,
			)
			n += 1
		} else {
			err := rows.Err()
			if err != nil {
				return 0, fmt.Errorf("result set reading ended prematurely due to err: %w", err)
			}
		}
	}

	if rows.Next() {
		nExtra := 1
		for rows.Next() {
			nExtra += 1
		}
		return 0, fmt.Errorf("%w: size of query result (%d rows) exceeed buffer capacity (%d)", BufferTooSmallErr, cap(buffer)+nExtra, cap(buffer))
	}

	return n, nil
}

func (db *ReplayEngineDB) GetHydrationStatus(ctx context.Context) ([]common.HydrationStatusRow, error) {
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
