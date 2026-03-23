package db

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5"
)

var DB_URL = "postgres://postgres:password@localhost:6543/postgres?sslmode=disable"

func getConnection() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), DB_URL)

	return conn, err
}

/*
- Want a `migrations` table, step 1 is to check that this exists and if it doesn't create it

- Want to verify that migrations haven't been modified post hoc

	- Could create a hash of each each migration and the prior migrations hash.
	- Each time the system launches, it iterates over the migrations, redoing the work
	of creating the hash chain. If one of them differs, then you know that either the
	migration, or order of the migration changed.

*/

func createMigrationsTable(ctx context.Context, conn *pgx.Conn) error {
	stmt := `
	CREATE TABLE IF NOT EXISTS migrations (
		id SERIAL PRIMARY KEY,
		name TEXT UNIQUE NOT NULL,
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		prev_hash CHAR(64) NOT NULL,
		hash CHAR(64) NOT NULL
	)`

	_, err := conn.Exec(ctx, stmt)
	return err
}

func getMigrations() ([]string, error) {
	linesTxt := make([]string, 0)
	file, err := os.Open("migrations.txt")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		linesTxt = append(linesTxt, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	paths := make([]string, len(linesTxt))
	for i, f := range linesTxt {
		absPath, err := filepath.Abs(f)
		if err != nil {
			return nil, err
		}
		paths[i] = absPath
	}

	return paths, nil
}

func setupDB(conn *pgx.Conn) error {
	ctx := context.WithoutCancel(context.Background())

	// 1. Create migrations table if it doesn't exist
	err := createMigrationsTable(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to setup db: %#v ", err)
	}

	// 2. Get list of migrations
	migrations, err := getMigrations()
	if err != nil {
		return fmt.Errorf("failed to get migrations: %#v ", err)
	}
	fmt.Print(migrations)

	return nil
}
