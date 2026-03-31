package db

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func createMigrationsTable(ctx context.Context, conn *pgxpool.Conn) error {
	stmt := `
	CREATE TABLE IF NOT EXISTS migrations (
		id         SERIAL PRIMARY KEY,
		name       TEXT UNIQUE NOT NULL,
		applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		prev_hash  CHAR(64) NOT NULL,
		hash       CHAR(64) NOT NULL
	);`
	_, err := conn.Exec(ctx, stmt)
	return err
}

func getMigrations() (map[string]string, error) {
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

	paths := make(map[string]string, len(linesTxt))
	for _, f := range linesTxt {
		absPath, err := filepath.Abs("migrations/" + f)
		if err != nil {
			return nil, err
		}
		paths[f] = absPath
	}

	return paths, nil
}

// TODO: Look into migration hash chains more
func applyMigrations(conn *pgxpool.Conn) error {
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

	// 3. Apply migrations
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %#v ", err)
	}
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits succesfully, this is a no-op
	defer tx.Rollback(ctx)

	prevHash := make([]byte, 32)

	for mName, mPath := range migrations {
		contents, err := os.ReadFile(mPath)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %#v", mName, err)
		}

		currentHash, _ := _hash(prevHash, contents)
		mApplied, storedHash, err := _migrationApplied(ctx, tx, mName)
		if err != nil {
			return fmt.Errorf("_migrationsApplied err: %#v", err)
		}

		if mApplied {
			if !slices.Equal(currentHash, storedHash) {
				return fmt.Errorf("stored hash doesn't match calculated hash for migration: `%s`", mName)
			}

		} else {
			stmt := string(contents)

			_, err := tx.Exec(ctx, stmt)
			if err != nil {
				return fmt.Errorf("failed to apply migration `%s`: %#v", mName, err)
			}

			err = _appendMigration(ctx, tx, mName, prevHash, currentHash)
			if err != nil {
				return fmt.Errorf("failed to append to migration table: %#v", err)
			}
		}
		prevHash = currentHash
	}
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit tx: %#v ", err)
	}

	return nil
}

func _hash(migrationContents, prevMigrationHash []byte) ([]byte, error) {
	h := sha256.New()

	if _, err := h.Write(prevMigrationHash); err != nil {
		return nil, err
	}
	if _, err := h.Write(migrationContents); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func _migrationApplied(ctx context.Context, tx pgx.Tx, migrationName string) (bool, []byte, error) {
	stmt := "SELECT name, hash FROM migrations WHERE name = $1"
	var name string
	var currentHash string
	rows, err := tx.Query(ctx, stmt, migrationName)
	if err != nil {
		return false, []byte{}, err
	}
	rows.Scan(&name, &currentHash)
	if rows.Next() {
		return false, []byte{}, fmt.Errorf(">1 row where name == `%s`", migrationName)
	}
	return (name == migrationName), []byte(currentHash), nil
}

func _appendMigration(ctx context.Context, tx pgx.Tx, name string, prevHash, hash []byte) error {
	if len(prevHash) != 32 {
		return fmt.Errorf("`prevHash` was len %d, expecting len 32", len(prevHash))
	}
	if len(hash) != 32 {
		return fmt.Errorf("`hash` was len %d, expecting len 32", len(hash))
	}
	stmt := `
	INSERT INTO migrations (name, prev_hash, hash)
	VALUES ($1, $2, $3);
	`
	prevHashStr := fmt.Sprintf("%x", prevHash)
	hashStr := fmt.Sprintf("%x", hash)
	fmt.Printf("prev: %s\ncurr: %s\n", prevHashStr, hashStr)
	tag, err := tx.Exec(
		ctx,
		stmt,
		name,
		prevHashStr,
		hashStr,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rows affected != 1")
	}

	return nil
}
