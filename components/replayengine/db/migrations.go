package db

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"log"
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

//go:embed migrations/*.sql
var migrationsFiles embed.FS

func (db *database) RequireApplyMigrations() {
	err := db.applyMigrations()
	if err != nil {
		log.Fatalf("Failed to apply migrations: %w", err)
	}
}

// TODO: Look into migration hash chains more
func (db *database) applyMigrations() error {
	ctx := context.WithoutCancel(context.Background())

	conn, err := db.getConn(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w ", err)
	}

	// 1. Create migrations table if it doesn't exist
	err = createMigrationsTable(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to setup db: %w ", err)
	}

	// 2. Get list of migrations
	migrations, err := migrationsFiles.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("failed to read embedded migrations dir: %w ", err)
	}

	// 3. Apply migrations
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w ", err)
	}
	// Rollback is safe to call even if the tx is already closed, so if
	// the tx commits succesfully, this is a no-op
	defer tx.Rollback(ctx)

	prevHash := make([]byte, 32)

	for _, entry := range migrations {
		name := entry.Name()
		content, err := migrationsFiles.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", name, err)
		}

		currentHash, _ := hash(content, prevHash)
		isApplied, storedHash, err := migrationIsApplied(ctx, tx, name)
		if err != nil {
			return fmt.Errorf("err whilst checking if migration has been applied: %w", err)
		}

		if isApplied {
			if !slices.Equal(currentHash, storedHash) {
				return fmt.Errorf("stored hash doesn't match calculated hash for migration: `%s`", name)
			}

		} else {
			stmt := string(content)

			_, err := tx.Exec(ctx, stmt)
			if err != nil {
				return fmt.Errorf("failed to apply migration `%s`: %w", name, err)
			}

			err = appendMigration(ctx, tx, name, prevHash, currentHash)
			if err != nil {
				return fmt.Errorf("failed to append to migration table: %w", err)
			}
		}

		prevHash = currentHash
	}

	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit tx: %w ", err)
	}

	return nil
}

func hash(migrationContents, prevMigrationHash []byte) ([]byte, error) {
	h := sha256.New()

	if _, err := h.Write(prevMigrationHash); err != nil {
		return nil, err
	}
	if _, err := h.Write(migrationContents); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func migrationIsApplied(ctx context.Context, tx pgx.Tx, migrationName string) (bool, []byte, error) {
	stmt := "SELECT name, hash FROM migrations WHERE name = $1"
	rows, err := tx.Query(ctx, stmt, migrationName)
	if err != nil {
		return false, []byte{}, err
	}
	var name string
	var currentString string
	rows.Next()
	rows.Scan(&name, &currentString)
	if rows.Next() {
		return false, []byte{}, fmt.Errorf(">1 row where name == `%s`", migrationName)
	}
	hashBytes, err := hex.DecodeString(currentString)
	if err != nil {
		return false, []byte{}, fmt.Errorf("failed to decode stored hash string: %w", err)
	}
	return (name == migrationName), hashBytes, nil
}

func appendMigration(ctx context.Context, tx pgx.Tx, name string, prevHash, hash []byte) error {
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
