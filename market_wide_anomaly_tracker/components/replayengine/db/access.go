package db

import (
	"context"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

/*
Need a thread safe access layer.

AIM:
	- Provide an interface
*/

type Database interface {
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
