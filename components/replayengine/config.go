package replayengine

import (
	"fmt"
	"os"

	"cloud.google.com/go/civil"
)

const (
	DefaultPostgresDB = "postgres"
	DefaultPostgresPassword = "password"
)

func fmtDBUrl(url string) string {
	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = DefaultPostgresDB
	}
	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = DefaultPostgresPassword
	}

	return fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", database, password, url)
}

const DefaultTimescale float32 = 5.0

var DefaultDay = civil.Date{Year: 2025, Month: 3, Day: 20}

const (
	MAX_TIMESCALE float32 = 3600.0
	MIN_TIMESCALE float32 = 0.1
)
