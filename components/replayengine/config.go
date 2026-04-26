package replayengine

import (
	"fmt"
	"os"

	"cloud.google.com/go/civil"
)

const WS_SOCKET = ":8080"

const DB_URL string = "postgres://postgres:password@localhost:6543/postgres?sslmode=disable"

func fmtDBUrl(url string) string {
	database := os.Getenv("POSTGRES_DB")
	if database == "" {
		database = "postgres"
	}

	password := os.Getenv("POSTGRES_PASSWORD")
	if password == "" {
		password = "password"
	}

	return fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", database, password, url)
}

const DEFAULT_TIMESCALE float32 = 5.0

var DEFAULT_DAY = civil.Date{Year: 2025, Month: 3, Day: 20}

const (
	MAX_TIMESCALE float32 = 60.0
	MIN_TIMESCALE float32 = 0.1
)
