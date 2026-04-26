package replayengine

import (
	"fmt"

	"cloud.google.com/go/civil"
)

const WS_SOCKET = ":8080"

const DB_URL string = "postgres://postgres:password@localhost:6543/postgres?sslmode=disable"

func fmtDBUrl(url string) string {
	return fmt.Sprintf("postgres://postgres:password@%s/postgres?sslmode=disable", url)
}

const DEFAULT_TIMESCALE float32 = 1.0

var DEFAULT_DAY = civil.Date{Year: 2025, Month: 3, Day: 20}

const (
	MAX_TIMESCALE float32 = 60.0
	MIN_TIMESCALE float32 = 0.1
)
