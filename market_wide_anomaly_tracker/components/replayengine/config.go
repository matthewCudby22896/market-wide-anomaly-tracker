package replayengine

import (
	"cloud.google.com/go/civil"
)

const WS_SOCKET = ":8080"

var DB_URL string = "postgres://postgres:password@localhost:6543/postgres?sslmode=disable"

var DEFAULT_DAY = civil.Date{Year: 2025, Month: 3, Day: 20}

var DEFAULT_SPEEDUP float32 = 1.0
