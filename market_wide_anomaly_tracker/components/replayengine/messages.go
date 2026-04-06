package replayengine

import (
	"cloud.google.com/go/civil"
	"github.com/matthewCudby22896/market_wide_anomaly_tracker/components/replayengine/common"
)

type symbolHydrated struct {
	symbol common.Symbol
	date   civil.Date
}
