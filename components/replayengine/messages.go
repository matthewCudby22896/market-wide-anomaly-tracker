package replayengine

import (
	"cloud.google.com/go/civil"
	"github.com/mcudby/mwat/components/replayengine/common"
)

type symbolHydrated struct {
	symbol common.Symbol
	date   civil.Date
}
