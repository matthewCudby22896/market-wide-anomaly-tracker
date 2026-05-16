package hub

import (
	"sync"
	"github.com/mcudby/mwat/components/replayengine/common"
	"github.com/mcudby/mwat/components/replayengine/defaults"
)

type configWrapper struct {
	lock   sync.Mutex
	config common.SimulationConfig
}

func defaultSimulationConfig() *configWrapper {
	return &configWrapper{
		lock: sync.Mutex{},
		config: common.SimulationConfig{
			Timescale: defaults.DefaultTimescale,
			Date:      defaults.DefaultDay,
		},
	}
}

func (s *configWrapper) GetConfig() common.SimulationConfig {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.config
}

func (s *configWrapper) SetConfig(newConfig common.SimulationConfig) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.config = newConfig
}