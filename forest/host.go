package forest

import (
	"github.com/devopsext/eye/common"
)

type HostFrames []*HostFrame

type HostFrame struct {
	stamp common.Stamp
	hash  common.Hash
}

type HostEngine struct {
}

// HostFrame

func (hf *HostFrame) Valid() bool {
	return false
}

func NewHostFrame(stamp common.Stamp, hash common.Hash, hostSignal *common.HostSignal) *HostFrame {

	r := &HostFrame{
		stamp: stamp,
		hash:  hash,
	}
	return r
}

// HostEngine

func (he *HostEngine) Train(frames HostFrames) error {
	return nil
}

func NewHostEngine() *HostEngine {

	return &HostEngine{}
}
