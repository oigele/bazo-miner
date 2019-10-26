package vm

import (
	"github.com/oigele/bazo-miner/protocol"
)

type MockContext struct {
	protocol.Context
}

func NewMockContext(byteCode []byte) *MockContext {
	mc := MockContext{}
	mc.Contract = byteCode
	mc.Fee = 50
	return &mc
}

func (mc *MockContext) SetContract(contract []byte) {
	mc.Contract = contract
}
