package grpc

import "github.com/zever4/Zchat/internal/service"

// Preserve old worker regressions until their v2 port. There is deliberately
// no production/exported switch that re-enables v1 DATA endpoints.
func newPrototypeDeviceServer(d *service.E2EEDeviceService, k *service.E2EEPreKeyService, c *service.E2EEConversationService, m *service.E2EEMessageService) *E2EEDeviceServer {
	s := NewE2EEDeviceServer(d, k, c, m)
	s.prototypeData = true
	return s
}
