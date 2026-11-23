package grpc

import (
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/grpc"
)

// RegisterOptionalMessenger is retained for prototype fixtures. Production uses
// RegisterOptionalEpochMessenger and the default server retires v1 DATA methods.
// Never register the unauthenticated legacy messenger.Messenger prototype here.
// The RPC implementation is constructed lazily; guard setup performs no DB
// operations or background work until a v1 request is admitted.
func RegisterOptionalMessenger(registrar grpc.ServiceRegistrar, flag string, factory func() pb.MessengerServer) bool {
	if flag != "true" {
		return false
	}
	pb.RegisterMessengerServer(registrar, factory())
	return true
}

func RegisterOptionalEpochMessenger(registrar grpc.ServiceRegistrar, flag string, factory func() (*E2EEDeviceServer, *E2EEEpochServer)) bool {
	if flag != "true" {
		return false
	}
	registry, epoch := factory()
	if registry == nil || epoch == nil || registry.prototypeData {
		panic("v2 registration requires registry with prototype DATA disabled")
	}
	pb.RegisterMessengerServer(registrar, registry)
	v2.RegisterMessengerServer(registrar, epoch)
	return true
}
