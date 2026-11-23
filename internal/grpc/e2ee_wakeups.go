package grpc

import (
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *E2EEDeviceServer) WatchInbox(_ *pb.StreamMessagesRequest, stream grpcgo.ServerStreamingServer[pb.InboxHint]) error {
	principal, ok := stream.Context().Value(e2eePrincipalKey{}).(e2eePrincipal)
	if !ok || principal.device == nil {
		return status.Error(codes.Unauthenticated, "missing E2EE device principal")
	}
	if s.wakeups == nil {
		return status.Error(codes.FailedPrecondition, "optional inbox hints unavailable")
	}
	hints, unsubscribe, ok := s.wakeups.Subscribe(principal.device.GenerationID)
	if !ok {
		return status.Error(codes.ResourceExhausted, "inbox hint capacity exhausted")
	}
	defer unsubscribe()
	// Covers backlog, lost publications and the subscription/publication race.
	if err := stream.Send(&pb.InboxHint{ProtocolVersion: 1}); err != nil {
		return err
	}
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-heartbeat.C:
		case <-hints:
		}
		// Coalesce bursts and bound writes. A stalled writer is forcibly released
		// by the auth interceptor's fixed WatchInbox lifetime, including no-read peers.
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-stream.Context().Done():
			timer.Stop()
			return stream.Context().Err()
		case <-timer.C:
		}
		if err := stream.Send(&pb.InboxHint{ProtocolVersion: 1}); err != nil {
			return err
		}
	}
}
