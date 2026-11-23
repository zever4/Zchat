package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	v1 "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	pb "github.com/zever4/Zchat/gen/messenger"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestOptionalMessengerRegistration(t *testing.T) {
	for _, flag := range []string{"", "false", "1", "TRUE", "typo", "true"} {
		t.Run("flag="+flag, func(t *testing.T) {
			server := grpc.NewServer()
			defer server.Stop()
			constructed := 0
			registered := RegisterOptionalMessenger(server, flag, func() v1.MessengerServer { constructed++; return &v1.UnimplementedMessengerServer{} })
			want := flag == "true"
			_, present := server.GetServiceInfo()[v1.Messenger_ServiceDesc.ServiceName]
			if registered != want || present != want || (constructed == 1) != want {
				t.Fatalf("registration=%v present=%v constructors=%d", registered, present, constructed)
			}
			if _, exposed := server.GetServiceInfo()["messenger.Messenger"]; exposed {
				t.Fatal("unsafe legacy API registered")
			}
			if want {
				return
			}
			listener := bufconn.Listen(1024 * 1024)
			defer listener.Close()
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			defer func() { server.Stop(); <-done }()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := pb.NewMessengerClient(conn)
			_, err = v1.NewMessengerClient(conn).GetCapabilities(ctx, &v1.Empty{})
			requireUnavailable(t, err)
			_, err = v1.NewMessengerClient(conn).GetPreKeyInventory(ctx, &v1.GetPreKeyInventoryRequest{})
			requireUnavailable(t, err)
			_, err = client.UploadPreKeys(ctx, &pb.PreKeyBundleUpload{})
			requireUnavailable(t, err)
			_, err = client.FetchPreKeys(ctx, &pb.UserId{})
			requireUnavailable(t, err)
			_, err = client.SendMessage(ctx, &pb.EncryptedMessage{})
			requireUnavailable(t, err)
			stream, err := client.StreamMessages(ctx, &pb.UserId{})
			if err == nil {
				_, err = stream.Recv()
			}
			requireUnavailable(t, err)
		})
	}
}
func requireUnavailable(t *testing.T, err error) {
	t.Helper()
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("want unregistered service, got %v", err)
	}
}

func TestOptionalEpochRegistration(t *testing.T) {
	for _, flag := range []string{"", "false", "TRUE", "true"} {
		t.Run(flag, func(t *testing.T) {
			s := grpc.NewServer()
			defer s.Stop()
			calls := 0
			got := RegisterOptionalEpochMessenger(s, flag, func() (*E2EEDeviceServer, *E2EEEpochServer) {
				calls++
				return NewE2EEDeviceServer(nil, nil, nil, nil), NewE2EEEpochServer(nil)
			})
			want := flag == "true"
			_, registry := s.GetServiceInfo()[v1.Messenger_ServiceDesc.ServiceName]
			_, epoch := s.GetServiceInfo()[v2.Messenger_ServiceDesc.ServiceName]
			if got != want || registry != want || epoch != want || (calls == 1) != want {
				t.Fatal("partial/eager registration")
			}
			if _, unsafe := s.GetServiceInfo()["messenger.Messenger"]; unsafe {
				t.Fatal("unauthenticated legacy service exposed")
			}
		})
	}
	t.Run("unsafe mixed graph", func(t *testing.T) {
		s := grpc.NewServer()
		defer s.Stop()
		defer func() {
			if recover() == nil || len(s.GetServiceInfo()) != 0 {
				t.Fatal("unsafe graph partially registered")
			}
		}()
		RegisterOptionalEpochMessenger(s, "true", func() (*E2EEDeviceServer, *E2EEEpochServer) {
			return newPrototypeDeviceServer(nil, nil, nil, nil), NewE2EEEpochServer(nil)
		})
	})
}
