package service

import (
	"context"
	"errors"
	"os"
	"testing"

	common "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func TestAttachmentRpcRemainsUnavailableWithoutOwnedAdmission(t *testing.T) {
	raw, err := os.ReadFile("../../proto/fixtures/attachment-envelope-v3/envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	e := new(v2.Envelope)
	if err = proto.Unmarshal(raw, e); err != nil {
		t.Fatal(err)
	}
	req := &v2.SendMessageRequest{Context: &common.ClientContext{ProtocolVersion: 2, DeviceGenerationId: e.Sender.GenerationId}, Envelope: e}
	if err = boundary.ValidateEpochSend(req); err != nil {
		t.Fatal(err)
	}
	// Repository support alone does not expose Send3.
	receipt, err := NewE2EEEpochService(nil, nil, nil).Send(context.Background(), 1, req)
	if receipt != nil || !errors.Is(err, boundary.ErrUnsupportedVersion) {
		t.Fatal("unimplemented blob service admitted send", err)
	}
}

type attachmentAdmission func(context.Context) (context.Context, func(), error)

func (a attachmentAdmission) BeginAttachmentSend(ctx context.Context) (context.Context, func(), error) {
	return a(ctx)
}

type attachmentMessages struct {
	E2EEEpochMessages
	send func(context.Context, int64, *v2.SendMessageRequest) (*v2.SendReceipt, error)
}

func (m attachmentMessages) SendV2(ctx context.Context, uid int64, r *v2.SendMessageRequest) (*v2.SendReceipt, error) {
	return m.send(ctx, uid, r)
}

func TestAttachmentSendAdmissionJoinsOnlyValidatedFileCalls(t *testing.T) {
	raw, err := os.ReadFile("../../proto/fixtures/attachment-envelope-v3/envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	e := new(v2.Envelope)
	if err = proto.Unmarshal(raw, e); err != nil {
		t.Fatal(err)
	}
	r := &v2.SendMessageRequest{Context: &common.ClientContext{ProtocolVersion: 2, DeviceGenerationId: e.Sender.GenerationId}, Envelope: e}
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "repository error"}[fail], func(t *testing.T) {
			owned, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, released, admitted := 0, 0, 0
			s := NewE2EEEpochService(attachmentMessages{send: func(ctx context.Context, uid int64, req *v2.SendMessageRequest) (*v2.SendReceipt, error) {
				calls++
				if ctx != owned || uid != 42 || req != r {
					t.Fatal("wrong owned request")
				}
				if fail {
					return nil, domain.ErrE2EEMessageConflict
				}
				return &v2.SendReceipt{MessageId: e.MessageId}, nil
			}}, nil, nil).WithAttachmentAdmission(attachmentAdmission(func(ctx context.Context) (context.Context, func(), error) {
				admitted++
				if ctx != t.Context() {
					t.Fatal("wrong parent")
				}
				return owned, func() { released++ }, nil
			}))
			got, err := s.Send(t.Context(), 42, r)
			if calls != 1 || admitted != 1 || released != 1 || (fail && !errors.Is(err, domain.ErrE2EEMessageConflict)) || (!fail && (err != nil || got == nil)) {
				t.Fatal("admission lifetime", calls, admitted, released, err)
			}
			s.WithAttachmentAdmission(attachmentAdmission(func(context.Context) (context.Context, func(), error) { return nil, nil, domain.ErrE2EEBlobNotReady }))
			if _, err = s.Send(t.Context(), 42, r); !errors.Is(err, domain.ErrE2EEBlobNotReady) || calls != 1 {
				t.Fatal("failed admission reached SQL", err)
			}
			bad := proto.Clone(r).(*v2.SendMessageRequest)
			bad.Envelope.MessageId = "invalid"
			s.WithAttachmentAdmission(attachmentAdmission(func(context.Context) (context.Context, func(), error) {
				t.Fatal("malformed request reached admission")
				return nil, nil, nil
			}))
			if _, err = s.Send(t.Context(), 42, bad); err == nil {
				t.Fatal("malformed request")
			}
			text := proto.Clone(r).(*v2.SendMessageRequest)
			text.Envelope.ProtocolVersion = 2
			text.Envelope.Blob = nil
			s.messages = attachmentMessages{send: func(ctx context.Context, _ int64, req *v2.SendMessageRequest) (*v2.SendReceipt, error) {
				if ctx != t.Context() || req != text {
					t.Fatal("text context changed")
				}
				return &v2.SendReceipt{}, nil
			}}
			if _, err = s.Send(t.Context(), 42, text); err != nil {
				t.Fatal("text affected by file admission", err)
			}
		})
	}
}
