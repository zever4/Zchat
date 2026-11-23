package grpc

import (
	"errors"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/admission"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestE2EEAdmissionRejectsBeforeCommitAndPreservesExactRetryPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	g := admission.New(admission.DefaultConfig(), nil)
	f.auth.SetRequestAdmission(g)
	request := f.message(0)
	original := proto.Clone(request)
	var releases []func()
	for i := int64(100); i < 116; i++ {
		r, e := g.Try(t.Context(), i, admission.Data)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	_, err := f.v2.SendMessage(f.ownerContext(t, 1), request)
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	var count int
	if err = f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_send_receipts`).Scan(&count); err != nil || count != 0 {
		t.Fatal(count, err)
	}
	if !proto.Equal(request, original) {
		t.Fatal("busy mutated immutable request")
	}
	for _, r := range releases {
		r()
	}
	got, e := f.v2.SendMessage(f.ownerContext(t, 1), request)
	if e != nil {
		t.Fatal(e)
	}
	again, e := f.v2.SendMessage(f.ownerContext(t, 1), request)
	if e != nil || !proto.Equal(got, again) {
		t.Fatal("exact retry receipt", e)
	}
	if err = f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_send_receipts`).Scan(&count); err != nil || count != 1 {
		t.Fatal(count, err)
	}
	// A single hot authenticated account cannot consume Bob's allowance.
	releases = nil
	for i := 0; i < 4; i++ {
		r, e := g.Try(t.Context(), f.aliceID, admission.Data)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	if _, e := g.Try(t.Context(), f.aliceID, admission.Data); !errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	_, e = f.v2.GetPendingMessages(f.ownerContext(t, 2), &v2.GetPendingMessagesRequest{Context: f.wire(2)})
	if e != nil {
		t.Fatal("other account starved", e)
	}
	for _, r := range releases {
		r()
	}
	if g.Snapshot().Active != 0 || g.Snapshot().Accounts != 0 {
		t.Fatal(g.Snapshot())
	}
}
