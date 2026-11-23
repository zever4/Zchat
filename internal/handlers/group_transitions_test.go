package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

type atomicGroupChats struct {
	repository.ChatRepository
	err     error
	called  bool
	approve bool
}

func (r *atomicGroupChats) DecideGroupJoinRequest(_ context.Context, chat, actor, target int64, approve bool) error {
	if chat != 10 || actor != 1 || target != 2 {
		panic("wrong transition arguments")
	}
	r.called, r.approve = true, approve
	return r.err
}
func TestGroupDecisionHTTP(t *testing.T) {
	auth := service.NewAuthService(visibilityUsers{}, "handler-test-only")
	token, _ := auth.GenerateToken(1, 1)
	for _, approve := range []bool{true, false} {
		for _, tc := range []struct {
			name   string
			err    error
			status int
		}{
			{"success", nil, 200}, {"forbidden", domain.ErrNotGroupOwner, 403},
			{"missing group", domain.ErrChatNotFound, 404}, {"missing request", domain.ErrJoinRequestNotFound, 404},
			{"opposite decision", domain.ErrJoinRequestNotPending, 409}, {"database error", errors.New("private database detail"), 500},
		} {
			name := "reject/"
			if approve {
				name = "approve/"
			}
			t.Run(name+tc.name, func(t *testing.T) {
				chats := &atomicGroupChats{err: tc.err}
				handler := HandleRejectGroupJoinRequest(auth, chats)
				if approve {
					handler = HandleApproveGroupJoinRequest(auth, chats)
				}
				req := httptest.NewRequest(http.MethodPost, "/", nil)
				req.SetPathValue("id", "10")
				req.SetPathValue("user_id", "2")
				req.Header.Set("Authorization", "Bearer "+token)
				res := httptest.NewRecorder()
				handler.ServeHTTP(res, req)
				if res.Code != tc.status || !chats.called || chats.approve != approve {
					t.Fatalf("status=%d called=%t approve=%t", res.Code, chats.called, chats.approve)
				}
				if tc.status == 500 && res.Body.String() != "Internal error\n" {
					t.Fatal("database error leaked")
				}
			})
		}
	}
	for _, tc := range []struct {
		name, id, target, token string
		status                  int
	}{
		{"unauthenticated", "10", "2", "", 401}, {"invalid group", "bad", "2", token, 400},
		{"negative group", "-1", "2", token, 400}, {"invalid target", "10", "0", token, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chats := &atomicGroupChats{}
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.SetPathValue("id", tc.id)
			req.SetPathValue("user_id", tc.target)
			req.Header.Set("Authorization", "Bearer "+tc.token)
			res := httptest.NewRecorder()
			HandleApproveGroupJoinRequest(auth, chats).ServeHTTP(res, req)
			if res.Code != tc.status || chats.called {
				t.Fatalf("status=%d called=%t", res.Code, chats.called)
			}
		})
	}
}
