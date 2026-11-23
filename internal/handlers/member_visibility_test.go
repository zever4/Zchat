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

type visibilityUsers struct{ repository.UserRepository }

func (visibilityUsers) GetSessionVersion(context.Context, int64, string) (int, error) { return 1, nil }

type visibilityChats struct {
	repository.ChatRepository
	member, hidden, manager bool
	fail                    string
	read                    bool
}

func (r *visibilityChats) IsMember(context.Context, int64, int64) (bool, error) {
	if r.fail == "member" {
		return false, errors.New("db")
	}
	return r.member, nil
}
func (r *visibilityChats) GetChatByID(context.Context, int64) (*domain.Chat, error) {
	if r.fail == "meta" {
		return nil, errors.New("db")
	}
	return &domain.Chat{Type: "group", HideMembersList: r.hidden}, nil
}
func (r *visibilityChats) IsGroupAdminOrOwner(context.Context, int64, int64) (bool, error) {
	if r.fail == "manager" {
		return false, errors.New("db")
	}
	return r.manager, nil
}
func (r *visibilityChats) GetMembersWithInfo(context.Context, int64) ([]*domain.User, error) {
	r.read = true
	return []*domain.User{{ID: 2, Username: "member"}}, nil
}
func (r *visibilityChats) GetMembersWithRoles(context.Context, int64) ([]*domain.ChatMemberWithRole, error) {
	r.read = true
	return []*domain.ChatMemberWithRole{}, nil
}

func TestBothMemberRoutesEnforceVisibility(t *testing.T) {
	auth := service.NewAuthService(visibilityUsers{}, "test-only-secret")
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"chats", "groups"} {
		for _, tc := range []struct {
			name                    string
			member, hidden, manager bool
			fail                    string
			status                  int
		}{
			{name: "outsider", status: 403},
			{name: "hidden member", member: true, hidden: true, status: 403},
			{name: "visible member", member: true, status: 200},
			{name: "hidden manager", member: true, hidden: true, manager: true, status: 200},
			{name: "metadata failure", member: true, fail: "meta", status: 500},
			{name: "permission failure", member: true, hidden: true, fail: "manager", status: 500},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				repo := &visibilityChats{member: tc.member, hidden: tc.hidden, manager: tc.manager, fail: tc.fail}
				var handler http.HandlerFunc
				if route == "chats" {
					handler = HandleGetChatMembers(auth, repo, visibilityUsers{})
				} else {
					handler = HandleListGroupMembers(auth, repo)
				}
				mux := http.NewServeMux()
				mux.HandleFunc("GET /api/"+route+"/{id}/members", handler)
				req := httptest.NewRequest("GET", "/api/"+route+"/7/members", nil)
				req.Header.Set("Authorization", "Bearer "+token)
				response := httptest.NewRecorder()
				mux.ServeHTTP(response, req)
				if response.Code != tc.status {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
				if repo.read != (tc.status == 200) {
					t.Fatalf("member data read=%v", repo.read)
				}
			})
		}
	}
}
