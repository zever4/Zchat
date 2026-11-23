package service

import (
	"context"
	"errors"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

var errPrivacyDB = errors.New("database unavailable")

type privacyUsers struct {
	repository.UserRepository
	settings map[int64]*domain.UserSettings
	failID   int64
}

func (r privacyUsers) GetSettings(_ context.Context, id int64) (*domain.UserSettings, error) {
	if id == r.failID {
		return nil, errPrivacyDB
	}
	return r.settings[id], nil
}
func (r privacyUsers) GetByID(_ context.Context, id int64) (*domain.User, error) {
	return &domain.User{ID: id}, nil
}

type privacyFriends struct {
	repository.FriendRequestRepository
	request   *domain.FriendRequest
	mutations int
}

func (r *privacyFriends) AreFriends(context.Context, int64, int64) (bool, error) { return false, nil }
func (r *privacyFriends) GetBetweenUsers(context.Context, int64, int64) (*domain.FriendRequest, error) {
	return r.request, nil
}
func (r *privacyFriends) GetByID(context.Context, int64) (*domain.FriendRequest, error) {
	return r.request, nil
}
func (r *privacyFriends) Create(context.Context, *domain.FriendRequest) (int64, error) {
	r.mutations++
	return 7, nil
}
func (r *privacyFriends) UpdateStatus(context.Context, int64, string) error {
	r.mutations++
	return nil
}

type privacyChats struct {
	repository.ChatRepository
	existing               *domain.Chat
	failLookup, failCreate bool
	created                int
}

func (r *privacyChats) GetPrivateChatBetween(context.Context, int64, int64) (*domain.Chat, error) {
	if r.failLookup {
		return nil, errPrivacyDB
	}
	return r.existing, nil
}
func (r *privacyChats) CreateWithMembers(context.Context, string, int64, int64) (*domain.Chat, error) {
	r.created++
	if r.failCreate {
		return nil, errPrivacyDB
	}
	return &domain.Chat{ID: 8}, nil
}

func TestFriendRequestPrivacy(t *testing.T) {
	for _, tc := range []struct {
		name     string
		allow    bool
		existing string
		fail     bool
		want     error
	}{
		{name: "allowed", allow: true},
		{name: "disabled", want: domain.ErrFriendRequestsDisabled},
		{name: "resend disabled", existing: domain.FriendRequestRejected, want: domain.ErrFriendRequestsDisabled},
		{name: "cross request disabled", existing: domain.FriendRequestPending, want: domain.ErrFriendRequestsDisabled},
		{name: "settings failure", fail: true, want: errPrivacyDB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			users := privacyUsers{settings: map[int64]*domain.UserSettings{2: {AllowFriendRequests: tc.allow, AllowDirectMessages: true}}}
			if tc.fail {
				users.failID = 2
			}
			friends := &privacyFriends{}
			if tc.existing != "" {
				friends.request = &domain.FriendRequest{ID: 7, FromUserID: 2, ToUserID: 1, Status: tc.existing}
			}
			chats := &privacyChats{}
			_, err := NewFriendService(friends, chats, users, privacyTransaction{friends, chats, users}).SendRequest(context.Background(), 1, 2)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error=%v want=%v", err, tc.want)
			}
			if tc.want != nil && (friends.mutations != 0 || chats.created != 0) {
				t.Fatal("denied request changed state")
			}
			if tc.want == nil && friends.mutations != 1 {
				t.Fatal("allowed request missing")
			}
		})
	}
}

func TestNewPrivateChatPrivacy(t *testing.T) {
	for _, mode := range []string{"accept", "cross request"} {
		for _, tc := range []struct {
			name                                   string
			disabledID, missingID, failID          int64
			existing, lookupFailure, createFailure bool
			withoutChat                            bool
			want                                   error
		}{
			{name: "allowed"},
			{name: "sender disabled", disabledID: 1, withoutChat: true},
			{name: "recipient disabled", disabledID: 2, withoutChat: true},
			{name: "missing settings", missingID: 2, withoutChat: true},
			{name: "sender settings failure", failID: 1, want: errPrivacyDB},
			{name: "recipient settings failure", failID: 2, want: errPrivacyDB},
			{name: "existing chat remains", disabledID: 1, existing: true},
			{name: "chat lookup failure", lookupFailure: true, want: errPrivacyDB},
			{name: "chat create failure returned", createFailure: true, want: errPrivacyDB},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				users := privacyUsers{failID: tc.failID, settings: map[int64]*domain.UserSettings{1: {AllowFriendRequests: true, AllowDirectMessages: true}, 2: {AllowFriendRequests: true, AllowDirectMessages: true}}}
				if tc.disabledID != 0 {
					users.settings[tc.disabledID].AllowDirectMessages = false
				}
				if tc.missingID != 0 {
					delete(users.settings, tc.missingID)
				}
				friends := &privacyFriends{request: &domain.FriendRequest{ID: 7, FromUserID: 1, ToUserID: 2, Status: domain.FriendRequestPending}}
				chats := &privacyChats{failLookup: tc.lookupFailure, failCreate: tc.createFailure}
				if tc.existing {
					chats.existing = &domain.Chat{ID: 8}
				}
				service := NewFriendService(friends, chats, users, privacyTransaction{friends, chats, users})
				var err error
				if mode == "accept" {
					_, err = service.AcceptRequest(context.Background(), 7, 2)
				} else {
					_, err = service.SendRequest(context.Background(), 2, 1)
				}
				if !errors.Is(err, tc.want) {
					t.Fatalf("error=%v want=%v", err, tc.want)
				}
				if tc.want != nil && !tc.createFailure && (friends.mutations != 0 || chats.created != 0) {
					t.Fatal("denied acceptance changed state")
				}
				if tc.want == nil {
					wantCreate := 1
					if tc.existing || tc.withoutChat {
						wantCreate = 0
					}
					if friends.mutations != 1 || chats.created != wantCreate {
						t.Fatalf("mutations=%d creates=%d", friends.mutations, chats.created)
					}
				}
			})
		}
	}
}

// These tests exercise service policy. PostgreSQL tests verify actual rollback/locking.
type privacyTransaction struct {
	friends *privacyFriends
	chats   *privacyChats
	users   privacyUsers
}

func (r privacyTransaction) WithPair(ctx context.Context, _, _ int64, fn func(repository.FriendRequestRepository, repository.ChatRepository, repository.UserRepository) error) error {
	return fn(r.friends, r.chats, r.users)
}
func (r *privacyFriends) ReopenRequest(context.Context, int64, int64, int64) error {
	r.mutations++
	return nil
}
