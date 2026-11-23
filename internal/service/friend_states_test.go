package service

import (
	"context"
	"errors"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
)

func TestFriendStateGuards(t *testing.T) {
	for _, tc := range []struct {
		name, status, action string
		want                 error
		mutations            int
	}{
		{name: "rejected cannot accept", status: domain.FriendRequestRejected, action: "accept", want: domain.ErrFriendRequestNotPending},
		{name: "accepted retry does not mutate", status: domain.FriendRequestAccepted, action: "accept"},
		{name: "accepted cannot reject", status: domain.FriendRequestAccepted, action: "reject", want: domain.ErrFriendRequestNotPending},
		{name: "rejected retry does not mutate", status: domain.FriendRequestRejected, action: "reject"},
		{name: "pending reject", status: domain.FriendRequestPending, action: "reject", mutations: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			users := privacyUsers{}
			friends := &privacyFriends{request: &domain.FriendRequest{ID: 7, FromUserID: 1, ToUserID: 2, Status: tc.status}}
			chats := &privacyChats{existing: &domain.Chat{ID: 8}}
			svc := NewFriendService(friends, chats, users, privacyTransaction{friends, chats, users})
			var err error
			if tc.action == "accept" {
				_, err = svc.AcceptRequest(context.Background(), 7, 2)
			} else {
				err = svc.RejectRequest(context.Background(), 7, 2)
			}
			if !errors.Is(err, tc.want) || friends.mutations != tc.mutations || chats.created != 0 {
				t.Fatalf("err=%v mutations=%d created=%d", err, friends.mutations, chats.created)
			}
		})
	}
}
