package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

func TestAttachmentsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	attachments := repository.NewPgAttachmentRepository(db)
	newItem := func(chatID int64) *domain.Attachment {
		return &domain.Attachment{Path: "/uploads/media/test.png", ChatID: chatID, OwnerID: 2, Name: "test.png", MIME: "image/png", Kind: "media", Size: 32}
	}
	t.Run("bound to chat and current members only", func(t *testing.T) {
		_, chats, g := prepareMessagePermissions(t, db, "remove")
		a := newItem(g.ID)
		if err := attachments.CreateAuthorized(ctx, a); err != nil {
			t.Fatal(err)
		}
		for _, user := range []int64{1, 2} {
			got, err := attachments.GetForUser(ctx, a.Path, user)
			if err != nil || got.ChatID != g.ID || got.Size != 32 {
				t.Fatal(got, err)
			}
		}
		if _, err := attachments.GetForUser(ctx, a.Path, 999); !errors.Is(err, domain.ErrMessageNotFound) {
			t.Fatal("outsider read", err)
		}
		if err := chats.RemoveMember(ctx, g.ID, 2); err != nil {
			t.Fatal(err)
		}
		if _, err := attachments.GetForUser(ctx, a.Path, 2); !errors.Is(err, domain.ErrMessageNotFound) {
			t.Fatal("removed uploader read", err)
		}
	})
	t.Run("upload policy rejects nonmember and restricted member", func(t *testing.T) {
		_, chats, g := prepareMessagePermissions(t, db, "demote")
		a := newItem(g.ID)
		if err := chats.SetMemberRole(ctx, g.ID, 2, "member"); err != nil {
			t.Fatal(err)
		}
		a.OwnerID = 999
		if err := attachments.CreateAuthorized(ctx, a); !errors.Is(err, domain.ErrNotChatMember) {
			t.Fatal(err)
		}
		a.OwnerID = 2
		if err := attachments.CreateAuthorized(ctx, a); !errors.Is(err, domain.ErrChatWriteForbidden) {
			t.Fatal(err)
		}
		a.OwnerID = 1
		if err := attachments.CreateAuthorized(ctx, a); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("pending publication rechecks removal", func(t *testing.T) {
		_, _, g := prepareMessagePermissions(t, db, "remove")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.ExecContext(ctx, `DELETE FROM chat_members WHERE chat_id=$1 AND user_id=2`, g.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- attachments.CreateAuthorized(ctx, newItem(g.ID)) }()
		waitForGroupLock(t, db, `SELECT role FROM chat_members`)
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; !errors.Is(err, domain.ErrNotChatMember) {
			t.Fatal(err)
		}
	})
	t.Run("SQL failure rolls back publication", func(t *testing.T) {
		_, _, g := prepareMessagePermissions(t, db, "remove")
		a := newItem(g.ID)
		a.Size = -1
		if err := attachments.CreateAuthorized(ctx, a); err == nil {
			t.Fatal("bad metadata committed")
		}
		if _, err := attachments.GetForUser(ctx, a.Path, 1); !errors.Is(err, domain.ErrMessageNotFound) {
			t.Fatal("failed publication visible", err)
		}
	})
	t.Run("new messages cannot borrow an attachment from another chat", func(t *testing.T) {
		messages, chats, g := prepareMessagePermissions(t, db, "remove")
		a := newItem(g.ID)
		if err := attachments.CreateAuthorized(ctx, a); err != nil {
			t.Fatal(err)
		}
		private, err := chats.CreateWithMembers(ctx, "private", 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, tag := range []string{"[img]%s[/img]", "[file:test.png:32]%s[/file]", "[voice]%s[/voice]"} {
			msg := &domain.Message{SenderID: 2, ChatID: private.ID, Content: fmt.Sprintf(tag, a.Path)}
			if _, _, err = messages.SaveAuthorized(ctx, msg); !errors.Is(err, domain.ErrInvalidInput) || msg.ID != 0 {
				t.Fatal("cross-chat attachment saved", err)
			}
			msg.ChatID = g.ID
			if _, _, err = messages.SaveAuthorized(ctx, msg); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("concurrent user avatars return actual predecessor and preserve bio", func(t *testing.T) {
		resetFriends(t, db)
		users := repository.NewPgUserRepository(db)
		_, err := db.Exec(`UPDATE users SET bio='keep bio',avatar_url='old' WHERE id=1`)
		if err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		old := make(chan string, 8)
		fail := make(chan error, 8)
		for i := range 8 {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				s, e := users.ReplaceAvatar(ctx, 1, fmt.Sprintf("next%d", i))
				old <- s
				fail <- e
			}(i)
		}
		wg.Wait()
		close(old)
		close(fail)
		for e := range fail {
			if e != nil {
				t.Fatal(e)
			}
		}
		seen := map[string]bool{}
		for s := range old {
			if seen[s] {
				t.Fatal("same old avatar returned twice", s)
			}
			seen[s] = true
		}
		user, err := users.GetByID(ctx, 1)
		if err != nil || user.Bio != "keep bio" || seen[user.AvatarURL] || !seen["old"] {
			t.Fatal("replacement chain/bio incorrect", user, err)
		}
	})
	t.Run("group avatar revoked actor cannot replace", func(t *testing.T) {
		chats, g := prepareGroupTransition(t, db)
		if _, err := chats.ReplaceGroupAvatar(ctx, g.ID, 1, "initial"); err != nil {
			t.Fatal(err)
		}
		if err := chats.SetMemberRole(ctx, g.ID, 3, "member"); err != nil {
			t.Fatal(err)
		}
		if old, err := chats.ReplaceGroupAvatar(ctx, g.ID, 3, "forbidden"); !errors.Is(err, domain.ErrNotGroupOwner) || old != "" {
			t.Fatal(old, err)
		}
		if old, err := chats.ReplaceGroupAvatar(ctx, g.ID, 1, "new"); err != nil || old != "initial" {
			t.Fatal(old, err)
		}
	})
}
