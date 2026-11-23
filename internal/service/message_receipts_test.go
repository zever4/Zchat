package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

const receiptID = "12345678-1234-4321-8123-123456789abc"

func TestMessageReceiptsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	t.Run("concurrent retries commit one row", func(t *testing.T) {
		repo, _, group := prepareMessagePermissions(t, db, "")
		var wg sync.WaitGroup
		saved := make(chan *domain.Message, 12)
		errs := make(chan error, 12)
		for range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				msg := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "same send", ClientMessageID: receiptID}
				_, _, err := repo.SaveAuthorized(ctx, msg)
				if err != nil {
					errs <- err
				} else {
					saved <- msg
				}
			}()
		}
		wg.Wait()
		close(errs)
		close(saved)
		for err := range errs {
			t.Fatal(err)
		}
		var id int64
		var created time.Time
		fresh := 0
		for msg := range saved {
			if id == 0 {
				id, created = msg.ID, msg.CreatedAt
			}
			if msg.ID != id || !msg.CreatedAt.Equal(created) {
				t.Fatal("retry changed receipt")
			}
			if !msg.Replayed {
				fresh++
			}
		}
		if fresh != 1 {
			t.Fatalf("fresh writes=%d", fresh)
		}
		countMessages(t, db, 1)
		different := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "changed", ClientMessageID: receiptID}
		if _, _, err := repo.SaveAuthorized(ctx, different); !errors.Is(err, domain.ErrMessageIDConflict) || different.ID != 0 {
			t.Fatalf("ID content collision accepted: %v", err)
		}
		// UUID uniqueness belongs to the sender, not the whole installation.
		other := &domain.Message{SenderID: 1, ChatID: group.ID, Content: "other account", ClientMessageID: receiptID}
		if _, _, err := repo.SaveAuthorized(ctx, other); err != nil || other.ID == id || other.Replayed {
			t.Fatal("sender namespace", err)
		}
		countMessages(t, db, 2)
	})
	t.Run("edited and deleted messages are never restored", func(t *testing.T) {
		repo, _, group := prepareMessagePermissions(t, db, "")
		original := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "original", ClientMessageID: receiptID}
		if _, _, err := repo.SaveAuthorized(ctx, original); err != nil {
			t.Fatal(err)
		}
		for _, remove := range []bool{false, true} {
			if remove {
				if err := repo.SoftDelete(ctx, original.ID, 2); err != nil {
					t.Fatal(err)
				}
			} else if err := repo.UpdateContent(ctx, original.ID, 2, "edited"); err != nil {
				t.Fatal(err)
			}
			retry := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "original", ClientMessageID: receiptID}
			if _, _, err := repo.SaveAuthorized(ctx, retry); err != nil || !retry.Replayed || retry.ID != original.ID {
				t.Fatal("retry", err)
			}
			var text string
			var deleted bool
			if err := db.QueryRow(`SELECT content,is_deleted FROM messages WHERE id=$1`, original.ID).Scan(&text, &deleted); err != nil {
				t.Fatal(err)
			}
			if text != "edited" || deleted != remove {
				t.Fatal("retry resurrected old message")
			}
		}
		countMessages(t, db, 1)
	})
	t.Run("chat collision and revoked membership are rejected", func(t *testing.T) {
		repo, chats, group := prepareMessagePermissions(t, db, "")
		original := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "original", ClientMessageID: receiptID}
		if _, _, err := repo.SaveAuthorized(ctx, original); err != nil {
			t.Fatal(err)
		}
		other, err := chats.CreateWithMembers(ctx, "private", 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		retry := &domain.Message{SenderID: 2, ChatID: other.ID, Content: "original", ClientMessageID: receiptID}
		if _, _, err = repo.SaveAuthorized(ctx, retry); !errors.Is(err, domain.ErrMessageIDConflict) {
			t.Fatal("cross-chat replay", err)
		}
		if _, err = db.Exec(`DELETE FROM chat_members WHERE user_id=2 AND chat_id=$1`, group.ID); err != nil {
			t.Fatal(err)
		}
		retry.ChatID = group.ID
		if _, _, err = repo.SaveAuthorized(ctx, retry); !errors.Is(err, domain.ErrNotChatMember) {
			t.Fatal("revoked retry", err)
		}
		countMessages(t, db, 1)
	})
	t.Run("timeout before commit leaves no receipt and can retry", func(t *testing.T) {
		repo, _, group := prepareMessagePermissions(t, db, "")
		lock, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback()
		if _, err = lock.Exec(`SELECT id FROM chats WHERE id=$1 FOR UPDATE`, group.ID); err != nil {
			t.Fatal(err)
		}
		timeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		msg := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "retry", ClientMessageID: receiptID}
		if _, _, err = repo.SaveAuthorized(timeout, msg); err == nil || msg.ID != 0 {
			t.Fatal("early acknowledgement", err)
		}
		if err = lock.Rollback(); err != nil {
			t.Fatal(err)
		}
		if _, _, err = repo.SaveAuthorized(ctx, msg); err != nil || msg.ID == 0 || msg.Replayed {
			t.Fatal("retry after rollback", err)
		}
		countMessages(t, db, 1)
	})
	t.Run("legacy clients keep independent inserts", func(t *testing.T) {
		repo, _, group := prepareMessagePermissions(t, db, "")
		for range 2 {
			msg := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "legacy"}
			if _, _, err := repo.SaveAuthorized(ctx, msg); err != nil || msg.Replayed {
				t.Fatal(err)
			}
		}
		countMessages(t, db, 2)
	})
}
