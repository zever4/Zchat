package service

import (
	"context"
	"errors"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

func TestHistoryCursorOrderPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	resetFriends(t, db)
	execFriendSQL(t, db, `INSERT INTO chats(id,type) VALUES(1,'private');
INSERT INTO chat_members(chat_id,user_id) VALUES(1,1),(1,2);
INSERT INTO messages(id,chat_id,sender_id,content,created_at) VALUES
(1,1,1,'one','2026-01-03'),(2,1,1,'two','2026-01-01'),(3,1,1,'three','2026-01-02'),(4,1,1,'four','2026-01-02');`)
	repo := repository.NewPgMessageRepository(db)
	var ids []int64
	var cursor int64
	for range 4 {
		page, err := repo.GetHistory(context.Background(), 1, cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		for _, msg := range page {
			ids = append(ids, msg.ID)
		}
		cursor = page[0].ID
	}
	want := []int64{3, 4, 1, 2}
	if len(ids) != len(want) {
		t.Fatalf("cursor pagination lost/duplicated messages: %v; want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("unstable cursor pagination: %v; want %v", ids, want)
		}
	}
}

func TestHistoryPagesPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	resetFriends(t, db)
	execFriendSQL(t, db, `INSERT INTO chats(id,type) VALUES(1,'private');
INSERT INTO chat_members(chat_id,user_id) VALUES(1,1),(1,2);
INSERT INTO messages(id,chat_id,sender_id,content) SELECT i,1,1,'message-'||i FROM generate_series(1,105) i;`)
	repo := repository.NewPgMessageRepository(db)
	svc := NewChatService(repository.NewPgChatRepository(db), repo)
	for _, tc := range []struct {
		name         string
		cursor       int64
		limit, count int
		first, last  int64
		more         bool
	}{
		{"default", 0, 0, 50, 56, 105, true},
		{"maximum", 0, 100, 100, 6, 105, true},
		{"next", 56, 50, 50, 6, 55, true},
		{"last short", 6, 50, 5, 1, 5, false},
		{"last exactly full", 6, 5, 5, 1, 5, false},
		{"empty", 1, 5, 0, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page, more, err := svc.GetChatHistory(context.Background(), 1, tc.cursor, 1, tc.limit)
			if err != nil || len(page) != tc.count || more != tc.more {
				t.Fatalf("count=%d more=%v error=%v", len(page), more, err)
			}
			if len(page) > 0 && (page[0].ID != tc.first || page[len(page)-1].ID != tc.last) {
				t.Fatalf("wrong page endpoints: %d..%d", page[0].ID, page[len(page)-1].ID)
			}
		})
	}
	for _, limit := range []int{-1, 101, 1 << 30} {
		if _, _, err := svc.GetChatHistory(context.Background(), 1, 0, 1, limit); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatalf("invalid limit %d accepted: %v", limit, err)
		}
	}
	if _, _, err := svc.GetChatHistory(context.Background(), 1, 0, 3, 50); !errors.Is(err, domain.ErrNotChatMember) {
		t.Fatal("outsider history allowed", err)
	}
	for _, limit := range []int{-1, 0, 102} {
		if _, err := repo.GetHistory(context.Background(), 1, 0, limit); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal("repository accepted unbounded limit", limit, err)
		}
	}
}
