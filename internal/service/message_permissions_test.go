package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

func prepareMessagePermissions(t *testing.T, db *sql.DB, mode string) (*repository.PgMessageRepository, *repository.PgChatRepository, *domain.Chat) {
	t.Helper()
	chats, g := prepareGroupTransition(t, db)
	if err := chats.AddMember(context.Background(), g.ID, 2); err != nil {
		t.Fatal(err)
	}
	if mode == "demote" {
		if err := chats.SetMemberRole(context.Background(), g.ID, 2, "admin"); err != nil {
			t.Fatal(err)
		}
		if err := chats.SetAdminsOnlyWrite(context.Background(), g.ID, true); err != nil {
			t.Fatal(err)
		}
	}
	return repository.NewPgMessageRepository(db), chats, g
}
func messageMutation(mode string) string {
	switch mode {
	case "remove":
		return `DELETE FROM chat_members WHERE chat_id=$1 AND user_id=2`
	case "demote":
		return `UPDATE chat_members SET role='member' WHERE chat_id=$1 AND user_id=2`
	case "restrict":
		return `UPDATE chats SET admins_only_write=TRUE WHERE id=$1`
	default:
		panic("unknown mutation")
	}
}
func countMessages(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM messages`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("messages=%d want=%d", n, want)
	}
}

func TestMessageMutationPermissionsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	for _, mode := range []string{"remove", "demote", "restrict"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			messages, _, group := prepareMessagePermissions(t, db, mode)
			msg := &domain.Message{SenderID: 2, ChatID: group.ID, Content: "original"}
			if _, _, err := messages.SaveAuthorized(ctx, msg); err != nil {
				t.Fatal(err)
			}
			if err := messages.UpdateContent(ctx, msg.ID, 1, "different author"); !errors.Is(err, domain.ErrMessageNotFound) {
				t.Fatalf("editing someone else's message: %v", err)
			}
			if err := messages.UpdateContent(ctx, msg.ID, 2, "edited"); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(ctx, messageMutation(mode), group.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- messages.UpdateContent(ctx, msg.ID, 2, "must be denied") }()
			waiting := "SELECT role FROM chat_members"
			if mode == "restrict" {
				waiting = "SELECT id FROM chats"
			}
			waitForGroupLock(t, db, waiting)
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			want := domain.ErrChatWriteForbidden
			if mode == "remove" {
				want = domain.ErrNotChatMember
			}
			if err := <-done; !errors.Is(err, want) {
				t.Fatalf("edit after permission change: %v", err)
			}
			stored, err := messages.GetByID(ctx, msg.ID)
			if err != nil || stored.Content != "edited" {
				t.Fatalf("denied edit changed the message: %+v, %v", stored, err)
			}
			var wantDelete error
			if mode == "remove" {
				wantDelete = domain.ErrNotChatMember
			}
			if err := messages.SoftDelete(ctx, msg.ID, 2); !errors.Is(err, wantDelete) {
				t.Fatalf("delete after permission change: %v", err)
			}
		})
	}
}

func TestAnonymousGroupHistoryRetainsSendTimeMaskPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := t.Context()
	messages, chats, group := prepareMessagePermissions(t, db, "")
	write := func(content string) {
		t.Helper()
		if _, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 2, ChatID: group.ID, Content: content}); err != nil {
			t.Fatal(err)
		}
	}
	setAnonymous := func(on bool) {
		t.Helper()
		if err := chats.ChangeGroupPermissions(ctx, group.ID, 1, nil, nil, &on); err != nil {
			t.Fatal(err)
		}
	}
	write("public before")
	setAnonymous(true)
	write("anonymous")
	history, err := messages.GetHistory(ctx, group.ID, 0, 10)
	if err != nil || len(history) != 2 || history[0].SenderID != 0 || history[1].SenderID != 0 {
		t.Fatalf("anonymous history: %+v, %v", history, err)
	}
	setAnonymous(false)
	write("public after")
	history, err = messages.GetHistory(ctx, group.ID, 0, 10)
	if err != nil || len(history) != 3 {
		t.Fatalf("history: len=%d, %v", len(history), err)
	}
	if history[0].SenderID != 2 || history[1].SenderID != 0 || history[1].SenderName == history[0].SenderName || history[2].SenderID != 2 {
		t.Fatal("history did not preserve the anonymous author's mask")
	}
}

func TestMessagePermissionsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	for _, mode := range []string{"remove", "demote", "restrict"} {
		t.Run(mode+" commits first and denies queued save", func(t *testing.T) {
			messages, _, g := prepareMessagePermissions(t, db, mode)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.Exec(messageMutation(mode), g.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			msg := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "must be denied"}
			go func() { _, _, err := messages.SaveAuthorized(ctx, msg); done <- err }()
			waiting := "SELECT role FROM chat_members"
			if mode == "restrict" {
				waiting = "SELECT id FROM chats"
			}
			waitForGroupLock(t, db, waiting)
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			err = <-done
			want := domain.ErrChatWriteForbidden
			if mode == "remove" {
				want = domain.ErrNotChatMember
			}
			if !errors.Is(err, want) || msg.ID != 0 || !msg.CreatedAt.IsZero() {
				t.Fatalf("err=%v id=%d time=%v", err, msg.ID, msg.CreatedAt)
			}
			countMessages(t, db, 0)
		})
		t.Run("save commits before "+mode+"; later sends denied", func(t *testing.T) {
			messages, _, g := prepareMessagePermissions(t, db, mode)
			execFriendSQL(t, db, `CREATE FUNCTION pause_message_save() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(current_schema()),NEW.chat_id); RETURN NEW; END $$; CREATE TRIGGER pause_message_save BEFORE INSERT ON messages FOR EACH ROW EXECUTE FUNCTION pause_message_save()`)
			control, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer control.Rollback()
			if _, err = control.Exec(`SELECT pg_advisory_xact_lock(hashtext(current_schema()),$1::integer)`, g.ID); err != nil {
				t.Fatal(err)
			}
			saved := make(chan error, 1)
			msg := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "earlier authorized send"}
			go func() { _, _, err := messages.SaveAuthorized(ctx, msg); saved <- err }()
			waitForGroupLock(t, db, "INSERT INTO messages")
			mutated := make(chan error, 1)
			go func() { _, err := db.Exec(messageMutation(mode), g.ID); mutated <- err }()
			fragment := "UPDATE chat_members"
			if mode == "remove" {
				fragment = "DELETE FROM chat_members"
			}
			if mode == "restrict" {
				fragment = "UPDATE chats"
			}
			waitForGroupLock(t, db, fragment)
			if err = control.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-saved; err != nil {
				t.Fatal(err)
			}
			if err = <-mutated; err != nil {
				t.Fatal(err)
			}
			if msg.ID == 0 || msg.CreatedAt.IsZero() {
				t.Fatal("missing committed message metadata")
			}
			later := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "forbidden later send"}
			if _, _, err = messages.SaveAuthorized(ctx, later); err == nil {
				t.Fatal("later send permitted")
			}
			countMessages(t, db, 1)
			execFriendSQL(t, db, `DROP TRIGGER pause_message_save ON messages; DROP FUNCTION pause_message_save()`)
		})
	}
	for _, failure := range []string{"insert", "commit"} {
		t.Run(failure+" failure does not return successful message", func(t *testing.T) {
			messages, _, g := prepareMessagePermissions(t, db, "")
			constraint, event, deferral := "", "BEFORE INSERT", ""
			if failure == "commit" {
				constraint, event, deferral = "CONSTRAINT ", "AFTER INSERT", "DEFERRABLE INITIALLY DEFERRED"
			}
			execFriendSQL(t, db, `CREATE FUNCTION fail_message_save() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected save failure'; END $$; CREATE `+constraint+`TRIGGER fail_message_save `+event+` ON messages `+deferral+` FOR EACH ROW EXECUTE FUNCTION fail_message_save()`)
			msg := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "must rollback"}
			meta, recipients, err := messages.SaveAuthorized(ctx, msg)
			if err == nil || meta != nil || recipients != nil || msg.ID != 0 || !msg.CreatedAt.IsZero() {
				t.Fatal("uncommitted message reported as success")
			}
			countMessages(t, db, 0)
			execFriendSQL(t, db, `DROP TRIGGER fail_message_save ON messages; DROP FUNCTION fail_message_save()`)
		})
	}
	t.Run("parallel authorized sends remain concurrent and produce unique IDs", func(t *testing.T) {
		messages, _, g := prepareMessagePermissions(t, db, "")
		start := make(chan struct{})
		ids := make(chan int64, 12)
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			go func() {
				<-start
				msg := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "parallel"}
				_, _, err := messages.SaveAuthorized(ctx, msg)
				ids <- msg.ID
				errs <- err
			}()
		}
		close(start)
		unique := map[int64]bool{}
		for i := 0; i < 12; i++ {
			id := <-ids
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
			if id == 0 || unique[id] {
				t.Fatal("invalid or duplicate ID")
			}
			unique[id] = true
		}
		countMessages(t, db, 12)
	})
	t.Run("group role policy and private chat metadata", func(t *testing.T) {
		messages, chats, g := prepareMessagePermissions(t, db, "")
		if err := chats.SetAdminsOnlyWrite(ctx, g.ID, true); err != nil {
			t.Fatal(err)
		}
		if _, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 2, ChatID: g.ID, Content: "spoof", Type: "private"}); !errors.Is(err, domain.ErrChatWriteForbidden) {
			t.Fatal(err)
		}
		for _, sender := range []int64{1, 3} {
			if _, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: sender, ChatID: g.ID, Content: "authorized role"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 999, ChatID: g.ID, Content: "outsider"}); !errors.Is(err, domain.ErrNotChatMember) {
			t.Fatal(err)
		}
		if _, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 2, ChatID: 999, Content: "missing"}); !errors.Is(err, domain.ErrChatNotFound) {
			t.Fatal(err)
		}
		private, err := chats.CreateWithMembers(ctx, "private", 1, 2)
		if err != nil {
			t.Fatal(err)
		}
		meta, recipients, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 2, ChatID: private.ID, Content: "private", Type: "group"})
		if err != nil || meta.Type != "private" || len(recipients) != 2 {
			t.Fatalf("private: %+v %v %v", meta, recipients, err)
		}
		countMessages(t, db, 3)
	})
	t.Run("anonymous metadata changes are rechecked after lock wait", func(t *testing.T) {
		messages, _, g := prepareMessagePermissions(t, db, "")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`UPDATE chats SET anonymous_mode=TRUE,name='Anonymous',avatar_url='anon.png' WHERE id=$1`, g.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			meta, _, err := messages.SaveAuthorized(ctx, &domain.Message{SenderID: 2, ChatID: g.ID, Content: "anon"})
			if err == nil && (!meta.AnonymousMode || meta.Name != "Anonymous" || meta.AvatarURL != "anon.png") {
				err = errors.New("stale chat metadata")
			}
			done <- err
		}()
		waitForGroupLock(t, db, "SELECT id FROM chats")
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		var sender int64
		if err = db.QueryRow(`SELECT sender_id FROM messages`).Scan(&sender); err != nil || sender != 2 {
			t.Fatal("stored sender lost", err)
		}
	})
	t.Run("cancellation while waiting for policy lock saves nothing", func(t *testing.T) {
		messages, _, g := prepareMessagePermissions(t, db, "")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`SELECT id FROM chats WHERE id=$1 FOR UPDATE`, g.ID); err != nil {
			t.Fatal(err)
		}
		timeout, cancel := context.WithTimeout(ctx, 70*time.Millisecond)
		defer cancel()
		msg := &domain.Message{SenderID: 2, ChatID: g.ID, Content: "canceled"}
		if _, _, err = messages.SaveAuthorized(timeout, msg); err == nil || msg.ID != 0 {
			t.Fatal("canceled save succeeded")
		}
		tx.Rollback()
		countMessages(t, db, 0)
	})
}
