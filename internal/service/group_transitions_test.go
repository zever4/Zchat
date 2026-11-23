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

func prepareGroupTransition(t *testing.T, db *sql.DB) (*repository.PgChatRepository, *domain.Chat) {
	t.Helper()
	resetFriends(t, db)
	r := repository.NewPgChatRepository(db)
	g, err := r.CreateGroup(context.Background(), 1, "transaction group", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = r.AddMember(context.Background(), g.ID, 3); err != nil {
		t.Fatal(err)
	}
	if err = r.SetMemberRole(context.Background(), g.ID, 3, "admin"); err != nil {
		t.Fatal(err)
	}
	_, status, err := r.JoinGroupByInvite(context.Background(), g.InviteCode, 2)
	if err != nil || status != "pending" {
		t.Fatalf("join: %s %v", status, err)
	}
	return r, g
}
func groupTransitionState(t *testing.T, db *sql.DB, g int64, status string, member bool, decisions int) {
	t.Helper()
	var gotStatus string
	var gotMember bool
	var gotCount int
	if err := db.QueryRow(`SELECT status FROM group_join_requests WHERE chat_id=$1 AND user_id=2`, g).Scan(&gotStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_members WHERE chat_id=$1 AND user_id=2)`, g).Scan(&gotMember); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM group_audit_logs WHERE chat_id=$1 AND action IN ('join_request_approved','join_request_rejected')`, g).Scan(&gotCount); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || gotMember != member || gotCount != decisions {
		t.Fatalf("status=%s member=%t decisions=%d; want %s/%t/%d", gotStatus, gotMember, gotCount, status, member, decisions)
	}
}
func waitForGroupLock(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND query ILIKE $1`, "%"+query+"%").Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected database lock wait for " + query)
}

func TestGroupTransitionsPostgres(t *testing.T) {
	db, _ := friendPostgres(t) // Reuse the isolated-schema/all-migrations fixture.
	ctx := context.Background()
	for _, failure := range []string{"status", "audit", "commit"} {
		t.Run("approval rollback on "+failure+" failure", func(t *testing.T) {
			r, g := prepareGroupTransition(t, db)
			table, event, deferral := "group_join_requests", "BEFORE UPDATE", ""
			if failure == "audit" {
				table, event = "group_audit_logs", "BEFORE INSERT"
			}
			constraint := ""
			if failure == "commit" {
				event, deferral, constraint = "AFTER UPDATE", "DEFERRABLE INITIALLY DEFERRED", "CONSTRAINT "
			}
			execFriendSQL(t, db, `CREATE FUNCTION fail_group_transition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected group failure'; END $$; CREATE `+constraint+`TRIGGER fail_group_transition `+event+` ON `+table+` `+deferral+` FOR EACH ROW EXECUTE FUNCTION fail_group_transition()`)
			if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true); err == nil {
				t.Fatal("failed approval returned success")
			}
			groupTransitionState(t, db, g.ID, "pending", false, 0)
			execFriendSQL(t, db, `DROP TRIGGER fail_group_transition ON `+table+`; DROP FUNCTION fail_group_transition()`)
			if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true); err != nil {
				t.Fatal(err)
			}
			groupTransitionState(t, db, g.ID, "approved", true, 1)
		})
	}
	t.Run("reject audit failure rolls back", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		execFriendSQL(t, db, `CREATE FUNCTION fail_group_reject() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected rejection audit failure'; END $$; CREATE TRIGGER fail_group_reject BEFORE INSERT ON group_audit_logs FOR EACH ROW EXECUTE FUNCTION fail_group_reject()`)
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, false); err == nil {
			t.Fatal("failed rejection succeeded")
		}
		groupTransitionState(t, db, g.ID, "pending", false, 0)
		execFriendSQL(t, db, `DROP TRIGGER fail_group_reject ON group_audit_logs; DROP FUNCTION fail_group_reject()`)
	})
	t.Run("parallel owner admin approval retries create one audit", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		start := make(chan struct{})
		errs := make(chan error, 16)
		for i := 0; i < 16; i++ {
			actor := int64(1)
			if i%2 == 0 {
				actor = 3
			}
			go func(actor int64) { <-start; errs <- r.DecideGroupJoinRequest(ctx, g.ID, actor, 2, true) }(actor)
		}
		close(start)
		for i := 0; i < 16; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		groupTransitionState(t, db, g.ID, "approved", true, 1)
	})
	t.Run("approve reject race has one consistent winner", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			r, g := prepareGroupTransition(t, db)
			start := make(chan struct{})
			errs := make(chan error, 2)
			go func() { <-start; errs <- r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true) }()
			go func() { <-start; errs <- r.DecideGroupJoinRequest(ctx, g.ID, 3, 2, false) }()
			close(start)
			a, b := <-errs, <-errs
			if (a == nil) == (b == nil) {
				t.Fatalf("expected one winner: %v / %v", a, b)
			}
			for _, err := range []error{a, b} {
				if err != nil && !errors.Is(err, domain.ErrJoinRequestNotPending) {
					t.Fatal(err)
				}
			}
			req, err := r.GetGroupJoinRequest(ctx, g.ID, 2)
			if err != nil {
				t.Fatal(err)
			}
			groupTransitionState(t, db, g.ID, req.Status, req.Status == "approved", 1)
		}
	})
	t.Run("join retry racing approval cannot reset approved status", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		start := make(chan struct{})
		errs := make(chan error, 9)
		for i := 0; i < 8; i++ {
			go func() { <-start; _, _, err := r.JoinGroupByInvite(ctx, g.InviteCode, 2); errs <- err }()
		}
		go func() { <-start; errs <- r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true) }()
		close(start)
		for i := 0; i < 9; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		groupTransitionState(t, db, g.ID, "approved", true, 1)
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM group_audit_logs WHERE action='join_request_created'`).Scan(&n); err != nil || n != 1 {
			t.Fatalf("duplicate request audit %d %v", n, err)
		}
	})
	t.Run("authorization and missing request guards", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		for _, actor := range []int64{2, 999} {
			if err := r.DecideGroupJoinRequest(ctx, g.ID, actor, 2, true); !errors.Is(err, domain.ErrNotGroupOwner) {
				t.Fatal(err)
			}
		}
		if err := r.SetMemberRole(ctx, g.ID, 3, "member"); err != nil {
			t.Fatal(err)
		}
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 3, 2, true); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal(err)
		}
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 999, true); !errors.Is(err, domain.ErrJoinRequestNotFound) {
			t.Fatal(err)
		}
		if err := r.DecideGroupJoinRequest(ctx, 999, 1, 2, true); !errors.Is(err, domain.ErrChatNotFound) {
			t.Fatal(err)
		}
		groupTransitionState(t, db, g.ID, "pending", false, 0)
	})
	t.Run("rejected retry and new application; old approval cannot restore removed member", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		for i := 0; i < 2; i++ {
			if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, false); err != nil {
				t.Fatal(err)
			}
		}
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true); !errors.Is(err, domain.ErrJoinRequestNotPending) {
			t.Fatal(err)
		}
		groupTransitionState(t, db, g.ID, "rejected", false, 1)
		if _, status, err := r.JoinGroupByInvite(ctx, g.InviteCode, 2); err != nil || status != "pending" {
			t.Fatalf("new application: %s %v", status, err)
		}
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true); err != nil {
			t.Fatal(err)
		}
		if err := r.RemoveMember(ctx, g.ID, 2); err != nil {
			t.Fatal(err)
		}
		if err := r.DecideGroupJoinRequest(ctx, g.ID, 1, 2, true); !errors.Is(err, domain.ErrJoinRequestNotPending) {
			t.Fatal("restored removed member", err)
		}
		groupTransitionState(t, db, g.ID, "approved", false, 2)
	})
	for _, mutation := range []string{"demote", "remove"} {
		t.Run(mutation+" committed before approval denies stale admin", func(t *testing.T) {
			r, g := prepareGroupTransition(t, db)
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			query := `UPDATE chat_members SET role='member' WHERE chat_id=$1 AND user_id=3`
			if mutation == "remove" {
				query = `DELETE FROM chat_members WHERE chat_id=$1 AND user_id=3`
			}
			if _, err = tx.Exec(query, g.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- r.DecideGroupJoinRequest(ctx, g.ID, 3, 2, true) }()
			waitForGroupLock(t, db, "SELECT role FROM chat_members")
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, domain.ErrNotGroupOwner) {
				t.Fatal(err)
			}
			groupTransitionState(t, db, g.ID, "pending", false, 0)
		})
	}
	t.Run("approval holds admin role until decision commits", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		execFriendSQL(t, db, `CREATE FUNCTION pause_group_approval() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(current_schema()),NEW.chat_id); RETURN NEW; END $$; CREATE TRIGGER pause_group_approval BEFORE UPDATE ON group_join_requests FOR EACH ROW EXECUTE FUNCTION pause_group_approval()`)
		control, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer control.Rollback()
		if _, err = control.Exec(`SELECT pg_advisory_xact_lock(hashtext(current_schema()),$1::integer)`, g.ID); err != nil {
			t.Fatal(err)
		}
		approved := make(chan error, 1)
		go func() { approved <- r.DecideGroupJoinRequest(ctx, g.ID, 3, 2, true) }()
		waitForGroupLock(t, db, "UPDATE group_join_requests")
		demoted := make(chan error, 1)
		go func() { demoted <- r.SetMemberRole(ctx, g.ID, 3, "member") }()
		waitForGroupLock(t, db, "UPDATE chat_members")
		if err = control.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-approved; err != nil {
			t.Fatal(err)
		}
		if err = <-demoted; err != nil {
			t.Fatal(err)
		}
		if err = r.DecideGroupJoinRequest(ctx, g.ID, 3, 2, true); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal("demoted retry authorized", err)
		}
		groupTransitionState(t, db, g.ID, "approved", true, 1)
		execFriendSQL(t, db, `DROP TRIGGER pause_group_approval ON group_join_requests; DROP FUNCTION pause_group_approval()`)
	})
	t.Run("cancellation while waiting for group lock leaves pending", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
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
		if err = r.DecideGroupJoinRequest(timeout, g.ID, 1, 2, true); err == nil {
			t.Fatal("canceled approval succeeded")
		}
		tx.Rollback()
		groupTransitionState(t, db, g.ID, "pending", false, 0)
	})
	t.Run("public join audit failure rolls back member and request", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		if err := r.UpdateGroupSettings(ctx, g.ID, 1, g.Name, true); err != nil {
			t.Fatal(err)
		}
		execFriendSQL(t, db, `CREATE FUNCTION fail_public_join() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected public join audit failure'; END $$; CREATE TRIGGER fail_public_join BEFORE INSERT ON group_audit_logs FOR EACH ROW EXECUTE FUNCTION fail_public_join()`)
		group, status, err := r.JoinGroupByInvite(ctx, g.InviteCode, 2)
		if err == nil || group != nil || status != "" {
			t.Fatal("failed join returned success")
		}
		groupTransitionState(t, db, g.ID, "pending", false, 0)
		execFriendSQL(t, db, `DROP TRIGGER fail_public_join ON group_audit_logs; DROP FUNCTION fail_public_join()`)
		for i := 0; i < 2; i++ {
			if _, status, err = r.JoinGroupByInvite(ctx, g.InviteCode, 2); err != nil || status != "joined" {
				t.Fatalf("public join: %s %v", status, err)
			}
		}
		groupTransitionState(t, db, g.ID, "approved", true, 0)
	})
	t.Run("revoked invite cannot create request", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		execFriendSQL(t, db, `UPDATE group_invite_links SET is_active=FALSE`)
		if group, _, err := r.JoinGroupByInvite(ctx, g.InviteCode, 2); !errors.Is(err, domain.ErrChatNotFound) || group != nil {
			t.Fatal(err)
		}
		groupTransitionState(t, db, g.ID, "pending", false, 0)
	})
	t.Run("public private change before join is rechecked", func(t *testing.T) {
		r, g := prepareGroupTransition(t, db)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`UPDATE chats SET is_public=TRUE WHERE id=$1`, g.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, status, err := r.JoinGroupByInvite(ctx, g.InviteCode, 2)
			if err == nil && status != "joined" {
				err = errors.New("used stale private mode")
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
		groupTransitionState(t, db, g.ID, "approved", true, 0)
	})
}
