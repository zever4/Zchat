package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
)

func TestGroupManagerPermissionsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	for _, operation := range []string{"kick", "permissions", "settings", "avatar", "create invite", "activate invite", "delete invite", "regenerate invite"} {
		t.Run("stale admin cannot "+operation, func(t *testing.T) {
			_, r, g := prepareMessagePermissions(t, db, "")
			var inviteID int64
			if err := db.QueryRow(`SELECT id FROM group_invite_links WHERE chat_id=$1`, g.ID).Scan(&inviteID); err != nil {
				t.Fatal(err)
			}
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err = tx.Exec(`UPDATE chat_members SET role='member' WHERE chat_id=$1 AND user_id=3`, g.ID); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				var err error
				yes := true
				switch operation {
				case "kick":
					err = r.KickGroupMember(ctx, g.ID, 3, 2)
				case "permissions":
					err = r.ChangeGroupPermissions(ctx, g.ID, 3, &yes, nil, nil)
				case "settings":
					err = r.UpdateGroupSettings(ctx, g.ID, 3, "unauthorized", true)
				case "avatar":
					err = r.UpdateGroupAvatar(ctx, g.ID, 3, "unauthorized.png")
				case "create invite":
					_, err = r.CreateGroupInviteLink(ctx, g.ID, 3)
				case "activate invite":
					err = r.SetGroupInviteActive(ctx, g.ID, inviteID, 3, false)
				case "delete invite":
					err = r.DeleteGroupInviteLink(ctx, g.ID, inviteID, 3)
				case "regenerate invite":
					_, err = r.RegenerateGroupInviteCode(ctx, g.ID, 3)
				}
				done <- err
			}()
			waitForGroupLock(t, db, "SELECT role FROM chat_members")
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err = <-done; !errors.Is(err, domain.ErrNotGroupOwner) {
				t.Fatalf("operation authorized after demotion: %v", err)
			}
			assertUnchangedGroup(t, db, g.ID)
		})
	}
	t.Run("target promoted before admin kick cannot be removed", func(t *testing.T) {
		_, r, g := prepareMessagePermissions(t, db, "")
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`UPDATE chat_members SET role='admin' WHERE chat_id=$1 AND user_id=2`, g.ID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- r.KickGroupMember(ctx, g.ID, 3, 2) }()
		waitForGroupLock(t, db, "SELECT role FROM chat_members")
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal(err)
		}
		assertUnchangedGroup(t, db, g.ID)
	})
	for _, operation := range []string{"kick", "role", "permissions"} {
		t.Run(operation+" audit error rolls back mutation", func(t *testing.T) {
			_, r, g := prepareMessagePermissions(t, db, "")
			execFriendSQL(t, db, `CREATE FUNCTION fail_manager_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected audit failure'; END $$; CREATE TRIGGER fail_manager_audit BEFORE INSERT ON group_audit_logs FOR EACH ROW EXECUTE FUNCTION fail_manager_audit()`)
			yes := true
			var err error
			switch operation {
			case "kick":
				err = r.KickGroupMember(ctx, g.ID, 1, 2)
			case "role":
				err = r.ChangeGroupMemberRole(ctx, g.ID, 1, 2, "admin")
			case "permissions":
				err = r.ChangeGroupPermissions(ctx, g.ID, 1, &yes, nil, nil)
			}
			if err == nil {
				t.Fatal("failed audit returned success")
			}
			assertUnchangedGroup(t, db, g.ID)
			role, err := r.GetMemberRole(ctx, g.ID, 2)
			if err != nil || role != "member" {
				t.Fatal("role changed despite failure", err)
			}
			execFriendSQL(t, db, `DROP TRIGGER fail_manager_audit ON group_audit_logs; DROP FUNCTION fail_manager_audit()`)
		})
	}
	t.Run("authorized permissions update holds actor role until commit", func(t *testing.T) {
		_, r, g := prepareMessagePermissions(t, db, "")
		execFriendSQL(t, db, `CREATE FUNCTION pause_manager_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(current_schema()),NEW.chat_id); RETURN NEW; END $$; CREATE TRIGGER pause_manager_audit BEFORE INSERT ON group_audit_logs FOR EACH ROW EXECUTE FUNCTION pause_manager_audit()`)
		control, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer control.Rollback()
		if _, err = control.Exec(`SELECT pg_advisory_xact_lock(hashtext(current_schema()),$1::integer)`, g.ID); err != nil {
			t.Fatal(err)
		}
		yes := true
		changed := make(chan error, 1)
		go func() { changed <- r.ChangeGroupPermissions(ctx, g.ID, 3, &yes, nil, nil) }()
		waitForGroupLock(t, db, "INSERT INTO group_audit_logs")
		demoted := make(chan error, 1)
		go func() { demoted <- r.SetMemberRole(ctx, g.ID, 3, "member") }()
		waitForGroupLock(t, db, "UPDATE chat_members")
		if err = control.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-changed; err != nil {
			t.Fatal(err)
		}
		if err = <-demoted; err != nil {
			t.Fatal(err)
		}
		no := false
		if err = r.ChangeGroupPermissions(ctx, g.ID, 3, &no, nil, nil); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal(err)
		}
		meta, err := r.GetChatByID(ctx, g.ID)
		if err != nil || !meta.AdminsOnlyWrite {
			t.Fatal("later unauthorized update succeeded", err)
		}
		execFriendSQL(t, db, `DROP TRIGGER pause_manager_audit ON group_audit_logs; DROP FUNCTION pause_manager_audit()`)
	})
	t.Run("role guards and safe owner retries", func(t *testing.T) {
		_, r, g := prepareMessagePermissions(t, db, "")
		if err := r.ChangeGroupMemberRole(ctx, g.ID, 3, 2, "admin"); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal("admin changed roles", err)
		}
		if err := r.ChangeGroupMemberRole(ctx, g.ID, 1, 1, "member"); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal(err)
		}
		if err := r.ChangeGroupMemberRole(ctx, g.ID, 1, 2, "owner"); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal(err)
		}
		if err := r.KickGroupMember(ctx, g.ID, 3, 1); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal("owner kicked", err)
		}
		for i := 0; i < 2; i++ {
			if err := r.ChangeGroupMemberRole(ctx, g.ID, 1, 2, "admin"); err != nil {
				t.Fatal(err)
			}
		}
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM group_audit_logs WHERE action='promoted_to_admin'`).Scan(&n); err != nil || n != 1 {
			t.Fatal("duplicate role audit", n, err)
		}
		if err := r.KickGroupMember(ctx, g.ID, 3, 2); !errors.Is(err, domain.ErrNotGroupOwner) {
			t.Fatal("admin kicked admin", err)
		}
		if err := r.KickGroupMember(ctx, g.ID, 1, 2); err != nil {
			t.Fatal(err)
		}
		if err := r.KickGroupMember(ctx, g.ID, 1, 2); !errors.Is(err, domain.ErrNotChatMember) {
			t.Fatal("missing member", err)
		}
	})
	t.Run("invite replacement SQL failure rolls back new invite", func(t *testing.T) {
		_, r, g := prepareMessagePermissions(t, db, "")
		execFriendSQL(t, db, `CREATE FUNCTION fail_invite_pointer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected pointer update failure'; END $$; CREATE TRIGGER fail_invite_pointer BEFORE UPDATE ON chats FOR EACH ROW EXECUTE FUNCTION fail_invite_pointer()`)
		code, err := r.RegenerateGroupInviteCode(ctx, g.ID, 1)
		if err == nil || code != "" {
			t.Fatal("failed replacement returned code")
		}
		assertUnchangedGroup(t, db, g.ID)
		execFriendSQL(t, db, `DROP TRIGGER fail_invite_pointer ON chats; DROP FUNCTION fail_invite_pointer()`)
	})
}
func assertUnchangedGroup(t *testing.T, db *sql.DB, id int64) {
	t.Helper()
	var name, avatar string
	var public, restricted, member, active bool
	var invites int
	if err := db.QueryRow(`SELECT name,avatar_url,is_public,admins_only_write FROM chats WHERE id=$1`, id).Scan(&name, &avatar, &public, &restricted); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM chat_members WHERE chat_id=$1 AND user_id=2)`, id).Scan(&member); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*),bool_and(is_active) FROM group_invite_links WHERE chat_id=$1`, id).Scan(&invites, &active); err != nil {
		t.Fatal(err)
	}
	if name != "transaction group" || avatar != "" || public || restricted || !member || invites != 1 || !active {
		t.Fatalf("unexpected group state: %s %s %t %t %t %d %t", name, avatar, public, restricted, member, invites, active)
	}
}
