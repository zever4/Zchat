//go:build linux

package handlers

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
)

type lostAttachmentReply struct {
	repository.AttachmentRepository
	item *domain.Attachment
}

func (r *lostAttachmentReply) CreateAuthorized(ctx context.Context, a *domain.Attachment) error {
	r.item = a
	if err := r.AttachmentRepository.CreateAuthorized(ctx, a); err != nil {
		return err
	}
	return errors.New("synthetic lost reply AFTER actual COMMIT")
}
func TestManagedUploadActualPostgresCommitAndRejection(t *testing.T) {
	t.Chdir(t.TempDir())
	db := testutil.Postgres(t)
	auth := service.NewAuthService(repository.NewPgUserRepository(db), "managed-upload-PG-synthetic-secret-atleast32")
	token, err := auth.Register(t.Context(), "owner", "owner@storage.invalid", "Synthetic-test-password")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := auth.ParseToken(token)
	if err != nil {
		t.Fatal(err)
	}
	var chat int64
	if err = db.QueryRowContext(t.Context(), `INSERT INTO chats(type,name,owner_id)VALUES('group','storage fixture',$1)RETURNING id`, claims.UserID).Scan(&chat); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(t.Context(), `INSERT INTO chat_members(chat_id,user_id,role)VALUES($1,$2,'owner')`, chat, claims.UserID); err != nil {
		t.Fatal(err)
	}
	m := storageAt(t, "web/uploads")
	base, _ := m.Usage()
	repo := repository.NewPgAttachmentRepository(db)
	unknown := &lostAttachmentReply{AttachmentRepository: repo}
	w := httptest.NewRecorder()
	payload := []byte("accepted file bytes")
	withUploadStorage(auth, HandleUploadFile(auth, unknown), "file", m)(w, uploadRequest(t, token, "file", payload, strconv.FormatInt(chat, 10), 0))
	if w.Code != 500 || unknown.item == nil {
		t.Fatal(w.Code, w.Body.String())
	}
	if row, err := repo.GetForUser(t.Context(), unknown.item.Path, claims.UserID); err != nil || row.Size != int64(len(payload)) {
		t.Fatal("actual COMMIT lost", row, err)
	}
	response := httptest.NewRecorder()
	ProtectSession(auth, HandleDownload(auth, repo), false, m)(response, downloadRequest(token, unknown.item.Path))
	if response.Code != 200 || response.Body.String() != string(payload) {
		t.Fatal("committed file was removed", response.Code, response.Body.String())
	}
	if used, _ := m.Usage(); used.Bytes <= base.Bytes {
		t.Fatal("physical quota released after lost reply")
	}
	if _, err = db.ExecContext(t.Context(), `DELETE FROM chat_members WHERE chat_id=$1 AND user_id=$2`, chat, claims.UserID); err != nil {
		t.Fatal(err)
	}
	before, _ := m.Usage()
	rejected := httptest.NewRecorder()
	withUploadStorage(auth, HandleUploadFile(auth, repo), "file", m)(rejected, uploadRequest(t, token, "file", []byte("not accepted"), strconv.FormatInt(chat, 10), 0))
	if rejected.Code != 403 {
		t.Fatal(rejected.Code, rejected.Body.String())
	}
	if after, _ := m.Usage(); after != before {
		t.Fatal("confirmed rejection charged retained data", before, after)
	}
	files, err := os.ReadDir(filepath.Join("web/uploads", "files"))
	if err != nil || len(files) != 1 {
		t.Fatal(files, err)
	}
}
