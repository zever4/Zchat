package repository

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/lib/pq"
	pb "github.com/zever4/Zchat/gen/messenger"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/protobuf/proto"
)

func prekeyWireFixture(t *testing.T) *pb.PreKeyBundleUpload {
	t.Helper()
	data, err := os.ReadFile("../../proto/fixtures/prekey-upload-v1.bin")
	if err != nil {
		t.Fatal(err)
	}
	upload := new(pb.PreKeyBundleUpload)
	if err := proto.Unmarshal(data, upload); err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(upload)
	if err != nil || !bytes.Equal(encoded, data) {
		t.Fatalf("Go cannot reproduce the shared Rust wire fixture: %v", err)
	}
	return upload
}

func TestPreKeyWireFixture(t *testing.T) {
	upload := prekeyWireFixture(t)
	if upload.UserId != "11111111-1111-4111-8111-111111111111" || upload.DeviceId != 1 ||
		upload.RegistrationId != 16383 || upload.SignedPreKeyTimestamp != 1700000000123 {
		t.Fatal("fixture metadata changed")
	}
	if len(upload.IdentityKey) != 33 || upload.SignedPreKey == nil || upload.SignedPreKey.KeyId != 4294967293 ||
		len(upload.SignedPreKey.PublicKey) != 33 || len(upload.SignedPreKey.Signature) != 64 {
		t.Fatal("identity/signed-prekey contract changed")
	}
	if len(upload.OneTimePreKeys) != 1 || upload.OneTimePreKeys[0].KeyId != 2147483648 || len(upload.OneTimePreKeys[0].PublicKey) != 33 {
		t.Fatal("one-time prekey contract changed")
	}
	if len(upload.KyberPreKeys) != 2 {
		t.Fatal("expected one one-time and one last-resort Kyber key")
	}
	for i, key := range upload.KyberPreKeys {
		if key.KeyId != 4294967294+uint32(i) || key.IsLastResort != (i == 1) || len(key.PublicKey) != 1569 || len(key.Signature) != 64 {
			t.Fatal("Kyber1024 public-key contract changed")
		}
	}
}

func TestPreKeyWirePostgres(t *testing.T) {
	db := prekeyPostgres(t)
	ctx := t.Context()
	upload := prekeyWireFixture(t)
	if _, err := db.ExecContext(ctx, `INSERT INTO users(user_uuid,username,email,password_hash)
	 VALUES($1,'contract_fixture','contract@example.invalid','unused')`, upload.UserId); err != nil {
		t.Fatal(err)
	}
	wantDevice := &domain.DeviceKeys{
		UserID: upload.UserId, DeviceID: int(upload.DeviceId), RegistrationID: upload.RegistrationId,
		IdentityKey: upload.IdentityKey, SignedPreKeyID: upload.SignedPreKey.KeyId,
		SignedPreKeyPublic: upload.SignedPreKey.PublicKey, SignedPreKeySignature: upload.SignedPreKey.Signature,
		SignedPreKeyTimestamp: int64(upload.SignedPreKeyTimestamp),
	}
	devices := NewPgDeviceKeyRepository(db)
	if err := devices.Upsert(ctx, wantDevice); err != nil {
		t.Fatal(err)
	}
	gotDevice, err := devices.Get(ctx, wantDevice.UserID, wantDevice.DeviceID)
	if err != nil {
		t.Fatal(err)
	}
	wantDevice.CreatedAt = gotDevice.CreatedAt
	if !reflect.DeepEqual(gotDevice, wantDevice) {
		t.Fatal("device public data changed in PostgreSQL")
	}
	ec := NewPgOneTimePreKeyRepository(db)
	wantEC := domain.OneTimePreKey{UserID: upload.UserId, DeviceID: int(upload.DeviceId),
		KeyID: upload.OneTimePreKeys[0].KeyId, PublicKey: upload.OneTimePreKeys[0].PublicKey}
	if err := ec.BulkInsert(ctx, wantEC.UserID, wantEC.DeviceID, []domain.OneTimePreKey{wantEC}); err != nil {
		t.Fatal(err)
	}
	gotEC, err := ec.Acquire(ctx, wantEC.UserID, wantEC.DeviceID)
	if err != nil || !reflect.DeepEqual(gotEC, &wantEC) {
		t.Fatalf("EC bytes/uint32 ID roundtrip: %v", err)
	}
	kyber := NewPgKyberPreKeyRepository(db)
	var keys []domain.KyberPreKey
	for _, key := range upload.KyberPreKeys {
		keys = append(keys, domain.KyberPreKey{UserID: upload.UserId, DeviceID: int(upload.DeviceId),
			KeyID: key.KeyId, PublicKey: key.PublicKey, Signature: key.Signature, IsLastResort: key.IsLastResort})
	}
	if err := kyber.BulkInsert(ctx, upload.UserId, int(upload.DeviceId), keys); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		got, err := kyber.Acquire(ctx, upload.UserId, int(upload.DeviceId))
		want := &keys[min(i, 1)]
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("Kyber bytes/signature/uint32 ID roundtrip %d: %v", i, err)
		}
	}
	message := &domain.EncryptedMessage{
		MessageID: "contract-envelope", SenderID: upload.UserId, SenderDeviceID: int(upload.DeviceId),
		RecipientID: "prekey-a", RecipientDeviceID: 1, Ciphertext: []byte("opaque-test-ciphertext"),
		MessageType: 1, Timestamp: int64(upload.SignedPreKeyTimestamp),
		PreKeyID: &upload.OneTimePreKeys[0].KeyId, SignedPreKeyID: &upload.SignedPreKey.KeyId,
		KyberPreKeyID: &upload.KyberPreKeys[0].KeyId,
	}
	messages := NewE2EEPgMessageRepository(db)
	if err := messages.Save(ctx, message); err != nil {
		t.Fatal(err)
	}
	got, err := messages.GetUndelivered(ctx, message.RecipientID, message.RecipientDeviceID)
	if err != nil || len(got) != 1 {
		t.Fatalf("message key IDs roundtrip: count=%d err=%v", len(got), err)
	}
	message.CreatedAt = got[0].CreatedAt
	if !reflect.DeepEqual(got[0], message) {
		t.Fatal("message optional key IDs changed in PostgreSQL")
	}
}

func TestPreKeyScalarBoundsPostgres(t *testing.T) {
	db := prekeyPostgres(t)
	ctx := t.Context()
	if _, err := db.ExecContext(ctx, `INSERT INTO one_time_prekeys(user_id,device_id,key_id,public_key)
	 VALUES('prekey-a',1,1,'public');
	 INSERT INTO kyber_prekeys(user_id,device_id,key_id,public_key,signature)
	 VALUES('prekey-a',1,1,'public','signature');
	 INSERT INTO encrypted_messages(message_id,sender_id,sender_device_id,recipient_id,recipient_device_id,ciphertext,message_type,timestamp)
	 VALUES('contract-message','prekey-b',1,'prekey-a',1,'ciphertext',1,1)`); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ table, column, scope string }{
		{"device_keys", "registration_id", "user_id='prekey-a' AND device_id=1"},
		{"device_keys", "signed_pre_key_id", "user_id='prekey-a' AND device_id=1"},
		{"one_time_prekeys", "key_id", "user_id='prekey-a' AND device_id=1"},
		{"kyber_prekeys", "key_id", "user_id='prekey-a' AND device_id=1"},
		{"encrypted_messages", "pre_key_id", "message_id='contract-message'"},
		{"encrypted_messages", "signed_pre_key_id", "message_id='contract-message'"},
		{"encrypted_messages", "kyber_pre_key_id", "message_id='contract-message'"},
	} {
		t.Run(tc.table+"/"+tc.column, func(t *testing.T) {
			q := fmt.Sprintf("UPDATE %s SET %s=$1 WHERE %s RETURNING %s", pq.QuoteIdentifier(tc.table),
				pq.QuoteIdentifier(tc.column), tc.scope, pq.QuoteIdentifier(tc.column))
			for _, value := range []int64{0, 2147483648, 4294967295} {
				var got int64
				if err := db.QueryRowContext(ctx, q, value).Scan(&got); err != nil || got != value {
					t.Fatalf("uint32 %d did not roundtrip: got=%d err=%v", value, got, err)
				}
			}
			for _, value := range []int64{-1, 4294967296} {
				var got int64
				err := db.QueryRowContext(ctx, q, value).Scan(&got)
				var pgErr *pq.Error
				if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
					t.Errorf("out-of-range %d: expected check violation, got %v", value, err)
				}
			}
		})
	}
}
