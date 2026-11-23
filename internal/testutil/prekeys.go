package testutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/proto"
)

// PublicPreKeyUpload uses reviewed public-only material, not a real account or
// private state. Substitute only the test-owned generation and request UUID.
func PublicPreKeyUpload(t *testing.T, generation string) *pb.UploadPreKeysRequest {
	t.Helper()
	_, filename, _, _ := runtime.Caller(0)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(filename), "../../proto/fixtures/e2ee-v1-publication.bin"))
	if err != nil {
		t.Fatal(err)
	}
	req := new(pb.UploadPreKeysRequest)
	if err := proto.Unmarshal(data, req); err != nil {
		t.Fatal(err)
	}
	req.Context.DeviceGenerationId = generation
	req.BatchId = uuid.NewString()
	req.ExpectedRevision = 0
	return req
}
