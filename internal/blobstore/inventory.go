package blobstore

import (
	"context"
	"errors"
	"io"
	"strings"

	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Artifact is metadata only; no path or decrypted filename is returned.
type Artifact struct {
	AttemptID string
	Part      bool
	Bytes     int64
}

// Inventory requires a quiescent startup owner. Memory and entry count are
// explicitly capped; unknown names/symlinks/special files fail without deletion.
func (s *Store) Inventory(ctx context.Context, max int) ([]Artifact, error) {
	if max < 1 || max > 20000 {
		return nil, ErrInvalid
	}
	if err := s.begin(ctx, "startup-manifest", 0); err != nil {
		return nil, err
	}
	defer s.end("startup-manifest")
	return s.inventory(ctx, max)
}

// Caller owns one Store operation across the entire bounded scan.
func (s *Store) inventory(ctx context.Context, max int) ([]Artifact, error) {
	d, err := s.root.Open(".")
	if err != nil {
		return nil, err
	}
	defer d.Close()
	result := make([]Artifact, 0, min(max, 64))
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		entries, e := d.ReadDir(64)
		if e != nil && e != io.EOF {
			return nil, e
		}
		for _, entry := range entries {
			n := entry.Name()
			if n == ".owner.lock" || n == ".store-id" {
				continue
			}
			if len(result) >= max {
				return nil, ErrHeadroom
			}
			part := strings.HasSuffix(n, ".part")
			blob := strings.HasSuffix(n, ".blob")
			if (!part && !blob) || len(n) != 41 || !validID(n[:36]) {
				return nil, ErrRoot
			}
			f, info, er := openArtifact(s.root, n, true)
			if er != nil {
				return nil, er
			}
			if er = f.Close(); er != nil {
				return nil, er
			}
			result = append(result, Artifact{n[:36], part, info.Size()})
		}
		if e == io.EOF {
			return result, nil
		}
	}
}

// Verify checks an ALREADY persisted Ready/Linked artifact. It never returns
// Written and cannot promote an orphan or reconstruct cryptographic state.
func (s *Store) Verify(ctx context.Context, a Attempt) (err error) {
	if !validID(a.ID) || a.Bytes <= boundary.AttachmentBlobOverhead || a.Bytes > boundary.MaxAttachmentBlobBytes {
		return ErrInvalid
	}
	if err = s.begin(ctx, a.ID, 0); err != nil {
		return err
	}
	defer s.end(a.ID)
	f, info, err := openArtifact(s.root, a.ID+".blob", false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	if info.Size() != a.Bytes {
		return ErrIntegrity
	}
	return copyExact(ctx, io.Discard, f, a)
}
