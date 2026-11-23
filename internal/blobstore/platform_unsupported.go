//go:build !linux

package blobstore

import "os"

// The server's durable filesystem contract is Linux/Docker. Desktop Windows
// continues to use its existing Rust account SQLite, not this server package.
func lockRoot(*os.Root) (*os.File, error)    { return nil, ErrUnsupported }
func diskAvailable(*os.File) (uint64, error) { return 0, ErrUnsupported }
func rootIdentity(*os.Root) (string, error)  { return "", ErrUnsupported }
func openArtifact(*os.Root, string, bool) (*os.File, os.FileInfo, error) {
	return nil, nil, ErrUnsupported
}
