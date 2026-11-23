package grpc

// Ordinary RPC requests carry profile/chat metadata, not attachment bytes.
const MaxRequestSize = 64 << 10
