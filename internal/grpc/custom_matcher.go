package grpc

import "github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

func CustomMatcher(key string) (string, bool) {
	switch key {
	// runtime already forwards Authorization as "authorization". Forwarding it
	// here again produces duplicate credentials; the metadata alias can spoof it.
	case "Grpc-Metadata-Authorization", "Authorization":
		return "", false
	default:
		return runtime.DefaultHeaderMatcher(key)
	}
}
