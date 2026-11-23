package calls

import (
	"strings"
	"testing"
)

func TestMissingCredentialsDoNotExposeConfiguredValue(t *testing.T) {
	for _, missing := range []string{"LIVEKIT_API_KEY", "LIVEKIT_API_SECRET"} {
		t.Run(missing, func(t *testing.T) {
			const marker = "private-credential-must-not-appear"
			t.Setenv("LIVEKIT_API_KEY", marker)
			t.Setenv("LIVEKIT_API_SECRET", marker)
			t.Setenv(missing, "")
			token, err := GenerateLivekitToken(1, 1, "af548fd6-e187-438d-9b54-afc5c5325663", "user", "")
			if err == nil || token != "" {
				t.Fatal("missing credentials did not fail")
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatal("configuration error exposes the remaining credential")
			}
		})
	}
}
