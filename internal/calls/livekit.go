package calls

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
)

// Generates livekit token for user.
func GenerateLivekitToken(chatID, userID int64, sessionID, name, avatarURL string) (string, error) {
	apiKey := os.Getenv("LIVEKIT_API_KEY")
	apiSecret := os.Getenv("LIVEKIT_API_SECRET")
	if apiKey == "" || apiSecret == "" {
		return "", fmt.Errorf("LiveKit API credentials are not configured")
	}
	id, err := uuid.Parse(sessionID)
	if chatID <= 0 || userID <= 0 || err != nil || id == uuid.Nil || id.String() != sessionID {
		return "", fmt.Errorf("invalid call session")
	}

	chatIDstr := strconv.FormatInt(chatID, 10)
	userIDstr := strconv.FormatInt(userID, 10)

	metadata, err := json.Marshal(map[string]string{
		"avatar_url": avatarURL,
	})
	if err != nil {
		return "", fmt.Errorf("failed to encode participant metadata: %w", err)
	}

	at := auth.NewAccessToken(apiKey, apiSecret)

	truePtr := true
	grant := &auth.VideoGrant{
		RoomJoin:       true,
		Room:           chatIDstr, // Передаем ID чата как имя комнаты LiveKit
		CanPublish:     &truePtr,
		CanSubscribe:   &truePtr,
		CanPublishData: &truePtr,
	}

	at.SetVideoGrant(grant).
		SetIdentity(userIDstr + ":" + sessionID).
		SetName(name).
		SetMetadata(string(metadata)).
		SetValidFor(time.Hour)

	return at.ToJWT()
}
