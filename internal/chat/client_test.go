package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type recordingMessages struct {
	repository.MessageRepository
	saved    []domain.Message
	err      error
	meta     *domain.Chat
	calls    int
	replayed bool
}

type typingChat struct {
	repository.ChatRepository
	meta *domain.Chat
	err  error
}

func (r typingChat) GetChatByID(context.Context, int64) (*domain.Chat, error) { return r.meta, r.err }
func (r typingChat) IsMember(context.Context, int64, int64) (bool, error)     { return true, nil }
func (r typingChat) GetMemberIDs(context.Context, int64) ([]int64, error)     { return []int64{1, 2}, nil }

func TestTypingUsesVerifiedAnonymousPolicy(t *testing.T) {
	group := &domain.Chat{Type: "group", AnonymousMode: true}
	for _, tc := range []struct {
		name string
		repo typingChat
		send bool
	}{
		{"metadata unavailable", typingChat{err: errors.New("database read failed")}, false},
		{"missing chat", typingChat{}, false},
		{"anonymous group", typingChat{meta: group}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hub := &Hub{Direct: make(chan *DirectMessage, 1)}
			c := &Client{ID: 1, Username: "real name", Hub: hub, ChatRepo: tc.repo}
			c.broadcastTyping(t.Context(), 7)
			select {
			case event := <-hub.Direct:
				if !tc.send {
					t.Fatal("typing event emitted without confirmed chat policy")
				}
				var payload map[string]any
				if err := json.Unmarshal(event.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				if payload["sender_id"] != float64(0) || payload["sender_name"] != "Кто-то" {
					t.Fatal("identity leaked in anonymous typing event")
				}
			default:
				if tc.send {
					t.Fatal("typing event missing")
				}
			}
		})
	}
}

func (r *recordingMessages) SaveAuthorized(ctx context.Context, m *domain.Message) (*domain.Chat, []int64, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded save")
	}
	r.calls++
	if r.err != nil {
		return nil, nil, r.err
	}
	m.ID, m.CreatedAt = 42, time.Unix(123456, 0)
	m.Replayed = r.replayed
	r.saved = append(r.saved, *m)
	return r.meta, []int64{1, 2}, nil
}

func TestMessageReceipts(t *testing.T) {
	const clientID = "12345678-1234-4321-8123-123456789abc"
	for _, tc := range []struct {
		name                string
		err                 error
		replayed, retryable bool
		code                string
	}{
		{name: "saved"}, {name: "replay", replayed: true},
		{name: "deadline", err: context.DeadlineExceeded, retryable: true, code: "save_unconfirmed"},
		{name: "db", err: errors.New("private database detail"), retryable: true, code: "save_unconfirmed"},
		{name: "kicked", err: domain.ErrNotChatMember, code: "forbidden"},
		{name: "collision", err: domain.ErrMessageIDConflict, code: "id_conflict"},
		{name: "bad attachment", err: domain.ErrInvalidInput, code: "invalid_message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := &recordingMessages{err: tc.err, replayed: tc.replayed, meta: &domain.Chat{Type: "group", AnonymousMode: true, Name: "Anonymous"}}
			hub := &Hub{Direct: make(chan *DirectMessage, 3)}
			c := &Client{ID: 1, Username: "secret sender", Hub: hub, MessageRepo: repo}
			// Expired SQL budget must not suppress a correlated error/receipt.
			parent := context.Background()
			if tc.err != nil {
				canceled, cancel := context.WithCancel(parent)
				cancel()
				parent = canceled
			}
			c.handleMessageContext(parent, incomingMessage{ChatID: 7, Content: "hello", ClientMessageID: clientID})
			count := 1
			if tc.err == nil && !tc.replayed {
				count = 2
			}
			if len(hub.Direct) != count {
				t.Fatalf("events=%d want=%d", len(hub.Direct), count)
			}
			if count == 2 {
				var broadcast map[string]any
				json.Unmarshal((<-hub.Direct).Payload, &broadcast)
				if broadcast["client_message_id"] != nil || broadcast["sender_id"] != float64(0) {
					t.Fatal("anonymous receipt leaked", broadcast)
				}
			}
			receipt := <-hub.Direct
			if len(receipt.RecipientIDs) != 1 || receipt.RecipientIDs[0] != 1 || receipt.Connection != c {
				t.Fatal("receipt sent to peers")
			}
			var wire map[string]any
			json.Unmarshal(receipt.Payload, &wire)
			if wire["client_message_id"] != clientID || wire["chat_id"] != float64(7) {
				t.Fatal("uncorrelated receipt")
			}
			if tc.err != nil {
				if wire["type"] != "error" || wire["code"] != tc.code || wire["retryable"] != tc.retryable || strings.Contains(string(receipt.Payload), "private database") {
					t.Fatal("wrong error", wire)
				}
				if wire["id"] != nil {
					t.Fatal("failure acknowledged")
				}
			} else if wire["type"] != "message_ack" || wire["id"] != float64(42) {
				t.Fatal("missing durable receipt", wire)
			}
		})
	}
	t.Run("invalid client id cannot insert", func(t *testing.T) {
		repo := &recordingMessages{}
		hub := &Hub{Direct: make(chan *DirectMessage, 1)}
		c := &Client{ID: 1, Hub: hub, MessageRepo: repo}
		c.handleMessage(incomingMessage{ChatID: 7, Content: "hello", ClientMessageID: "not-uuid"})
		if repo.calls != 0 || len(hub.Direct) != 1 {
			t.Fatal("invalid id saved or silently discarded")
		}
	})
}

// Database policy/locking is tested against PostgreSQL. These tests verify the WS
// boundary: atomic repository errors never become deliveries, DB metadata wins.
func TestMessageAuthorization(t *testing.T) {
	for _, tc := range []struct {
		name, wireType, chatType, content string
		err                               error
		anonymous, skip                   bool
	}{
		{name: "outsider", err: domain.ErrNotChatMember}, {name: "missing chat", err: domain.ErrChatNotFound},
		{name: "restricted member", err: domain.ErrChatWriteForbidden}, {name: "database failure", err: errors.New("database unavailable")},
		{name: "private", chatType: "private", wireType: "group", content: " hello <world> "},
		{name: "group", chatType: "group", wireType: "private", content: "hello"},
		{name: "anonymous", chatType: "group", wireType: "private", content: "hello", anonymous: true},
		{name: "empty", content: "  ", skip: true}, {name: "too long", content: strings.Repeat("x", 1500), skip: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			messages := &recordingMessages{err: tc.err, meta: &domain.Chat{Type: tc.chatType, AnonymousMode: tc.anonymous, Name: "Group", AvatarURL: "group.png"}}
			hub := &Hub{Direct: make(chan *DirectMessage, 2)}
			// ChatRepo intentionally nil: persistent writes must use only the atomic operation.
			c := &Client{ID: 1, Username: "sender", AvatarURL: "sender.png", Hub: hub, MessageRepo: messages}
			content := tc.content
			if content == "" && !tc.skip {
				content = "hello"
			}
			c.handleMessage(incomingMessage{ChatID: 7, Content: content, Type: tc.wireType})
			if tc.skip {
				if messages.calls != 0 {
					t.Fatal("invalid content reached database")
				}
				return
			}
			if tc.err != nil {
				if len(messages.saved) != 0 {
					t.Fatal("unauthorized save")
				}
				for len(hub.Direct) > 0 {
					var payload map[string]any
					json.Unmarshal((<-hub.Direct).Payload, &payload)
					if payload["type"] != "error" {
						t.Fatal("unauthorized delivery")
					}
				}
				return
			}
			if len(messages.saved) != 1 || len(hub.Direct) != 1 {
				t.Fatal("missing save/delivery")
			}
			delivery := <-hub.Direct
			var msg domain.Message
			if err := json.Unmarshal(delivery.Payload, &msg); err != nil {
				t.Fatal(err)
			}
			if msg.ID != 42 || msg.Type != tc.chatType || msg.ChatID != 7 || !msg.CreatedAt.Equal(time.Unix(123456, 0)) || len(delivery.RecipientIDs) != 2 {
				t.Fatalf("wrong delivery: %+v", msg)
			}
			if messages.saved[0].SenderID != 1 {
				t.Fatal("stored sender lost")
			}
			if tc.name == "private" && msg.Content != "hello &lt;world&gt;" {
				t.Fatal("content escaping changed")
			}
			if tc.anonymous {
				if msg.SenderID != 0 || msg.SenderName != "Group" || msg.SenderAvatar != "group.png" {
					t.Fatal("identity leaked")
				}
			} else if msg.SenderID != 1 {
				t.Fatal("sender lost")
			}
		})
	}
}
