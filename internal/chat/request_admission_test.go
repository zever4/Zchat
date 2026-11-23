package chat

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/service"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebSocketBusyKeepsSocketAndDoesNotAuthorizeOrSave(t *testing.T) {
	hub := startTestHub(t)
	u := &sessionUsers{}
	u.version.Store(1)
	a := service.NewAuthService(u, "admission-ws-test-secret-atleast32")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	raw, _ := a.GenerateToken(1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, a, u, nil, nil) }))
	defer srv.Close()
	c, _, e := websocket.DefaultDialer.Dial(webSocketTestURL(t, a, raw, srv.URL), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, e = c.ReadMessage(); e != nil {
		t.Fatal(e)
	}
	var releases []func()
	for i := int64(10); i < 26; i++ {
		r, e := g.Try(t.Context(), i, admission.Data)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	reads := u.reads.Load()
	id := "00000000-0000-4000-8000-000000000001"
	if e = c.WriteJSON(map[string]any{"chat_id": 42, "content": "unchanged", "client_message_id": id}); e != nil {
		t.Fatal(e)
	}
	_, data, e := c.ReadMessage()
	if e != nil {
		t.Fatal(e)
	}
	var got struct {
		Type, Code string
		Retryable  bool
		ID         string `json:"client_message_id"`
	}
	if e = json.Unmarshal(bytes.TrimSpace(data), &got); e != nil {
		t.Fatal(e)
	}
	if got.Type != "error" || got.Code != "server_busy" || !got.Retryable || got.ID != id || u.reads.Load() != reads {
		t.Fatal("busy frame touched auth/storage or changed correlation")
	}
	for _, r := range releases {
		r()
	}
	// Invalid body exercises the still-open authenticated socket without a
	// message repository; the native ACK must never have been fabricated.
	if e = c.WriteJSON(map[string]any{"chat_id": 42, "content": "", "client_message_id": id}); e != nil {
		t.Fatal(e)
	}
	_, data, e = c.ReadMessage()
	if e != nil {
		t.Fatal("overload closed socket", e)
	}
	if e = json.Unmarshal(bytes.TrimSpace(data), &got); e != nil {
		t.Fatal(e)
	}
	if got.Code != "invalid_message" || u.reads.Load() <= reads {
		t.Fatal(got)
	}
}
