package calls

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/twitchtv/twirp"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/service"
	"golang.org/x/sync/semaphore"
)

type membership interface {
	IsMember(context.Context, int64, int64) (bool, error)
}

var ErrMediaUnavailable = errors.New("chat access changed; media disconnection pending")

// Gateway is the only public signaling entrance. ICE media still goes directly
// to the SFU; RemoveParticipant closes it even if a client ignores our UI/WS.
type Gateway struct {
	ctx         context.Context
	auth        *service.AuthService
	chats       membership
	key, secret string
	proxy       *httputil.ReverseProxy
	rooms       livekit.RoomService
	chatLocks   [64]*semaphore.Weighted
}

func NewGateway(ctx context.Context, sessions *service.AuthService, chats membership, internalURL, key, secret string) (*Gateway, error) {
	g := &Gateway{ctx: ctx, auth: sessions, chats: chats, key: key, secret: secret}
	for i := range g.chatLocks {
		g.chatLocks[i] = semaphore.NewWeighted(1 << 20)
	}
	if key == "" && secret == "" {
		return g, nil
	}
	u, err := url.Parse(internalURL)
	if err != nil || key == "" || secret == "" || u == nil || u.Host == "" ||
		(u.Scheme != "http" && u.Scheme != "https") || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
		return nil, errors.New("calls require credentials and a private LIVEKIT_INTERNAL_URL HTTP(S) origin")
	}
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout: 3 * time.Second, ResponseHeaderTimeout: 5 * time.Second,
		IdleConnTimeout: 90 * time.Second, MaxIdleConns: 32, MaxIdleConnsPerHost: 16}
	g.proxy = &httputil.ReverseProxy{Transport: transport, Rewrite: func(r *httputil.ProxyRequest) {
		r.SetURL(u)
		r.Out.URL.Path = strings.TrimPrefix(r.In.URL.Path, "/rtc")
		r.Out.URL.RawPath = ""
		// Do not trust browser forwarding headers or send app credentials upstream.
		r.Out.Header.Del("Authorization")
		r.Out.Header.Del("Cookie")
	}, ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
		metrics.CallProxyErrors.Inc()
		// The default proxy logger prints the URL, including its media access token.
		http.Error(w, "Call server unavailable", http.StatusServiceUnavailable)
	}, ErrorLog: log.New(io.Discard, "", 0)}
	g.rooms = livekit.NewRoomServiceProtobufClient(u.String(), &http.Client{Transport: transport, Timeout: 3 * time.Second})
	return g, nil
}

func (g *Gateway) Enabled() bool { return g.proxy != nil }

// User ID and session UUID are identifiers, never bearer credentials. Legacy
// user-only media tokens are deliberately rejected. SFU refresh preserves identity.
func parseIdentity(identity string) (int64, string, error) {
	user, session, ok := strings.Cut(identity, ":")
	userID, err := strconv.ParseInt(user, 10, 64)
	id, uuidErr := uuid.Parse(session)
	if !ok || err != nil || userID <= 0 || strconv.FormatInt(userID, 10) != user || uuidErr != nil || id == uuid.Nil || id.String() != session {
		return 0, "", domain.ErrInvalidSession
	}
	return userID, session, nil
}

func roomID(name string) (int64, error) {
	id, err := strconv.ParseInt(name, 10, 64)
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != name {
		return 0, domain.ErrInvalidInput
	}
	return id, nil
}

// Lock order everywhere: session barrier -> chat barrier. Group removal holds
// the write side through its DB commit and SFU removal; admission holds the read
// side until LiveKit has registered the participant and answered the handshake.
func (g *Gateway) LockChat(ctx context.Context, chatID int64) (func(), error) {
	l := g.chatLocks[uint64(chatID)%64]
	if err := l.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { l.Release(1) }, nil
}

func (g *Gateway) RevokeChat(ctx context.Context, chatID int64, mutate func() error, targetUserID int64) error {
	l := g.chatLocks[uint64(chatID)%64]
	if err := l.Acquire(ctx, 1<<20); err != nil {
		return err
	}
	defer l.Release(1 << 20)
	if err := mutate(); err != nil {
		return err
	}
	if !g.Enabled() {
		return nil
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := g.removeMatching(cleanup, strconv.FormatInt(chatID, 10), targetUserID, ""); err != nil {
		return ErrMediaUnavailable
	}
	return nil
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !g.Enabled() {
		http.Error(w, "Calls disabled", http.StatusServiceUnavailable)
		return
	}
	switch r.URL.Path {
	case "/rtc/rtc", "/rtc/rtc/v1", "/rtc/rtc/validate", "/rtc/rtc/v1/validate":
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Reject ambiguous credentials. The browser SDK uses the access_token query;
	// this entrance never accepts an app JWT, admin JWT, or alternate auth source.
	q, err := url.ParseQuery(r.URL.RawQuery)
	raw := q.Get("access_token")
	if err != nil || len(q["access_token"]) != 1 || len(raw) > 8192 || raw == "" || r.Header.Get("Authorization") != "" {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	verifier, err := auth.ParseAPIToken(raw)
	if err != nil || verifier.APIKey() != g.key {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	standard, grants, err := verifier.Verify(g.secret)
	if err != nil || standard.Expiry == nil || !time.Now().Before(standard.Expiry.Time()) || grants.Video == nil || !grants.Video.RoomJoin {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	userID, sessionID, err := parseIdentity(grants.Identity)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	chatID, err := roomID(grants.Video.Room)
	if err != nil {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	// Only the admission has a deadline. Never give an established call a fixed
	// HTTP request timeout or hold the session read barrier for its entire life.
	timer := time.AfterFunc(5*time.Second, cancel)
	defer timer.Stop()
	unlockSession, err := g.auth.AuthorizeCallSession(ctx, userID, sessionID)
	if err != nil {
		admissionError(w, err)
		return
	}
	unlockSession = sync.OnceFunc(unlockSession)
	defer unlockSession()
	unlockChat, err := g.LockChat(ctx, chatID)
	if err != nil {
		admissionError(w, err)
		return
	}
	unlockChat = sync.OnceFunc(unlockChat)
	defer unlockChat()
	member, err := g.chats.IsMember(ctx, chatID, userID)
	if err != nil {
		admissionError(w, err)
		return
	}
	if !member {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}
	// LiveKit v1.13.10 registers the participant before returning HTTP101.
	release := sync.OnceFunc(func() { timer.Stop(); unlockChat(); unlockSession() })
	defer release()
	proxy := *g.proxy
	upgraded := false
	defer func() {
		if upgraded {
			metrics.CallSignalingConnections.Dec()
		}
	}()
	proxy.ModifyResponse = func(response *http.Response) error {
		if response.StatusCode == http.StatusSwitchingProtocols {
			metrics.CallSignalingConnections.Inc()
			upgraded = true
		}
		release()
		return nil
	}
	proxy.ServeHTTP(w, r.WithContext(ctx))
}

func admissionError(w http.ResponseWriter, err error) {
	if errors.Is(err, domain.ErrInvalidSession) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	http.Error(w, "Call authorization unavailable", http.StatusServiceUnavailable)
}

func (g *Gateway) adminContext(ctx context.Context, room string, list bool) (context.Context, error) {
	grant := &auth.VideoGrant{Room: room, RoomAdmin: !list, RoomList: list}
	token, err := auth.NewAccessToken(g.key, g.secret).SetVideoGrant(grant).SetValidFor(time.Minute).ToJWT()
	if err != nil {
		return nil, err
	}
	return twirp.WithHTTPRequestHeaders(ctx, http.Header{"Authorization": []string{"Bearer " + token}})
}

func (g *Gateway) participants(ctx context.Context, room string) (people []*livekit.ParticipantInfo, err error) {
	start := time.Now()
	defer func() { metrics.ObserveCallOperation("list_participants", start, err) }()
	ctx, err = g.adminContext(ctx, room, false)
	if err != nil {
		return nil, err
	}
	res, err := g.rooms.ListParticipants(ctx, &livekit.ListParticipantsRequest{Room: room})
	if absent(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return res.Participants, nil
}

func absent(err error) bool {
	var t twirp.Error
	return errors.As(err, &t) && t.Code() == twirp.NotFound
}

func (g *Gateway) remove(ctx context.Context, room, identity string) (err error) {
	start := time.Now()
	defer func() { metrics.ObserveCallOperation("remove_participant", start, err) }()
	ctx, err = g.adminContext(ctx, room, false)
	if err != nil {
		return err
	}
	_, err = g.rooms.RemoveParticipant(ctx, &livekit.RoomParticipantIdentity{Room: room, Identity: identity})
	if absent(err) {
		return nil
	}
	return err
}

func (g *Gateway) removeMatching(ctx context.Context, room string, userID int64, sessionID string) error {
	people, err := g.participants(ctx, room)
	if err != nil {
		return err
	}
	for _, p := range people {
		uid, sid, e := parseIdentity(p.Identity)
		if userID == 0 || (e == nil && uid == userID && (sessionID == "" || sid == sessionID)) {
			if err := g.remove(ctx, room, p.Identity); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Gateway) listRooms(ctx context.Context) (rooms []*livekit.Room, err error) {
	start := time.Now()
	defer func() { metrics.ObserveCallOperation("list_rooms", start, err) }()
	ctx, err = g.adminContext(ctx, "", true)
	if err != nil {
		return nil, err
	}
	res, err := g.rooms.ListRooms(ctx, &livekit.ListRoomsRequest{})
	if err != nil {
		return nil, err
	}
	return res.Rooms, nil
}

// Called under the auth write barrier, after DB commit. Listing the SFU also
// catches clients which dropped signaling, ignore the UI, or survived a Go restart.
func (g *Gateway) DisconnectSession(ctx context.Context, userID int64, sessionID string) error {
	if !g.Enabled() {
		return nil
	}
	rooms, err := g.listRooms(ctx)
	if err != nil {
		return err
	}
	for _, room := range rooms {
		if _, err := roomID(room.Name); err != nil {
			continue
		}
		if err := g.removeMatching(ctx, room.Name, userID, sessionID); err != nil {
			return err
		}
	}
	return nil
}

// Reconciliation retries failed removals and detects DB expiry, membership
// removal outside this HTTP process and orphaned media after a backend restart.
func (g *Gateway) Reconcile(ctx context.Context) (err error) {
	if !g.Enabled() {
		return nil
	}
	start := time.Now()
	defer func() {
		metrics.ObserveCallOperation("reconcile", start, err)
		if err == nil {
			metrics.CallReconcileLastSuccess.SetToCurrentTime()
		}
	}()
	rooms, err := g.listRooms(ctx)
	if err != nil {
		return err
	}
	var unavailable error
	for _, room := range rooms {
		chatID, err := roomID(room.Name)
		if err != nil {
			continue
		}
		people, err := g.participants(ctx, room.Name)
		if err != nil {
			return err
		}
		for _, p := range people {
			uid, sid, err := parseIdentity(p.Identity)
			valid := false
			if err == nil {
				check, cancel := context.WithTimeout(ctx, 3*time.Second)
				unlock, authErr := g.auth.AuthorizeCallSession(check, uid, sid)
				if authErr == nil {
					valid, err = g.chats.IsMember(check, chatID, uid)
					unlock()
				}
				cancel()
				// A transient DB/admission failure is not revocation. Leave
				// this participant intact and retry on the next tick. Continue
				// checking others so one busy account cannot hide a known revoke.
				if authErr != nil && !errors.Is(authErr, domain.ErrInvalidSession) {
					if unavailable == nil {
						unavailable = authErr
					}
					continue
				}
				if err != nil {
					if unavailable == nil {
						unavailable = err
					}
					continue
				}
			}
			if !valid {
				if err := g.remove(ctx, room.Name, p.Identity); err != nil {
					return err
				}
			}
		}
	}
	return unavailable
}

func (g *Gateway) Run() {
	if !g.Enabled() {
		return
	}
	defer g.proxy.Transport.(*http.Transport).CloseIdleConnections()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(g.ctx, 5*time.Second)
			if err := g.Reconcile(ctx); err != nil && g.ctx.Err() == nil {
				log.Print("Call access reconciliation unavailable; will retry")
			}
			cancel()
		}
	}
}

var _ http.Handler = (*Gateway)(nil)
