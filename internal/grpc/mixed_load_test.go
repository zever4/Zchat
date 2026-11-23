package grpc

// Opt-in server-path diagnostic, not a libsignal/desktop or capacity benchmark.
// Both transports and every synthetic account share ONE production 25/25 pool.
// No durable native receive proof is made: recipient ACKs are simulated here.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/admission"
	plainchat "github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/handlers"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type loadAccount struct {
	token string
	raw   []byte
	ref   *pb.DeviceRef
}
type loadPair struct {
	sender, recipient loadAccount
	conversation      string
	plainChat         int64
}
type mixedLoadFixture struct {
	gate            *admission.Gate
	wsBusy, rpcBusy atomic.Int64

	*epochRPCFixture
	serverRPCActive, serverRPCPeak, serverRPCStarted atomic.Int64
	pairs                                            []loadPair
	http                                             *httptest.Server
	wsReceipts, e2eeReceipts, rpcAttempts            atomic.Int64
}

func loadContext(parent context.Context, a loadAccount) context.Context {
	return metadata.NewOutgoingContext(parent, metadata.Pairs("authorization", "Bearer "+a.token, E2EEDeviceCredentialMetadata, string(a.raw)))
}
func (f *mixedLoadFixture) observeRPC(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
	f.serverRPCStarted.Add(1)
	active := f.serverRPCActive.Add(1)
	defer f.serverRPCActive.Add(-1)
	for peak := f.serverRPCPeak.Load(); active > peak; peak = f.serverRPCPeak.Load() {
		if f.serverRPCPeak.CompareAndSwap(peak, active) {
			break
		}
	}
	return handler(ctx, req)
}
func loadWire(a loadAccount) *pb.ClientContext {
	return &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: a.ref.GenerationId}
}

func newMixedLoadFixture(t *testing.T, pairs int) *mixedLoadFixture {
	t.Helper()
	f := &mixedLoadFixture{}
	f.epochRPCFixture = newEpochRPCFixture(t, f.observeRPC)
	chatRepo := repository.NewPgChatRepository(f.db)
	add := func(a, b loadAccount, aID, bID int64, conversation string) {
		c, err := chatRepo.CreateWithMembers(t.Context(), "private", aID, bID)
		if err != nil {
			t.Fatal(err)
		}
		f.pairs = append(f.pairs, loadPair{a, b, conversation, c.ID})
	}
	add(loadAccount{f.aliceToken, f.aliceRaw, f.device.ActiveDevice.Reference}, loadAccount{f.bobToken, f.bobRaw, f.bob.ActiveDevice.Reference}, f.aliceID, f.bobID, f.chat.ConversationId)
	for i := 1; i < pairs; i++ {
		accounts := make([]loadAccount, 2)
		ids := make([]int64, 2)
		for j := range accounts {
			name := fmt.Sprintf("load_%d_%d", i, j)
			token, err := f.auth.Register(t.Context(), name, name+"@load.invalid", deviceTestPassword)
			if err != nil {
				t.Fatal(err)
			}
			claims, err := f.auth.ParseToken(token)
			if err != nil {
				t.Fatal(err)
			}
			ids[j] = claims.UserID
			raw := bytes.Repeat([]byte{byte(i*2 + j)}, 32)
			hash := service.E2EEDeviceCredentialHash(raw)
			a := loadAccount{token: token, raw: raw}
			own, err := f.registry.RegisterDevice(loadContext(t.Context(), a), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:]})
			if err != nil {
				t.Fatal(err)
			}
			a.ref = own.ActiveDevice.Reference
			if _, err := f.registry.UploadPreKeys(loadContext(t.Context(), a), testutil.PublicPreKeyUpload(t, a.ref.GenerationId)); err != nil {
				t.Fatal(err)
			}
			accounts[j] = a
		}
		if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
			t.Fatal(err)
		}
		c, err := f.registry.CreateConversation(loadContext(t.Context(), accounts[0]), &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: accounts[0].ref.GenerationId}, CreationRequestId: uuid.NewString(), PeerUserId: ids[1]})
		if err != nil {
			t.Fatal(err)
		}
		add(accounts[0], accounts[1], ids[0], ids[1], c.ConversationId)
	}
	hub := plainchat.NewHub()
	ctx, cancel := context.WithCancel(t.Context())
	go hub.Run(ctx)
	users, messages := repository.NewPgUserRepository(f.db), repository.NewPgMessageRepository(f.db)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/ws-ticket", handlers.HandleWebSocketTicket(f.auth))
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		plainchat.ServeWs(hub, w, r, f.auth, users, messages, chatRepo)
	})
	f.http = httptest.NewServer(mux)
	t.Cleanup(func() { cancel(); hub.Stop(); f.http.Close() })
	return f
}

type loadFailure struct{ stage, code string }

func (e loadFailure) Error() string { return e.stage + ":" + e.code }
func loadFail(stage string, err error) error {
	if err == nil {
		return nil
	}
	code := status.Code(err).String()
	if errors.Is(err, context.DeadlineExceeded) {
		code = "DeadlineExceeded"
	} else if errors.Is(err, context.Canceled) {
		code = "Canceled"
	}
	return loadFailure{stage, code}
}

// One completed cycle checks exact receipt/pending bytes/simulated ACK/Stored.
// A failed message is never retried or assigned another ID. Later iterations
// are separate probes; ambiguous copies remain in the disposable schema.
func (f *mixedLoadFixture) encrypted(ctx context.Context, pair loadPair) error {
	request := &v2.SendMessageRequest{Context: loadWire(pair.sender), Envelope: &v2.Envelope{ProtocolVersion: 2, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: pair.conversation, Sender: pair.sender.ref, Recipient: pair.recipient.ref, MessageType: 1, Ciphertext: bytes.Repeat([]byte{42}, 512), CryptoEpoch: proto.Uint64(0)}}
	r, err := loadRPC(ctx, f, func() (*v2.SendReceipt, error) { return f.v2.SendMessage(loadContext(ctx, pair.sender), request) })
	if err != nil {
		return loadFail("send", err)
	}
	if r.MessageId != request.Envelope.MessageId || len(r.EnvelopeDigest) != 32 {
		return loadFailure{"send", "InvalidReceipt"}
	}
	f.e2eeReceipts.Add(1)
	page, err := loadRPC(ctx, f, func() (*v2.PendingMessages, error) {
		return f.v2.GetPendingMessages(loadContext(ctx, pair.recipient), &v2.GetPendingMessagesRequest{Context: loadWire(pair.recipient)})
	})
	if err != nil {
		return loadFail("pending", err)
	}
	found := false
	for _, item := range page.Messages {
		if item.Envelope.GetMessageId() == r.MessageId {
			found = proto.Equal(item.Envelope, request.Envelope) && bytes.Equal(item.EnvelopeDigest, r.EnvelopeDigest)
		}
	}
	if !found {
		return loadFailure{"pending", "MissingOrChangedMessage"}
	}
	_, err = loadRPC(ctx, f, func() (*pb.Empty, error) {
		return f.v2.AcknowledgeMessages(loadContext(ctx, pair.recipient), &pb.AcknowledgeMessagesRequest{Context: loadWire(pair.recipient), Messages: []*pb.Acknowledgement{{MessageId: r.MessageId, EnvelopeDigest: r.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}})
	})
	if err != nil {
		return loadFail("ack", err)
	}
	observed, err := loadRPC(ctx, f, func() (*v2.MessageStatuses, error) {
		return f.v2.GetMessageStatuses(loadContext(ctx, pair.sender), &pb.GetMessageStatusesRequest{Context: loadWire(pair.sender), Messages: []*pb.MessageReference{{MessageId: r.MessageId, EnvelopeDigest: r.EnvelopeDigest}}})
	})
	if err != nil {
		return loadFail("status", err)
	}
	if len(observed.Messages) != 1 || observed.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED || !proto.Equal(observed.Messages[0].Receipt, r) {
		return loadFailure{"status", "WrongStoredReceipt"}
	}
	return nil
}
func (f *mixedLoadFixture) poll(ctx context.Context, a loadAccount) error {
	if _, err := loadRPC(ctx, f, func() (*v2.PendingMessages, error) {
		return f.v2.GetPendingMessages(loadContext(ctx, a), &v2.GetPendingMessagesRequest{Context: loadWire(a)})
	}); err != nil {
		return loadFail("idle_pending", err)
	}
	_, err := loadRPC(ctx, f, func() (*v2.PendingEpochRecoveries, error) {
		return f.v2.GetPendingEpochRecoveries(loadContext(ctx, a), &v2.GetPendingEpochRecoveriesRequest{Context: loadWire(a), Limit: 8})
	})
	return loadFail("idle_recovery", err)
}

type loadSockets struct{ sender, recipient *websocket.Conn }

func (s *loadSockets) close() {
	for _, conn := range []*websocket.Conn{s.sender, s.recipient} {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		_ = conn.Close()
	}
}
func (f *mixedLoadFixture) connect(ctx context.Context, pair loadPair) (*loadSockets, error) {
	openOnce := func(a loadAccount) (*websocket.Conn, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.http.URL+"/api/ws-ticket", nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+a.token)
		req.Header.Set("Origin", f.http.URL)
		response, err := f.http.Client().Do(req)
		if err != nil {
			return nil, err
		}
		var value struct {
			Ticket string `json:"ticket"`
		}
		if response.StatusCode == http.StatusTooManyRequests {
			response.Body.Close()
			return nil, loadFailure{"connect", "Busy"}
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&value)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || err != nil || value.Ticket == "" {
			return nil, loadFailure{"ticket", "NotIssued"}
		}
		headers := http.Header{"Origin": []string{f.http.URL}}
		conn, response, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(f.http.URL, "http")+"/ws?ticket="+url.QueryEscape(value.Ticket), headers)
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		if response != nil && response.StatusCode == http.StatusTooManyRequests {
			return nil, loadFailure{"connect", "Busy"}
		}
		return conn, err
	}
	open := func(a loadAccount) (*websocket.Conn, error) {
		for {
			c, e := openOnce(a)
			if !loadIsBusy(e) {
				return c, e
			}
			f.wsBusy.Add(1)
			if e = loadPause(ctx); e != nil {
				return nil, e
			}
		}
	}
	a, err := open(pair.sender)
	if err != nil {
		return nil, loadFail("connect_sender", err)
	}
	b, err := open(pair.recipient)
	if err != nil {
		a.Close()
		return nil, loadFail("connect_recipient", err)
	}
	return &loadSockets{a, b}, nil
}
func (f *mixedLoadFixture) ordinary(ctx context.Context, pair loadPair, sockets *loadSockets) error {
	id := uuid.NewString()
	for {
		err := f.ordinaryOnce(ctx, pair, sockets, id)
		if !loadIsBusy(err) {
			return err
		}
		f.wsBusy.Add(1)
		if err = loadPause(ctx); err != nil {
			return err
		}
	}
}
func (f *mixedLoadFixture) ordinaryOnce(ctx context.Context, pair loadPair, sockets *loadSockets, id string) error {
	deadline, _ := ctx.Deadline()
	sockets.sender.SetWriteDeadline(deadline)
	if err := sockets.sender.WriteJSON(map[string]any{"chat_id": pair.plainChat, "content": "synthetic-load-" + id, "client_message_id": id}); err != nil {
		return loadFail("ws_write", err)
	}
	wait := func(conn *websocket.Conn, ack bool) error {
		conn.SetReadDeadline(deadline)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return loadFail("ws_read", err)
			}
			for _, line := range bytes.Split(raw, []byte{'\n'}) {
				var m struct {
					Type, Content, Code string
					Retryable           bool
					ClientMessageID     string `json:"client_message_id"`
				}
				if err := json.Unmarshal(line, &m); err != nil {
					return loadFailure{"ws_read", "InvalidJSON"}
				}
				// Correlation UUID belongs only to the sender receipt. Recipient
				// broadcasts deliberately omit it; match our synthetic body there.
				if !ack && m.Content == "synthetic-load-"+id {
					return nil
				}
				if m.ClientMessageID != id {
					continue
				}
				if m.Type == "error" && m.Code == "server_busy" && m.Retryable {
					return loadFailure{"ws", "Busy"}
				}
				if m.Type == "error" {
					return loadFailure{"ws_persist", "Rejected"}
				}
				if ack && m.Type == "message_ack" {
					f.wsReceipts.Add(1)
					return nil
				}
			}
		}
	}
	if err := wait(sockets.sender, true); err != nil {
		return err
	}
	return wait(sockets.recipient, false)
}

// Bounded reservoir: exact counts/mean/max; percentiles explicitly sampled.
// Store durations only, never account IDs, tokens, text, payloads or request IDs.
type loadLane struct {
	Attempts, Completed int64
	Errors              map[string]int64
	MeanMs, MaxMs       float64
	P50Ms, P95Ms, P99Ms float64
	Samples             int
	mu                  sync.Mutex
	sum                 time.Duration
	values              []time.Duration
}

func (s *loadLane) record(d time.Duration, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Attempts++
	s.sum += d
	if err == nil {
		s.Completed++
	} else {
		if s.Errors == nil {
			s.Errors = make(map[string]int64)
		}
		key := "other"
		var failure loadFailure
		if errors.As(err, &failure) {
			key = failure.Error()
		}
		s.Errors[key]++
	}
	if ms := float64(d) / float64(time.Millisecond); ms > s.MaxMs {
		s.MaxMs = ms
	}
	if len(s.values) < 4096 {
		s.values = append(s.values, d)
	} else if i := rand.Int64N(s.Attempts); i < int64(len(s.values)) {
		s.values[i] = d
	}
}
func (s *loadLane) finish() {
	s.Samples = len(s.values)
	if s.Attempts == 0 {
		return
	}
	s.MeanMs = float64(s.sum) / float64(time.Millisecond) / float64(s.Attempts)
	slices.Sort(s.values)
	quantile := func(p int) float64 { return float64(s.values[(len(s.values)-1)*p/100]) / float64(time.Millisecond) }
	s.P50Ms, s.P95Ms, s.P99Ms = quantile(50), quantile(95), quantile(99)
}

type loadPhase struct {
	Admission                                        *admission.Snapshot
	AdmissionRejected, WSBusyRetries, RPCBusyRetries uint64

	ServerRPCAttempts, ServerPeakRPC                          int64
	StartedAt, FinishedAt                                     time.Time
	Name                                                      string
	IssueSeconds, ElapsedSeconds                              float64
	WSWorkers, RPCWorkers, PollWorkers                        int
	WSReconnect, ControlledPoolStall                          bool
	PoolMaxOpen, PoolPeakInUse, PoolPeakOpen                  int
	PoolWaitCount                                             int64
	PoolWaitSeconds                                           float64
	GoBaselineGoroutines, GoPeakGoroutines, GoAfterGoroutines int
	GoPeakHeapBytes                                           uint64
	RPCAttempts                                               int64
	Lanes                                                     map[string]*loadLane
}

func runLoadPhase(t *testing.T, f *mixedLoadFixture, name string, seconds, wsN, rpcN, pollN int, burst, stall bool) loadPhase {
	t.Helper()
	r := loadPhase{Name: name, WSWorkers: wsN, RPCWorkers: rpcN, PollWorkers: pollN, WSReconnect: burst && wsN > 0, ControlledPoolStall: stall, Lanes: make(map[string]*loadLane)}
	type task struct {
		lane                string
		run                 func(context.Context) error
		once, paced, socket bool
	}
	tasks := make([]task, 0, wsN+rpcN+pollN)
	var sockets []*loadSockets
	closeSockets := sync.OnceFunc(func() {
		for _, s := range sockets {
			s.close()
		}
	})
	defer closeSockets()
	for i := 0; i < wsN; i++ {
		pair := f.pairs[i%len(f.pairs)]
		if burst {
			tasks = append(tasks, task{lane: "ws_reconnect", once: true, socket: true, run: func(ctx context.Context) error {
				s, err := f.connect(ctx, pair)
				if err != nil {
					return err
				}
				defer s.close()
				return f.ordinary(ctx, pair, s)
			}})
		} else {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			s, err := f.connect(ctx, pair)
			cancel()
			if err != nil {
				t.Fatal("prepare socket", err)
			}
			sockets = append(sockets, s)
			tasks = append(tasks, task{lane: "ws_roundtrip", socket: true, run: func(ctx context.Context) error { return f.ordinary(ctx, pair, s) }})
		}
	}
	for i := 0; i < rpcN; i++ {
		pair := f.pairs[(i+wsN)%len(f.pairs)]
		tasks = append(tasks, task{lane: "e2ee_roundtrip", once: burst, run: func(ctx context.Context) error { return f.encrypted(ctx, pair) }})
	}
	for i := 0; i < pollN; i++ {
		pair := f.pairs[i%len(f.pairs)]
		a := pair.sender
		if i/len(f.pairs)%2 == 1 {
			a = pair.recipient
		}
		tasks = append(tasks, task{lane: "idle_poll", paced: true, run: func(ctx context.Context) error { return f.poll(ctx, a) }})
	}
	for _, job := range tasks {
		if r.Lanes[job.lane] == nil {
			r.Lanes[job.lane] = &loadLane{}
		}
	}
	var held []*sql.Conn
	if stall {
		for i := 0; i < 25; i++ {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			conn, err := f.db.Conn(ctx)
			cancel()
			if err != nil {
				for _, c := range held {
					c.Close()
				}
				t.Fatal(err)
			}
			held = append(held, conn)
		}
	}
	release := sync.OnceFunc(func() {
		for _, c := range held {
			c.Close()
		}
	})
	defer release()
	before := f.db.Stats()
	r.PoolMaxOpen = before.MaxOpenConnections
	r.GoBaselineGoroutines = runtime.NumGoroutine()
	if r.PoolMaxOpen != 25 {
		t.Fatal("production pool must remain25")
	}
	startWSBusy, startRPCBusy := f.wsBusy.Load(), f.rpcBusy.Load()
	var startRejected uint64
	if f.gate != nil {
		startRejected = f.gate.Snapshot().Rejected
	}
	startRPC := f.rpcAttempts.Load()
	startServerRPC := f.serverRPCStarted.Load()
	if f.serverRPCActive.Load() != 0 {
		t.Fatal("previous RPC phase has not drained")
	}
	f.serverRPCPeak.Store(0)
	start := time.Now()
	r.StartedAt = start.UTC()
	ctx, cancel := context.WithTimeout(t.Context(), time.Duration(seconds+4)*time.Second)
	defer cancel()
	issue, stopIssue := context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	defer stopIssue()
	if !burst {
		r.IssueSeconds = float64(seconds)
	}
	var workers, sampler sync.WaitGroup
	done := make(chan struct{})
	sampler.Go(func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			s := f.db.Stats()
			r.PoolPeakInUse = max(r.PoolPeakInUse, s.InUse)
			r.PoolPeakOpen = max(r.PoolPeakOpen, s.OpenConnections)
			r.GoPeakGoroutines = max(r.GoPeakGoroutines, runtime.NumGoroutine())
			var memory runtime.MemStats
			runtime.ReadMemStats(&memory)
			r.GoPeakHeapBytes = max(r.GoPeakHeapBytes, memory.HeapAlloc)
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	})
	var unblock sync.WaitGroup
	if stall {
		unblock.Go(func() {
			timer := time.NewTimer(500 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
			release()
		})
	}
	for n, job := range tasks {
		workers.Go(func() {
			if job.paced {
				timer := time.NewTimer(time.Duration(n*13%500) * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-issue.Done():
					return
				}
			}
			for issue.Err() == nil {
				call, cancel := context.WithTimeout(ctx, 3*time.Second)
				at := time.Now()
				err := job.run(call)
				cancel()
				r.Lanes[job.lane].record(time.Since(at), err)
				// A failed socket read cannot be reused. End that worker explicitly;
				// do not hide the failed/unknown attempt with automatic reconnection.
				if job.once || err != nil && job.socket {
					return
				}
				if job.paced {
					timer := time.NewTimer(500 * time.Millisecond)
					select {
					case <-timer.C:
					case <-issue.Done():
					}
					timer.Stop()
				}
			}
		})
	}
	workers.Wait()
	unblock.Wait()
	close(done)
	sampler.Wait()
	r.ElapsedSeconds = time.Since(start).Seconds()
	r.FinishedAt = time.Now().UTC()
	r.WSBusyRetries, r.RPCBusyRetries = uint64(f.wsBusy.Load()-startWSBusy), uint64(f.rpcBusy.Load()-startRPCBusy)
	if f.gate != nil {
		state := f.gate.Snapshot()
		r.Admission = &state
		r.AdmissionRejected = state.Rejected - startRejected
	}
	r.RPCAttempts = f.rpcAttempts.Load() - startRPC
	r.ServerRPCAttempts = f.serverRPCStarted.Load() - startServerRPC
	r.ServerPeakRPC = f.serverRPCPeak.Load()
	after := f.db.Stats()
	r.PoolWaitCount = after.WaitCount - before.WaitCount
	r.PoolWaitSeconds = (after.WaitDuration - before.WaitDuration).Seconds()
	closeSockets()
	time.Sleep(50 * time.Millisecond) // outside timing; let closed WS pumps retire
	r.GoAfterGoroutines = runtime.NumGoroutine()
	for _, lane := range r.Lanes {
		lane.finish()
	}
	t.Logf("%s %.2fs WS=%d RPC=%d poll=%d poolPeak=%d/25 waits=%d waitSeconds=%.3f", name, r.ElapsedSeconds, wsN, rpcN, pollN, r.PoolPeakInUse, r.PoolWaitCount, r.PoolWaitSeconds)
	return r
}

func mixedLoadInt(name string, fallback, maximum int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maximum {
		return 0, fmt.Errorf("%s must be1..%d", name, maximum)
	}
	return n, nil
}

func TestMixedMessagingLoadPostgres(t *testing.T) {
	if os.Getenv("ZCHAT_RUN_MIXED_LOAD") != "1" {
		t.Skip("opt-in short server-path diagnostic: scripts/load/mixed.py")
	}
	seconds, err := mixedLoadInt("ZCHAT_MIXED_LOAD_SECONDS", 8, 30)
	if err != nil {
		t.Fatal(err)
	}
	workers, err := mixedLoadInt("ZCHAT_MIXED_LOAD_WORKERS", 32, 64)
	if err != nil {
		t.Fatal(err)
	}
	startedAt := time.Now().UTC()
	f := newMixedLoadFixture(t, workers)
	// Explicit warmup, excluded from all measured counters/latencies.
	var warmup sync.WaitGroup
	errs := make(chan error, workers)
	for _, pair := range f.pairs {
		warmup.Go(func() {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			s, err := f.connect(ctx, pair)
			if err == nil {
				err = f.ordinary(ctx, pair, s)
				s.close()
			}
			if err == nil {
				err = f.encrypted(ctx, pair)
			}
			if err != nil {
				errs <- err
			}
		})
	}
	warmup.Wait()
	close(errs)
	for err := range errs {
		t.Fatal("warmup", err)
	}
	if mode := os.Getenv("ZCHAT_MIXED_ADMISSION"); mode != "off" {
		f.gate = admission.New(admission.DefaultConfig(), nil)
		f.auth.SetRequestAdmission(f.gate)
	}
	var phases []loadPhase
	for _, n := range []int{min(8, workers), workers} {
		for _, kind := range []string{"ordinary", "e2ee", "mixed"} {
			ws, rpc := 0, 0
			switch kind {
			case "ordinary":
				ws = n
			case "e2ee":
				rpc = n
			default:
				ws = n / 2
				rpc = n - ws
			}
			phases = append(phases, runLoadPhase(t, f, fmt.Sprintf("%s_%d", kind, n), seconds, ws, rpc, 0, false, false))
		}
	}
	phases = append(phases, runLoadPhase(t, f, "idle_poll", seconds, 0, 0, 2*workers, false, false))
	phases = append(phases, runLoadPhase(t, f, "mixed_reconnect_burst", 5, workers, workers, 0, true, false))
	phases = append(phases, runLoadPhase(t, f, "controlled_pool_stall", 5, workers, 4*workers, 0, true, true))
	counts := make(map[string]int64)
	for _, table := range []string{"messages", "e2ee_send_receipts", "e2ee_message_inbox", "e2ee_message_acknowledgements"} {
		var n int64
		if err := f.db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		counts[table] = n
	}
	if counts["messages"] < f.wsReceipts.Load() || counts["e2ee_send_receipts"] < f.e2eeReceipts.Load() {
		t.Error("acknowledged server receipts exceeded persisted rows")
	}
	report := struct {
		Version                                  int
		GoVersion                                string
		GOMAXPROCS, Pairs                        int
		StartedAt                                time.Time
		Limits                                   []string
		Phases                                   []loadPhase
		PersistedRows                            map[string]int64
		ObservedWSReceipts, ObservedE2EEReceipts int64
	}{1, runtime.Version(), runtime.GOMAXPROCS(0), len(f.pairs), startedAt, []string{"Go client+server share one process/CPU/heap; not independent server capacity or a full main/nginx/Grafana test", "Real WS+TLS gRPC/auth/transactions; synthetic512B ciphertext and simulated recipient ACK, NOT libsignal/native durable receive", "Closed-loop bounded workers, no HTTP history/WebRTC/key refill/rotation workload", "Latency percentiles use bounded4096-duration reservoirs; counts/mean/max exact; sampler20ms can miss shorter peaks", "Issue window then up to3s drain; only explicit Busy/ResourceExhausted retries exact request with bounded jitter; other failed Send may have committed", "Controlled pool stall intentionally holds all25connections500ms, NOT an observed disk/CPU incident"}, phases, counts, f.wsReceipts.Load(), f.e2eeReceipts.Load()}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := os.Getenv("ZCHAT_MIXED_LOAD_REPORT")
	if path == "" {
		t.Fatal("set ZCHAT_MIXED_LOAD_REPORT to a private report path")
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, phase := range phases {
		for _, lane := range phase.Lanes {
			for key := range lane.Errors {
				if strings.Contains(key, "Invalid") || strings.Contains(key, "MissingOrChanged") || strings.Contains(key, "WrongStored") {
					t.Errorf("%s invariant failure: %s", phase.Name, key)
				}
			}
		}
	}
}

func TestMixedLoadConfigurationBounds(t *testing.T) {
	const key = "ZCHAT_MIXED_LOAD_SECONDS"
	for _, raw := range []string{"0", "-1", "31", "1s", "999999999999999999999999"} {
		t.Setenv(key, raw)
		if _, err := mixedLoadInt(key, 8, 30); err == nil {
			t.Errorf("accepted invalid value %q", raw)
		}
	}
	for _, n := range []int{1, 8, 30} {
		t.Setenv(key, strconv.Itoa(n))
		got, err := mixedLoadInt(key, 8, 30)
		if got != n || err != nil {
			t.Fatal(got, err)
		}
	}
	t.Setenv(key, "")
	if got, err := mixedLoadInt(key, 8, 30); got != 8 || err != nil {
		t.Fatal(got, err)
	}
}
func TestMixedLoadStatisticsBoundedAndExactCounters(t *testing.T) {
	s := &loadLane{}
	for i := 1; i <= 10000; i++ {
		var err error
		if i%2 == 0 {
			err = loadFailure{"send", "Unavailable"}
		}
		s.record(time.Duration(i)*time.Millisecond, err)
	}
	s.finish()
	if s.Attempts != 10000 || s.Completed != 5000 || s.Errors["send:Unavailable"] != 5000 || s.Samples != 4096 || s.MeanMs != 5000.5 || s.MaxMs != 10000 {
		t.Fatalf("invalid count/mean/max/bound: %+v", s)
	}
	if s.P50Ms > s.P95Ms || s.P95Ms > s.P99Ms || s.P99Ms > s.MaxMs {
		t.Fatal("invalid sampled quantile order")
	}
	empty := &loadLane{}
	empty.finish()
	if empty.Samples != 0 || empty.MeanMs != 0 {
		t.Fatal("empty series must stay empty")
	}
}

func loadPause(ctx context.Context) error {
	timer := time.NewTimer(time.Duration(25+rand.IntN(76)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func loadIsBusy(err error) bool {
	var failure loadFailure
	return errors.As(err, &failure) && failure.code == "Busy"
}
func loadRPC[T any](ctx context.Context, f *mixedLoadFixture, call func() (T, error)) (T, error) {
	for {
		f.rpcAttempts.Add(1)
		result, err := call()
		if status.Code(err) != codes.ResourceExhausted {
			return result, err
		}
		f.rpcBusy.Add(1)
		if err = loadPause(ctx); err != nil {
			var zero T
			return zero, err
		}
	}
}
