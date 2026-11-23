package grpc

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/service"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const E2EEDeviceCredentialMetadata = "x-zchat-device-credential-bin"
const e2eeMethodPrefix = "/zchat.e2ee.v1.Messenger/"
const e2eeEpochMethodPrefix = "/zchat.e2ee.v2.Messenger/"
const maxE2EEStreams = 1024
const maxE2EEStreamsPerAccount = 4

type e2eePrincipalKey struct{}
type e2eePrincipal struct {
	userID int64
	device *domain.E2EEDevice
}
type e2eeStreamLease struct {
	userID                int64
	sessionID, generation string
	cancel                context.CancelCauseFunc
}

type E2EEAuth struct {
	root                context.Context
	auth                *service.AuthService
	devices             *service.E2EEDeviceService
	pollInterval        time.Duration
	firstRequestTimeout time.Duration
	watchLifetime       time.Duration
	mu                  sync.Mutex
	streams             map[*e2eeStreamLease]struct{}
}

func NewE2EEAuth(root context.Context, auth *service.AuthService, devices *service.E2EEDeviceService) *E2EEAuth {
	return &E2EEAuth{root: root, auth: auth, devices: devices, pollInterval: time.Second, firstRequestTimeout: 5 * time.Second, watchLifetime: 50 * time.Second, streams: make(map[*e2eeStreamLease]struct{})}
}

func (g *E2EEAuth) addLease(lease *e2eeStreamLease) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.streams) >= maxE2EEStreams {
		return false
	}
	n := 0
	for active := range g.streams {
		if active.userID == lease.userID {
			n++
		}
	}
	if n >= maxE2EEStreamsPerAccount {
		return false
	}
	g.streams[lease] = struct{}{}
	return true
}

func e2eeToken(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("authorization")
	if len(v) != 1 || len(md.Get("grpc-metadata-authorization")) != 0 {
		return "", domain.ErrInvalidSession
	}
	return service.BearerToken(v[0])
}
func e2eeCredential(ctx context.Context) ([]byte, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get(E2EEDeviceCredentialMetadata)
	if len(v) != 1 || len(v[0]) != 32 || len(md.Get("grpc-metadata-"+E2EEDeviceCredentialMetadata)) != 0 {
		return nil, domain.ErrE2EEDeviceCredential
	}
	return []byte(v[0]), nil
}
func e2eeStatus(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, admission.ErrBusy):
		return status.Error(codes.ResourceExhausted, "server busy; retry later")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "E2EE request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "E2EE request deadline exceeded")
	case errors.Is(err, domain.ErrInvalidSession):
		return e2eeFailure(codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_SESSION_INVALID, "invalid session")
	case errors.Is(err, domain.ErrE2EEDeviceCredential):
		return e2eeFailure(codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_DEVICE_CREDENTIAL_INVALID, "invalid device credential")
	case errors.Is(err, domain.ErrInvalidCredentials):
		return e2eeFailure(codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_PASSWORD_INVALID, "invalid password")
	case errors.Is(err, boundary.ErrUnsupportedVersion):
		return status.Error(codes.FailedPrecondition, "unsupported E2EE protocol version")
	case errors.Is(err, boundary.ErrInvalid), errors.Is(err, domain.ErrInvalidInput):
		return status.Error(codes.InvalidArgument, "invalid E2EE request")
	case errors.Is(err, domain.ErrE2EEDeviceConflict), errors.Is(err, domain.ErrE2EERequestConflict), errors.Is(err, domain.ErrE2EEKeyConflict):
		return status.Error(codes.AlreadyExists, "E2EE device or request conflict")
	case errors.Is(err, domain.ErrE2EEMessageConflict):
		return status.Error(codes.AlreadyExists, "E2EE message identifier conflicts with saved data")
	case errors.Is(err, domain.ErrE2EEMessageCapacity):
		return status.Error(codes.ResourceExhausted, "E2EE message storage capacity exhausted")
	case errors.Is(err, domain.ErrE2EEMessageNotFound):
		return status.Error(codes.NotFound, "E2EE message unavailable")
	case errors.Is(err, domain.ErrE2EEGenerationChanged):
		return e2eeFailure(codes.FailedPrecondition, pb.FailureReason_FAILURE_REASON_GENERATION_CHANGED, "E2EE device generation changed")
	case errors.Is(err, domain.ErrE2EEPublicationChanged):
		return status.Error(codes.Aborted, "E2EE public-key revision changed; refresh device state")
	case errors.Is(err, domain.ErrE2EEConversationNotFound), errors.Is(err, domain.ErrE2EEPeerUnavailable):
		return status.Error(codes.NotFound, "E2EE conversation or peer unavailable")
	case errors.Is(err, domain.ErrNotFriends), errors.Is(err, domain.ErrDirectMessagesDisabled):
		return status.Error(codes.PermissionDenied, "new E2EE conversation is not permitted")
	case errors.Is(err, domain.ErrE2EEKeysNotReady):
		return status.Error(codes.FailedPrecondition, "both E2EE devices must publish keys first")
	case errors.Is(err, domain.ErrE2EEConversationClosed):
		return status.Error(codes.FailedPrecondition, "E2EE conversation is closed; start a new conversation")
	case errors.Is(err, domain.ErrE2EEAcquisitionExpired):
		return e2eeFailure(codes.FailedPrecondition, pb.FailureReason_FAILURE_REASON_ACQUISITION_EXPIRED, "E2EE bundle acquisition expired")
	case errors.Is(err, domain.ErrE2EEAcquisitionCapacity):
		return status.Error(codes.ResourceExhausted, "E2EE bundle acquisition capacity exhausted")
	case errors.Is(err, domain.ErrE2EEConversationCapacity):
		return status.Error(codes.ResourceExhausted, "E2EE conversation capacity exhausted")
	case errors.Is(err, domain.ErrE2EEKeyCapacity):
		return status.Error(codes.ResourceExhausted, "E2EE retained public-key capacity exhausted")
	case errors.Is(err, domain.ErrE2EEReauthLimited), errors.Is(err, service.ErrAuthBusy):
		return status.Error(codes.ResourceExhausted, "E2EE authentication capacity exhausted; retry later")
	case errors.Is(err, domain.ErrUserNotFound):
		return e2eeFailure(codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_SESSION_INVALID, "invalid session")
	default:
		return status.Error(codes.Unavailable, "E2EE storage or authentication temporarily unavailable")
	}
}

func e2eeFailure(code codes.Code, reason pb.FailureReason, message string) error {
	s := status.New(code, message)
	withDetail, err := s.WithDetails(&pb.Failure{Reason: reason})
	if err != nil {
		return s.Err() // Generic code remains safe for older/unsupported clients.
	}
	return withDetail.Err()
}

// Dispatch instead of nesting AuthService read barriers: a queued writer must
// not deadlock a nested read acquisition. Non-E2EE APIs keep their old guard.
func (g *E2EEAuth) Unary(fallback grpcgo.UnaryServerInterceptor) grpcgo.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
		epochRoute := strings.HasPrefix(info.FullMethod, e2eeEpochMethodPrefix)
		if !epochRoute && !strings.HasPrefix(info.FullMethod, e2eeMethodPrefix) {
			return fallback(ctx, req, info, handler)
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		token, err := e2eeToken(ctx)
		if err != nil {
			return nil, e2eeStatus(err)
		}
		claims, release, err := g.auth.AuthorizeContext(ctx, token)
		if err != nil {
			return nil, e2eeStatus(err)
		}
		defer release()
		message, ok := req.(proto.Message)
		if !ok || proto.Size(message) > boundary.MaxRequestBytes {
			return nil, e2eeStatus(boundary.ErrInvalid)
		}
		principal := e2eePrincipal{userID: claims.UserID}
		prefix := e2eeMethodPrefix
		if epochRoute {
			prefix = e2eeEpochMethodPrefix
		}
		method := strings.TrimPrefix(info.FullMethod, prefix)
		switch {
		case method == "GetCapabilities" || !epochRoute && method == "GetOwnDevice": // Owner JWT only.
		case !epochRoute && (method == "RegisterDevice" || method == "ReplaceOwnDevice"):
			if _, err := e2eeCredential(ctx); err != nil {
				return nil, e2eeStatus(err)
			}
		default:
			contextual, ok := req.(interface{ GetContext() *pb.ClientContext })
			if !ok {
				return nil, e2eeStatus(boundary.ErrInvalid)
			}
			validate := boundary.ValidateContext
			if epochRoute {
				validate = boundary.ValidateEpochContext
			}
			if err := validate(contextual.GetContext()); err != nil {
				return nil, e2eeStatus(err)
			}
			raw, err := e2eeCredential(ctx)
			if err != nil {
				return nil, e2eeStatus(err)
			}
			device, unlock, err := g.devices.Authorize(ctx, claims.UserID, contextual.GetContext().DeviceGenerationId, raw)
			if err != nil {
				return nil, e2eeStatus(err)
			}
			defer unlock()
			principal.device = device
		}
		return handler(context.WithValue(ctx, e2eePrincipalKey{}, principal), req)
	}
}

func (g *E2EEAuth) DisconnectSession(userID int64, sessionID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for lease := range g.streams {
		if lease.userID == userID && (sessionID == "" || lease.sessionID == sessionID) {
			lease.cancel(domain.ErrInvalidSession)
		}
	}
}
func (g *E2EEAuth) DisconnectGeneration(userID int64, generation string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for lease := range g.streams {
		if lease.userID == userID && lease.generation == generation {
			lease.cancel(domain.ErrE2EEDeviceCredential)
		}
	}
}

func (g *E2EEAuth) Stream() grpcgo.StreamServerInterceptor {
	return func(srv any, stream grpcgo.ServerStream, info *grpcgo.StreamServerInfo, handler grpcgo.StreamHandler) error {
		if !strings.HasPrefix(info.FullMethod, e2eeMethodPrefix) {
			return handler(srv, stream)
		}
		token, err := e2eeToken(stream.Context())
		if err != nil {
			return e2eeStatus(err)
		}
		raw, err := e2eeCredential(stream.Context())
		if err != nil {
			return e2eeStatus(err)
		}
		admission, cancelAdmission := context.WithTimeout(stream.Context(), 5*time.Second)
		claims, release, err := g.auth.AuthorizeContext(admission, token)
		if err != nil {
			cancelAdmission()
			return e2eeStatus(err)
		}
		device, unlock, err := g.devices.Authorize(admission, claims.UserID, "", raw)
		if err != nil {
			release()
			cancelAdmission()
			return e2eeStatus(err)
		}
		expires, stopExpiry := context.WithDeadlineCause(stream.Context(), claims.ExpiresAt.Time, domain.ErrInvalidSession)
		ctx, cancel := context.WithCancelCause(expires)
		if info.FullMethod == pb.Messenger_WatchInbox_FullMethodName {
			// The interceptor can close transport even while SendMsg is blocked.
			lifetime := time.AfterFunc(g.watchLifetime, func() { cancel(context.DeadlineExceeded) })
			defer lifetime.Stop()
		}
		stopRoot := context.AfterFunc(g.root, func() { cancel(context.Canceled) })
		lease := &e2eeStreamLease{userID: claims.UserID, sessionID: claims.ID, generation: device.GenerationID, cancel: cancel}
		// Subscribe under admission barriers, closing the validation/revoke gap.
		if !g.addLease(lease) {
			unlock()
			release()
			cancelAdmission()
			stopRoot()
			stopExpiry()
			cancel(context.Canceled)
			return status.Error(codes.ResourceExhausted, "E2EE stream capacity exhausted")
		}
		unlock()
		release()
		cancelAdmission()
		wrapped := &e2eeServerStream{ServerStream: stream, ctx: context.WithValue(ctx, e2eePrincipalKey{}, e2eePrincipal{claims.UserID, device}), guard: g, token: token, credential: raw, lease: lease, received: make(chan struct{}, 1)}
		firstRequest := time.AfterFunc(g.firstRequestTimeout, func() {
			select {
			case <-wrapped.received:
			default:
				cancel(context.DeadlineExceeded)
			}
		})
		defer firstRequest.Stop()
		done := make(chan struct{})
		go func() {
			defer close(done)
			ticker := time.NewTicker(g.pollInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := wrapped.check(); err != nil {
						cancel(err)
						return
					}
				}
			}
		}()
		defer func() {
			cancel(context.Canceled)
			stopRoot()
			stopExpiry()
			<-done
			g.mu.Lock()
			delete(g.streams, lease)
			g.mu.Unlock()
		}()
		// RecvMsg/SendMsg use gRPC's original transport context. Canceling the
		// derived authorization context alone cannot unblock them. Returning from
		// the interceptor closes the transport and releases the handler's I/O.
		// Buffered completion prevents a late handler return from blocking forever.
		result := make(chan error, 1)
		go func() { result <- handler(srv, wrapped) }()
		select {
		case err := <-result:
			if cause := context.Cause(ctx); cause != nil {
				return e2eeStatus(cause)
			}
			return err
		case <-ctx.Done():
			return e2eeStatus(context.Cause(ctx))
		}
	}
}

type e2eeServerStream struct {
	grpcgo.ServerStream
	ctx        context.Context
	guard      *E2EEAuth
	token      string
	credential []byte
	lease      *e2eeStreamLease
	received   chan struct{}
}

func (s *e2eeServerStream) Context() context.Context { return s.ctx }
func (s *e2eeServerStream) RecvMsg(message any) error {
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	req, ok := message.(*pb.StreamMessagesRequest)
	if !ok {
		return e2eeStatus(boundary.ErrInvalid)
	}
	if err := boundary.ValidateStream(req); err != nil {
		return e2eeStatus(err)
	}
	if req.Context.DeviceGenerationId != s.lease.generation {
		return e2eeStatus(domain.ErrE2EEDeviceCredential)
	}
	select {
	case s.received <- struct{}{}:
	default:
	}
	return nil
}
func (s *e2eeServerStream) check() error {
	ctx, cancel := context.WithTimeout(s.ctx, 3*time.Second)
	defer cancel()
	_, release, err := s.guard.auth.AuthorizeContext(ctx, s.token)
	if err != nil {
		return err
	}
	defer release()
	_, unlock, err := s.guard.devices.Authorize(ctx, s.lease.userID, s.lease.generation, s.credential)
	if err != nil {
		return err
	}
	unlock()
	return nil
}
func (s *e2eeServerStream) SendMsg(message any) error {
	if err := s.check(); err != nil {
		return e2eeStatus(err)
	}
	if cause := context.Cause(s.ctx); cause != nil {
		return e2eeStatus(cause)
	}
	// Never hold revocation barriers over a network write. D5's bounded writer
	// must handle transport backpressure; an already admitted frame is in flight.
	return s.ServerStream.SendMsg(message)
}
