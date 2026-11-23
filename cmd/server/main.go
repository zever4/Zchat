package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/joho/godotenv"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	pbChat "github.com/zever4/Zchat/gen/chat"
	pbFriends "github.com/zever4/Zchat/gen/friends"
	pbUser "github.com/zever4/Zchat/gen/user"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/calls"
	"github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/diagnostics"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/e2ee/wakeup"
	internalGrpc "github.com/zever4/Zchat/internal/grpc"
	"github.com/zever4/Zchat/internal/handlers"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/pkg/postgres"
	"github.com/zever4/Zchat/internal/redis"
	repo "github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if err := godotenv.Load(".env", "../.env", "../../.env"); err != nil {
		log.Println("No .env file found, using system environment variables")
	}
	jwtSecret := os.Getenv("JWT_SECRET")
	storageConfig, err := storagebudget.ConfigFromEnv(os.Getenv)
	if err != nil {
		return fmt.Errorf("storage configuration: %w", err)
	}
	blobConfig, err := blobConfigFromEnv(os.Getenv, storageConfig)
	if err != nil {
		return fmt.Errorf("private storage configuration: %w", err)
	}
	if err := service.ValidateJWTSecret(jwtSecret); err != nil {
		return err
	}
	browserPolicy, err := handlers.NewBrowserPolicy(os.Getenv("LIVEKIT_URL"))
	if err != nil {
		return err
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()
	stopProfiler, err := diagnostics.Start(ctx, os.Getenv("ENABLE_PPROF") == "true")
	if err != nil {
		return err
	}
	defer stopProfiler()
	startupCtx, startupCancel := context.WithTimeout(ctx, 60*time.Second)
	db, err := postgres.InitPostgresDB(startupCtx)
	startupCancel()
	if err != nil {
		return err
	}
	resourcesJoined := true
	defer func() {
		if resourcesJoined {
			postgres.Close(db)
		}
	}()
	storageCtx, storageCancel := context.WithTimeout(ctx, 60*time.Second)
	uploads, err := uploadstorage.Open(storageCtx, "./web/uploads", storageConfig)
	storageCancel()
	if err != nil {
		return fmt.Errorf("initialize upload storage: %w", err)
	}
	defer func() {
		if !resourcesJoined {
			return
		}
		if err := uploads.Close(); err != nil {
			log.Printf("upload storage shutdown: %v", err)
		}
	}()
	dbMetrics := collectors.NewDBStatsCollector(db, "postgres")
	if err := prometheus.Register(dbMetrics); err != nil {
		return fmt.Errorf("register DB metrics: %w", err)
	}
	defer prometheus.Unregister(dbMetrics)
	defer cancel()

	// Repositories
	userRepo := repo.NewPgUserRepository(db)
	messageRepo := repo.NewPgMessageRepository(db)
	attachmentRepo := repo.NewPgAttachmentRepository(db)
	friendRepo := repo.NewPgFriendRequestRepository(db)
	chatRepo := repo.NewPgChatRepository(db)

	// Services
	authService := service.NewAuthService(userRepo, jwtSecret)
	requestGate := admission.New(admission.DefaultConfig(), metrics.ObserveAdmission)
	authService.SetRequestAdmission(requestGate)
	admissionMetrics := metrics.AdmissionActive(requestGate)
	if err := prometheus.Register(admissionMetrics); err != nil {
		return fmt.Errorf("register admission metrics: %w", err)
	}
	defer prometheus.Unregister(admissionMetrics)
	callGateway, err := calls.NewGateway(ctx, authService, chatRepo, os.Getenv("LIVEKIT_INTERNAL_URL"), os.Getenv("LIVEKIT_API_KEY"), os.Getenv("LIVEKIT_API_SECRET"))
	if err != nil {
		return err
	}
	if callGateway.Enabled() {
		metrics.CallsEnabled.Set(1)
	} else {
		metrics.CallsEnabled.Set(0)
	}
	deviceService := service.NewE2EEDeviceService(repo.NewPgE2EEDeviceRepository(db), authService)
	publicRoot, err := os.OpenRoot("./web")
	if err != nil {
		return fmt.Errorf("open public assets: %w", err)
	}
	defer publicRoot.Close()
	blobs, err := startBlobStorage(ctx, blobConfig, publicRoot, uploads.Budget(), repo.NewPgE2EEBlobRepository(db), authService, deviceService, metrics.ObserveBlobCleanup)
	if err != nil {
		return fmt.Errorf("initialize private blob storage: %w", err)
	}
	if blobs != nil {
		log.Println("Experimental private blob storage enabled; attachment Send remains disabled")
	}
	defer func() {
		if blobs == nil {
			return
		}
		closeCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := blobs.Close(closeCtx); err != nil {
			// Keep DB and shared owner alive if a producer/query did not join.
			// Process exit will terminate the remaining work; never unlock early.
			resourcesJoined = false
			log.Println("Private blob shutdown incomplete; shared resources retained until process exit")
		}
	}()
	e2eeAuth := internalGrpc.NewE2EEAuth(ctx, authService, deviceService)
	deviceService.SetDeviceReplacer(e2eeAuth.DisconnectGeneration)
	authService.SetSessionRevoker(func(ctx context.Context, userID int64, sessionID string) error {
		e2eeAuth.DisconnectSession(userID, sessionID)
		return callGateway.DisconnectSession(ctx, userID, sessionID)
	})
	var background sync.WaitGroup
	e2eeDeliveryRepo := repo.NewPgE2EEDeliveryRepository(db)
	// Accepted copies still need retention cleanup while the experimental API is
	// disabled. One serial worker shares the existing pool; no per-account jobs.
	cleanup := service.NewE2EECleanupWorker(e2eeDeliveryRepo, func(lane domain.E2EECleanupLane, result domain.E2EECleanupResult, err error) {
		name := "expired"
		if lane == domain.E2EECleanupReplaced {
			name = "replaced"
		}
		outcome := "ok"
		if err != nil {
			outcome = "error"
			kind := "storage"
			if errors.Is(err, domain.ErrE2EEMessageState) {
				kind = "state_invariant"
			} else if errors.Is(err, context.DeadlineExceeded) {
				kind = "deadline"
			}
			log.Printf("E2EE cleanup deferred: lane=%s cause=%s", name, kind)
		}
		metrics.E2EECleanupPasses.WithLabelValues(name, outcome).Inc()
		metrics.E2EECleanupRemoved.WithLabelValues(name).Add(float64(result.Removed))
		metrics.E2EECleanupBusy.WithLabelValues(name).Add(float64(result.Busy))
	})
	cleanupCtx, stopCleanup := context.WithCancel(ctx)
	cleanupDone := make(chan struct{})
	background.Go(func() { defer close(cleanupDone); cleanup.Run(cleanupCtx) })
	// Also join this worker on startup errors, before closing its database pool.
	defer func() { stopCleanup(); <-cleanupDone }()
	background.Add(1)
	go func() { defer background.Done(); callGateway.Run() }()
	background.Add(1)
	go func() {
		defer background.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				authService.CleanupWebSocketTickets()
				cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 3*time.Second)
				err := userRepo.CleanupExpiredSessions(cleanupCtx)
				cleanupCancel()
				if err != nil {
					log.Printf("expired session cleanup: %v", err)
				}
			}
		}
	}()
	friendService := service.NewFriendService(friendRepo, chatRepo, userRepo, repo.NewPgFriendTransactor(db))
	chatService := service.NewChatService(chatRepo, messageRepo)

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		return fmt.Errorf("listen gRPC: %w", err)
	}

	defer lis.Close()
	userSrv := internalGrpc.NewUserGRPCServer(userRepo, authService)
	chatSrv := internalGrpc.NewChatGRPCServer(messageRepo, *chatService)
	friendSrv := internalGrpc.NewFriendGRPCServer(friendService)
	srvMetrics := grpcprom.NewServerMetrics(grpcprom.WithServerHandlingTimeHistogram(grpcprom.WithHistogramBuckets(prometheus.DefBuckets)))
	if err := prometheus.Register(srvMetrics); err != nil {
		return fmt.Errorf("register gRPC metrics: %w", err)
	}
	defer prometheus.Unregister(srvMetrics)
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(internalGrpc.MaxRequestSize),
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
			serverContextInterceptor(ctx),
			e2eeAuth.Unary(userSrv.AuthInterceptor),
		),
		grpc.ChainStreamInterceptor(srvMetrics.StreamServerInterceptor(), e2eeAuth.Stream()),
	)

	if internalGrpc.RegisterOptionalEpochMessenger(grpcServer, os.Getenv("ENABLE_EXPERIMENTAL_E2EE"), func() (*internalGrpc.E2EEDeviceServer, *internalGrpc.E2EEEpochServer) {
		var hintRedis *redis.Client
		if addr := strings.TrimSpace(os.Getenv("E2EE_REDIS_ADDR")); addr != "" {
			hintRedis = redis.NewClient(addr)
		}
		hints := wakeup.New(hintRedis)
		background.Go(func() { hints.Run(ctx) })
		keys := repo.NewPgE2EEPreKeyRepository(db)
		registry := internalGrpc.NewE2EEDeviceServer(deviceService, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repo.NewPgE2EEConversationRepository(db)), nil).WithWakeups(hints)
		epochService := service.NewE2EEEpochService(e2eeDeliveryRepo, keys, repo.NewPgE2EERecoveryRepository(db))
		epoch := internalGrpc.NewE2EEEpochServer(epochService).WithWakeups(hints)
		if blobs != nil && blobs.expose {
			epoch.WithBlobControls(blobs)
			epochService.WithAttachmentAdmission(blobs)
		}
		return registry, epoch
	}) {
		log.Println("Experimental E2EE registry + v2 epoch core enabled; native/UI messaging not ready")
	} else {
		log.Println("Experimental E2EE Messenger API disabled")
	}
	pbUser.RegisterUserServiceServer(grpcServer, userSrv)
	pbChat.RegisterChatServiceServer(grpcServer, chatSrv)
	pbFriends.RegisterFriendServiceServer(grpcServer, friendSrv)
	srvMetrics.InitializeMetrics(grpcServer)

	reflection.Register(grpcServer)

	serverErrors := make(chan error, 2)
	var blobErrors <-chan error
	if blobs != nil {
		blobErrors = blobs.errors
	}
	go func() {
		log.Printf("gRPC server listening at %v", lis.Addr())
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErrors <- fmt.Errorf("serve gRPC: %w", err)
		}
	}()
	defer func() { go grpcServer.Stop() }()
	hub := chat.NewHub()
	hubQueue := prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "websocket_hub_queue_length", Help: "Broadcasts waiting in the hub queue"}, func() float64 { return float64(len(hub.Direct)) })
	if err := prometheus.Register(hubQueue); err != nil {
		return fmt.Errorf("register hub metrics: %w", err)
	}
	defer prometheus.Unregister(hubQueue)
	go hub.Run(ctx)
	defer hub.Stop()

	mux := http.NewServeMux()
	blobs.Register(mux) // Raw ciphertext owns auth/streaming; never ProtectSession.
	authThrottle := handlers.NewAuthThrottle()
	authenticatedHandle := func(pattern string, h http.HandlerFunc) {
		mux.HandleFunc(pattern, handlers.ProtectSession(authService, h, false, uploads))
	}

	gwMux := runtime.NewServeMux(
		runtime.WithIncomingHeaderMatcher(internalGrpc.CustomMatcher),
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: protojson.MarshalOptions{
				UseProtoNames:   true,
				EmitUnpopulated: true,
			},
		}),
	)

	gatewayConn, err := grpc.NewClient("127.0.0.1:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("gateway client: %w", err)
	}
	defer gatewayConn.Close()
	if err := pbUser.RegisterUserServiceHandler(ctx, gwMux, gatewayConn); err != nil {
		return err
	}
	if err := pbChat.RegisterChatServiceHandler(ctx, gwMux, gatewayConn); err != nil {
		return err
	}
	if err := pbFriends.RegisterFriendServiceHandler(ctx, gwMux, gatewayConn); err != nil {
		return err
	}

	fileServer := browserPolicy.Wrap(http.FileServerFS(publicRoot.FS()))
	mux.HandleFunc("GET /uploads/", handlers.ProtectSession(authService, handlers.HandleDownload(authService, attachmentRepo), true, uploads))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if handlers.IsUploadPath(r.URL.Path) {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/login.html", http.StatusFound)
			return
		}

		switch strings.ToLower(filepath.Ext(r.URL.Path)) {
		case ".html":
			// HTML must never stick in browser cache, otherwise it can keep
			// referencing old versioned CSS/JS and resurrect an outdated UI.
			w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")
		case ".css", ".js":
			if r.URL.Query().Get("v") != "" {
				w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
			} else {
				w.Header().Set("Cache-Control", "no-cache")
			}
		}

		fileServer.ServeHTTP(w, r)
	})

	// WebSocket
	mux.HandleFunc("GET /ws", func(w http.ResponseWriter, r *http.Request) {
		chat.ServeWs(hub, w, r, authService, userRepo, messageRepo, chatRepo)
	})
	ticketThrottle := handlers.NewAuthThrottle()
	mux.HandleFunc("POST /api/ws-ticket", ticketThrottle.Wrap(handlers.HandleWebSocketTicket(authService)))

	// Healthcheck
	mux.HandleFunc("/health", handlers.HandleHealthcheck(db))

	// Auth
	mux.HandleFunc("/api/login", authThrottle.Wrap(handlers.HandleLogin(authService)))
	mux.HandleFunc("/api/register", authThrottle.Wrap(handlers.HandleRegister(authService)))

	// User search
	authenticatedHandle("GET /api/users/search", handlers.HandleSearchUsers(authService, userRepo))

	// Profile
	authenticatedHandle("POST /api/profile/avatar", handlers.HandleUploadAvatar(authService, userRepo))
	mux.HandleFunc("PUT /api/profile/password", authThrottle.Wrap(handlers.HandleChangePassword(authService, hub)))
	mux.HandleFunc("POST /api/profile/logout-all", handlers.HandleLogoutSession(authService, hub, true))
	mux.HandleFunc("POST /api/profile/logout", handlers.HandleLogoutSession(authService, hub, false))
	authenticatedHandle("GET /api/settings", handlers.HandleGetSettings(authService, userRepo))
	authenticatedHandle("PUT /api/settings", handlers.HandleUpdateSettings(authService, userRepo))

	// Media
	authenticatedHandle("POST /api/media/upload", handlers.HandleUploadMedia(authService, attachmentRepo))
	authenticatedHandle("POST /api/files/upload", handlers.HandleUploadFile(authService, attachmentRepo))

	// Friends
	authenticatedHandle("POST /api/friends/request", handlers.HandleSendFriendRequest(authService, friendService))
	// mux.HandleFunc("GET /api/friends/requests", handlers.HandleGetPendingRequests(authService, friendService))
	authenticatedHandle("POST /api/friends/requests/{id}/accept", handlers.HandleAcceptRequest(authService, friendService))
	authenticatedHandle("POST /api/friends/requests/{id}/reject", handlers.HandleRejectRequest(authService, friendService))
	authenticatedHandle("GET /api/friends", handlers.HandleGetFriends(authService, friendService))

	// Chats
	authenticatedHandle("POST /api/users/{id}/private-chat", handlers.HandleOpenPrivateChat(authService, friendService))
	authenticatedHandle("GET /api/chats/{id}/members", handlers.HandleGetChatMembers(authService, chatRepo, userRepo))
	authenticatedHandle("POST /api/groups", handlers.HandleCreateGroup(authService, chatRepo))
	authenticatedHandle("GET /api/groups/{id}", handlers.HandleGetGroup(authService, chatRepo))
	authenticatedHandle("PUT /api/groups/{id}", handlers.HandleUpdateGroup(authService, chatRepo))
	authenticatedHandle("DELETE /api/groups/{id}", handlers.HandleDeleteGroup(authService, chatRepo, callGateway))
	authenticatedHandle("POST /api/groups/{id}/avatar", handlers.HandleUploadGroupAvatar(authService, chatRepo))
	authenticatedHandle("POST /api/groups/{id}/invite/regenerate", handlers.HandleRegenerateGroupInvite(authService, chatRepo))
	authenticatedHandle("GET /api/groups/{id}/invites", handlers.HandleListGroupInvites(authService, chatRepo))
	authenticatedHandle("POST /api/groups/{id}/invites", handlers.HandleCreateGroupInvite(authService, chatRepo))
	authenticatedHandle("PUT /api/groups/{id}/invites/{invite_id}", handlers.HandleSetGroupInviteActive(authService, chatRepo))
	authenticatedHandle("DELETE /api/groups/{id}/invites/{invite_id}", handlers.HandleDeleteGroupInvite(authService, chatRepo))
	authenticatedHandle("GET /api/groups/{id}/audit", handlers.HandleListGroupAuditLogs(authService, chatRepo))
	authenticatedHandle("GET /api/group-invites/{code}", handlers.HandleGetGroupInviteInfo(authService, chatRepo))
	authenticatedHandle("POST /api/groups/invite/{code}/join", handlers.HandleJoinGroupByInvite(authService, chatRepo))
	authenticatedHandle("GET /api/groups/{id}/requests", handlers.HandleListGroupJoinRequests(authService, chatRepo))
	authenticatedHandle("POST /api/groups/{id}/requests/{user_id}/approve", handlers.HandleApproveGroupJoinRequest(authService, chatRepo))
	authenticatedHandle("POST /api/groups/{id}/requests/{user_id}/reject", handlers.HandleRejectGroupJoinRequest(authService, chatRepo))
	authenticatedHandle("GET /api/groups/{id}/members", handlers.HandleListGroupMembers(authService, chatRepo))
	authenticatedHandle("PUT /api/groups/{id}/members/{user_id}/role", handlers.HandleSetMemberRole(authService, chatRepo))
	authenticatedHandle("DELETE /api/groups/{id}/members/{user_id}", handlers.HandleRemoveGroupMember(authService, chatRepo, callGateway))
	authenticatedHandle("GET /api/groups/{id}/permissions", handlers.HandleGetGroupPermissions(authService, chatRepo))
	authenticatedHandle("PUT /api/groups/{id}/permissions", handlers.HandleUpdateGroupPermissions(authService, chatRepo))

	// Message edit/delete
	authenticatedHandle("DELETE /api/messages/{id}", handlers.HandleDeleteMessage(authService, messageRepo, hub, chatRepo))
	authenticatedHandle("PUT /api/messages/{id}", handlers.HandleEditMessage(authService, messageRepo, hub, chatRepo))

	// Calls
	authenticatedHandle("GET /api/calls/join", handlers.HandleJoinCall(authService, chatRepo, userRepo, callGateway))
	mux.Handle("/rtc/", callGateway)

	// gRPC
	// mux.Handle("GET /api/users/", gwMux)

	// mux.Handle("GET /api/profile", gwMux)
	// mux.Handle("PUT /api/profile", gwMux)

	// mux.Handle("GET /api/chats", gwMux)
	// mux.Handle("GET /api/chats/", gwMux)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		if r.URL.Path != "/" && r.URL.Path[len(r.URL.Path)-1] == '/' {
			r.URL.Path = r.URL.Path[:len(r.URL.Path)-1]
		}
		gwMux.ServeHTTP(w, r)
	})

	mux.Handle("/api/", handler)

	mux.Handle("/metrics", promhttp.Handler())

	muxWithMiddleware := metrics.MetricsMiddleware(handlers.LimitRequests(mux, mux), mux)
	srv := &http.Server{
		Addr:              ":8080",
		BaseContext:       func(net.Listener) context.Context { return ctx },
		Handler:           muxWithMiddleware,
		MaxHeaderBytes:    16 << 10,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	start := time.Now()
	log.Println("Server started on :8080")

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErrors <- fmt.Errorf("serve HTTP: %w", err)
		}
	}()
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-serverErrors:
	case <-blobErrors:
		serveErr = errors.New("private blob collector stopped unexpectedly")
	}
	log.Println("Shutting down servers...")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := shutdownServers(shutdownCtx, srv, grpcServer, hub); err != nil {
		log.Printf("Server shutdown: %v", err)
	}
	backgroundDone := make(chan struct{})
	go func() { background.Wait(); close(backgroundDone) }()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		log.Println("Session cleanup shutdown deadline reached")
	}
	log.Println("Server worked for: ", time.Since(start))
	return serveErr
}
