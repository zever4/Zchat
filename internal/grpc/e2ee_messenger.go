package grpc

import (
	"context"
	"fmt"
	"log"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/redis"
	"github.com/zever4/Zchat/internal/repository"

	pb "github.com/zever4/Zchat/gen/messenger"
)

type E2EEMessenger struct {
	pb.UnimplementedMessengerServer

	deviceRepo repository.DeviceKeyRepository
	otpRepo    repository.OneTimePreKeyRepository
	kyberRepo  repository.KyberPreKeyRepository
	msgRepo    repository.E2EEMessageRepository
	redis      *redis.Client

	// Локальный fallback: user_id:device_id -> stream
	streams map[string]pb.Messenger_StreamMessagesServer
	mu      sync.RWMutex
}

func NewE2EEMessenger(
	deviceRepo repository.DeviceKeyRepository,
	otpRepo repository.OneTimePreKeyRepository,
	kyberRepo repository.KyberPreKeyRepository,
	msgRepo repository.E2EEMessageRepository,
	redis *redis.Client,
) *E2EEMessenger {
	return &E2EEMessenger{
		deviceRepo: deviceRepo,
		otpRepo:    otpRepo,
		kyberRepo:  kyberRepo,
		msgRepo:    msgRepo,
		redis:      redis,
		streams:    make(map[string]pb.Messenger_StreamMessagesServer),
	}
}

func (s *E2EEMessenger) UploadPreKeys(ctx context.Context, req *pb.PreKeyBundleUpload) (*pb.Empty, error) {
	dk := &domain.DeviceKeys{
		UserID:                req.UserId,
		DeviceID:              int(req.DeviceId),
		RegistrationID:        req.RegistrationId,
		IdentityKey:           req.IdentityKey,
		SignedPreKeyID:        req.SignedPreKey.KeyId,
		SignedPreKeyPublic:    req.SignedPreKey.PublicKey,
		SignedPreKeySignature: req.SignedPreKey.Signature,
		SignedPreKeyTimestamp: int64(req.SignedPreKeyTimestamp),
	}
	if err := s.deviceRepo.Upsert(ctx, dk); err != nil {
		return nil, fmt.Errorf("upsert device keys: %w", err)
	}

	if len(req.OneTimePreKeys) > 0 {
		keys := make([]domain.OneTimePreKey, len(req.OneTimePreKeys))
		for i, k := range req.OneTimePreKeys {
			keys[i] = domain.OneTimePreKey{
				UserID:    req.UserId,
				DeviceID:  int(req.DeviceId),
				KeyID:     k.KeyId,
				PublicKey: k.PublicKey,
			}
		}
		if err := s.otpRepo.BulkInsert(ctx, req.UserId, int(req.DeviceId), keys); err != nil {
			return nil, fmt.Errorf("bulk insert otp: %w", err)
		}
	}

	if len(req.KyberPreKeys) > 0 {
		keys := make([]domain.KyberPreKey, len(req.KyberPreKeys))
		for i, k := range req.KyberPreKeys {
			keys[i] = domain.KyberPreKey{
				UserID:       req.UserId,
				DeviceID:     int(req.DeviceId),
				KeyID:        k.KeyId,
				PublicKey:    k.PublicKey,
				Signature:    k.Signature,
				IsLastResort: k.IsLastResort,
			}
		}
		if err := s.kyberRepo.BulkInsert(ctx, req.UserId, int(req.DeviceId), keys); err != nil {
			return nil, fmt.Errorf("bulk insert kyber: %w", err)
		}
	}

	return &pb.Empty{}, nil
}

func (s *E2EEMessenger) FetchPreKeys(ctx context.Context, req *pb.UserId) (*pb.PreKeyBundles, error) {
	userID := req.UserId
	deviceID := int(req.GetDeviceId()) // 0 если не задан

	// Если device_id не задан — берём все устройства пользователя
	var devices []*domain.DeviceKeys
	var err error
	if deviceID == 0 {
		devices, err = s.deviceRepo.GetAllDevices(ctx, userID)
	} else {
		var dk *domain.DeviceKeys
		dk, err = s.deviceRepo.Get(ctx, userID, deviceID)
		if dk != nil {
			devices = []*domain.DeviceKeys{dk}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("get devices: %w", err)
	}

	bundles := make([]*pb.PreKeyBundle, 0, len(devices))
	for _, dk := range devices {
		bundle := &pb.PreKeyBundle{
			UserId:         dk.UserID,
			DeviceId:       uint32(dk.DeviceID),
			RegistrationId: dk.RegistrationID,
			IdentityKey:    dk.IdentityKey,
			SignedPreKey: &pb.SignedPreKey{
				KeyId:     dk.SignedPreKeyID,
				PublicKey: dk.SignedPreKeyPublic,
				Signature: dk.SignedPreKeySignature,
			},
		}

		otp, err := s.otpRepo.Acquire(ctx, dk.UserID, dk.DeviceID)
		if err == nil {
			bundle.OneTimePreKey = &pb.OneTimePreKey{
				KeyId:     otp.KeyID,
				PublicKey: otp.PublicKey,
			}
		}

		kyber, err := s.kyberRepo.Acquire(ctx, dk.UserID, dk.DeviceID)
		if err == nil {
			bundle.KyberPreKey = &pb.KyberPreKey{
				KeyId:        kyber.KeyID,
				PublicKey:    kyber.PublicKey,
				Signature:    kyber.Signature,
				IsLastResort: kyber.IsLastResort,
			}
		}

		bundles = append(bundles, bundle)
	}

	return &pb.PreKeyBundles{Bundles: bundles}, nil
}

func (s *E2EEMessenger) SendMessage(ctx context.Context, req *pb.EncryptedMessage) (*pb.Empty, error) {
	msg := &domain.EncryptedMessage{
		MessageID:         req.MessageId,
		SenderID:          req.SenderId,
		SenderDeviceID:    int(req.SenderDeviceId),
		RecipientID:       req.RecipientId,
		RecipientDeviceID: int(req.RecipientDeviceId),
		Ciphertext:        req.Ciphertext,
		MessageType:       int(req.MessageType),
		PreKeyID:          req.PreKeyId,
		SignedPreKeyID:    req.SignedPreKeyId,
		KyberPreKeyID:     req.KyberPreKeyId,
		Timestamp:         int64(req.Timestamp),
	}

	// 1. Durability: сохраняем в Postgres
	if err := s.msgRepo.Save(ctx, msg); err != nil {
		return nil, fmt.Errorf("save message: %w", err)
	}

	// 2. Real-time: публикуем в Redis
	channel := fmt.Sprintf("msg:%s:%d", req.RecipientId, req.RecipientDeviceId)
	payload, err := proto.Marshal(req)
	if err != nil {
		log.Printf("marshal error: %v", err)
	} else if err := s.redis.Publish(ctx, channel, payload); err != nil {
		log.Printf("redis publish error: %v", err)
	}

	// 3. Локальный fallback: если получатель на этом же сервере
	s.mu.RLock()
	stream, ok := s.streams[channel]
	s.mu.RUnlock()
	if ok {
		// Не блокируем ответ отправителю
		go func() {
			if err := stream.Send(req); err != nil {
				log.Printf("local stream send error: %v", err)
			}
		}()
	}

	return &pb.Empty{}, nil
}

func (s *E2EEMessenger) StreamMessages(req *pb.UserId, stream pb.Messenger_StreamMessagesServer) error {
	ctx := stream.Context()
	userID := req.UserId
	deviceID := int(req.GetDeviceId())
	channel := fmt.Sprintf("msg:%s:%d", userID, deviceID)

	// 1. Регистрируем локальный стрим
	s.mu.Lock()
	s.streams[channel] = stream
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.streams, channel)
		s.mu.Unlock()
	}()

	// 2. Отправляем накопленные из Postgres
	msgs, err := s.msgRepo.GetUndelivered(ctx, userID, deviceID)
	if err != nil {
		log.Printf("get undelivered: %v", err)
	} else {
		for _, m := range msgs {
			pbMsg := protoFromDomain(m)
			if err := stream.Send(pbMsg); err != nil {
				return fmt.Errorf("send backlog: %w", err)
			}
		}
	}

	// 3. Подписываемся на Redis
	sub := s.redis.Subscribe(ctx, channel)
	defer sub.Close()

	redisCh := sub.Channel()

	for {
		select {
		case redisMsg := <-redisCh:
			var pbMsg pb.EncryptedMessage
			if err := proto.Unmarshal([]byte(redisMsg.Payload), &pbMsg); err != nil {
				log.Printf("unmarshal error: %v", err)
				continue
			}
			if err := stream.Send(&pbMsg); err != nil {
				return fmt.Errorf("send redis msg: %w", err)
			}

			// Удаляем из Postgres после успешной отправки в стрим
			if err := s.msgRepo.Delete(ctx, pbMsg.MessageId); err != nil {
				log.Printf("delete delivered msg: %v", err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Helpers

func protoFromDomain(m *domain.EncryptedMessage) *pb.EncryptedMessage {
	return &pb.EncryptedMessage{
		MessageId:         m.MessageID,
		SenderId:          m.SenderID,
		SenderDeviceId:    uint32(m.SenderDeviceID),
		RecipientId:       m.RecipientID,
		RecipientDeviceId: uint32(m.RecipientDeviceID),
		Ciphertext:        m.Ciphertext,
		MessageType:       pb.MessageType(m.MessageType),
		PreKeyId:          m.PreKeyID,
		SignedPreKeyId:    m.SignedPreKeyID,
		KyberPreKeyId:     m.KyberPreKeyID,
		Timestamp:         uint64(m.Timestamp),
	}
}
