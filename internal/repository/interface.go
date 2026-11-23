package repository

import (
	"context"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type UserRepository interface {
	Create(ctx context.Context, user *domain.User) (int64, error)
	RegisterWithSession(ctx context.Context, user *domain.User, sessionID string, expires time.Time) (int64, error)
	GetByEmail(ctx context.Context, email string) (*domain.User, error)
	GetByID(ctx context.Context, id int64) (*domain.User, error)
	SearchByUsername(ctx context.Context, query string, excludeUserID int64, limit int) ([]*domain.User, error)
	UpdateProfile(ctx context.Context, userID int64, bio string, avatarURL string) error
	UpdateProfileBio(ctx context.Context, userID int64, bio string) error
	ReplaceAvatar(ctx context.Context, userID int64, avatarURL string) (string, error)
	GetAllUsers(ctx context.Context) ([]*domain.User, error)
	CreateSession(ctx context.Context, userID int64, passwordHash string, version int, sessionID string, expires time.Time) error
	GetSessionVersion(ctx context.Context, userID int64, sessionID string) (int, error)
	DeleteSession(ctx context.Context, userID int64, sessionID string) error
	RevokeAllSessions(ctx context.Context, userID int64, version int) error
	ChangePasswordAndRevoke(ctx context.Context, userID int64, version int, oldHash, newHash string) error
	GetTokenVersion(ctx context.Context, userID int64) (int, error)
	GetSettings(ctx context.Context, userID int64) (*domain.UserSettings, error)
	UpsertSettings(ctx context.Context, userID int64, settings *domain.UserSettings) error
}

type FriendRequestRepository interface {
	Create(ctx context.Context, req *domain.FriendRequest) (int64, error)
	GetByID(ctx context.Context, id int64) (*domain.FriendRequest, error)
	UpdateStatus(ctx context.Context, id int64, status string) error
	ReopenRequest(ctx context.Context, id, from, to int64) error
	GetPendingForUser(ctx context.Context, userID int64) ([]*domain.FriendRequest, error)
	AreFriends(ctx context.Context, userID1, userID2 int64) (bool, error)
	GetFriends(ctx context.Context, userID int64) ([]*domain.User, error)
	GetBetweenUsers(ctx context.Context, userID1, userID2 int64) (*domain.FriendRequest, error)
}

type ChatRepository interface {
	CreateWithMembers(ctx context.Context, chatType string, userID1, userID2 int64) (*domain.Chat, error)
	CreateGroup(ctx context.Context, ownerID int64, name string, isPublic bool) (*domain.Chat, error)
	GetUserChats(ctx context.Context, userID int64) ([]*domain.Chat, error)
	GetPrivateChatBetween(ctx context.Context, userID1, userID2 int64) (*domain.Chat, error)
	GetChatByID(ctx context.Context, chatID int64) (*domain.Chat, error)
	GetChatByInviteCode(ctx context.Context, inviteCode string) (*domain.Chat, error)
	IsMember(ctx context.Context, chatID, userID int64) (bool, error)
	IsGroupOwner(ctx context.Context, chatID, userID int64) (bool, error)
	AddMember(ctx context.Context, chatID, userID int64) error
	UpdateGroupSettings(ctx context.Context, chatID, ownerID int64, name string, isPublic bool) error
	UpdateGroupAvatar(ctx context.Context, chatID, ownerID int64, avatarURL string) error
	ReplaceGroupAvatar(ctx context.Context, chatID, actor int64, avatarURL string) (string, error)
	RegenerateGroupInviteCode(ctx context.Context, chatID, ownerID int64) (string, error)
	CreateGroupInviteLink(ctx context.Context, chatID, ownerID int64) (*domain.GroupInviteLink, error)
	ListGroupInviteLinks(ctx context.Context, chatID int64) ([]*domain.GroupInviteLink, error)
	SetGroupInviteActive(ctx context.Context, chatID, inviteID, ownerID int64, isActive bool) error
	DeleteGroupInviteLink(ctx context.Context, chatID, inviteID, ownerID int64) error
	DeleteGroup(ctx context.Context, chatID, ownerID int64) error
	DecideGroupJoinRequest(ctx context.Context, chatID, actorUserID, targetUserID int64, approve bool) error
	JoinGroupByInvite(ctx context.Context, code string, userID int64) (*domain.Chat, string, error)
	CreateGroupJoinRequest(ctx context.Context, chatID, userID int64) (*domain.GroupJoinRequest, error)
	GetGroupJoinRequest(ctx context.Context, chatID, userID int64) (*domain.GroupJoinRequest, error)
	ListGroupJoinRequests(ctx context.Context, chatID int64) ([]*domain.GroupJoinRequest, error)
	UpdateGroupJoinRequestStatus(ctx context.Context, chatID, userID int64, status string) error
	AddGroupAuditLog(ctx context.Context, chatID, actorUserID int64, action string, detailsJSON string) error
	ListGroupAuditLogs(ctx context.Context, chatID int64, limit int) ([]*domain.GroupAuditLog, error)
	GetMemberIDs(ctx context.Context, chatID int64) ([]int64, error)
	GetMembersWithInfo(ctx context.Context, chatID int64) ([]*domain.User, error)
	GetMemberRole(ctx context.Context, chatID, userID int64) (string, error)
	IsGroupAdminOrOwner(ctx context.Context, chatID, userID int64) (bool, error)
	ChangeGroupMemberRole(ctx context.Context, chatID, actor, target int64, role string) error
	KickGroupMember(ctx context.Context, chatID, actor, target int64) error
	ChangeGroupPermissions(ctx context.Context, chatID, actor int64, adminsOnlyWrite, hideMembersList, anonymousMode *bool) error
	SetMemberRole(ctx context.Context, chatID, targetUserID int64, role string) error
	GetMembersWithRoles(ctx context.Context, chatID int64) ([]*domain.ChatMemberWithRole, error)
	GetAdminsOnlyWrite(ctx context.Context, chatID int64) (bool, error)
	SetAdminsOnlyWrite(ctx context.Context, chatID int64, value bool) error
	SetGroupPermissions(ctx context.Context, chatID int64, adminsOnlyWrite, hideMembersList, anonymousMode *bool) error
	RemoveMember(ctx context.Context, chatID, userID int64) error
}
