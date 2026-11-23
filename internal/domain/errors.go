package domain

import "errors"

var (
	// User errors
	ErrUserNotFound  = errors.New("user not found")
	ErrEmailConflict = errors.New("email already taken")

	// Validation errors
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrInvalidInput       = errors.New("invalid input data")
	ErrPasswordTooShort   = errors.New("password is too short")
	ErrInvalidSession     = errors.New("invalid or revoked session")

	// Message errors
	ErrMessageNotFound   = errors.New("message not found")
	ErrMessageIDConflict = errors.New("client message id already used for another message")

	// Friend request errors
	ErrFriendRequestsDisabled  = errors.New("user does not accept friend requests")
	ErrDirectMessagesDisabled  = errors.New("new private chats are disabled by a participant")
	ErrFriendRequestExists     = errors.New("friend request already exists")
	ErrAlreadyFriends          = errors.New("users are already friends")
	ErrSelfFriendRequest       = errors.New("cannot send friend request to yourself")
	ErrFriendRequestNotPending = errors.New("friend request is not pending")
	ErrFriendRequestNotFound   = errors.New("friend request not found")

	// Chat errors
	ErrChatNotFound          = errors.New("chat not found")
	ErrChatWriteForbidden    = errors.New("only administrators can write in this chat")
	ErrNotChatMember         = errors.New("user is not a member of this chat")
	ErrNotFriends            = errors.New("users are not friends")
	ErrPrivateChatExists     = errors.New("private chat already exists")
	ErrNotGroupOwner         = errors.New("user is not group owner")
	ErrJoinRequestNotPending = errors.New("join request is not pending")
	ErrJoinRequestNotFound   = errors.New("join request not found")
	ErrGroupInviteNotFound   = errors.New("group invite not found")

	// Encryption errors
	ErrPreKeyPoolExhausted = errors.New("prekey pool exhausted")
	ErrDeviceNotFound      = errors.New("device not found")
)
