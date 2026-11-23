package domain

type UserSettings struct {
	NotificationsEnabled bool `json:"notifications_enabled"`
	NotificationSound    bool `json:"notification_sound"`
	DesktopNotifications bool `json:"desktop_notifications"`
	DNDEnabled           bool `json:"dnd_enabled"`
	DNDFromHour          int  `json:"dnd_from_hour"`
	DNDToHour            int  `json:"dnd_to_hour"`

	AllowFriendRequests bool   `json:"allow_friend_requests"`
	AllowDirectMessages bool   `json:"allow_direct_messages"`
	ShowOnlineStatus    bool   `json:"show_online_status"`
	ShowEmail           bool   `json:"show_email"`
	MicrophoneMode      string `json:"microphone_mode"`
	MicrophonePTTKey    string `json:"microphone_ptt_key"`
	ScreenShareMode     string `json:"screen_share_resolution"`
	ScreenShareFPS      int    `json:"screen_share_fps"`

	// Nil on a settings write from a client without the audience selector.
	AllowDirectMessagesFromNonFriends *bool `json:"allow_direct_messages_from_non_friends,omitempty"`
}
