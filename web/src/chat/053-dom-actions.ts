// Bind only the trusted static controls present at startup. Dynamic buttons
// register their own function listeners when rendered; data attributes are never evaluated.
((): void => {
  const actions: Record<string, () => unknown> = {
    'openProfileModal': () => { openProfileModal(); },
    'toggleNotifPanel': () => { toggleNotifPanel(); },
    'clearAllNotifications': () => { clearAllNotifications(); },
    'openCreateGroupModal': () => { openCreateGroupModal(); },
    'openFriendModal': () => { openFriendModal(); },
    'openGroupSettingsModal': () => { openGroupSettingsModal(); },
    'toggleInfoPanel': () => { toggleInfoPanel(); },
    'initiateCall-audio': () => { initiateCall('audio'); },
    'closeInfoPanel': () => { closeInfoPanel(); },
    'closeUpdateShowcaseModal': () => { closeUpdateShowcaseModal(); },
    'closeFriendModal': () => { closeFriendModal(); },
    'closeCreateGroupModal': () => { closeCreateGroupModal(); },
    'submitCreateGroup': () => { submitCreateGroup(); },
    'closeProfileModal': () => { closeProfileModal(); },
    'openSettingsFromProfile': () => { closeProfileModal();openSettingsModal(); },
    'saveProfile': () => { saveProfile(); },
    'closeGroupSettingsModal': () => { closeGroupSettingsModal(); },
    'switchGroupSettingsTab-main': () => { switchGroupSettingsTab('main'); },
    'switchGroupSettingsTab-members': () => { switchGroupSettingsTab('members'); },
    'switchGroupSettingsTab-permissions': () => { switchGroupSettingsTab('permissions'); },
    'switchGroupSettingsTab-invites': () => { switchGroupSettingsTab('invites'); },
    'switchGroupSettingsTab-requests': () => { switchGroupSettingsTab('requests'); },
    'switchGroupSettingsTab-audit': () => { switchGroupSettingsTab('audit'); },
    'switchGroupSettingsTab-danger': () => { switchGroupSettingsTab('danger'); },
    'saveGroupSettings': () => { saveGroupSettings(); },
    'toggleAdminsOnlyWrite': () => { toggleAdminsOnlyWrite(); },
    'toggleHideMembersList': () => { toggleHideMembersList(); },
    'toggleAnonymousMode': () => { toggleAnonymousMode(); },
    'createGroupInviteLink': () => { createGroupInviteLink(); },
    'deleteCurrentGroup': () => { deleteCurrentGroup(); },
    'closeUserProfileModal': () => { closeUserProfileModal(); },
    'closeSettingsModal': () => { closeSettingsModal(); },
    'openProfileFromSettings': () => { closeSettingsModal();openProfileModal(); },
    'switchSettingsTab-account': () => { switchSettingsTab('account'); },
    'switchSettingsTab-notifications': () => { switchSettingsTab('notifications'); },
    'switchSettingsTab-microphone': () => { switchSettingsTab('microphone'); },
    'switchSettingsTab-screen': () => { switchSettingsTab('screen'); },
    'switchSettingsTab-privacy': () => { switchSettingsTab('privacy'); },
    'switchSettingsTab-security': () => { switchSettingsTab('security'); },
    'switchSettingsTab-sessions': () => { switchSettingsTab('sessions'); },
    'saveSettingsPreferences-notifications': () => { saveSettingsPreferences('notifications'); },
    'toggleMicrophonePttKeyCapture': () => { toggleMicrophonePttKeyCapture(); },
    'clearMicrophonePttKey': () => { clearMicrophonePttKey(); },
    'saveSettingsPreferences-microphone': () => { saveSettingsPreferences('microphone'); },
    'resetDesktopMicrophonePermission': () => { void resetDesktopMicrophonePermission(); },
    'saveSettingsPreferences-screen': () => { saveSettingsPreferences('screen'); },
    'saveSettingsPreferences-privacy': () => { saveSettingsPreferences('privacy'); },
    'changePassword': () => { changePassword(); },
    'logoutCurrentSession': () => { logoutCurrentSession(); },
    'logoutAllSessions': () => { logoutAllSessions(); },
    'startEditMessage': () => { startEditMessage(); },
    'deleteMessage': () => { deleteMessage(); },
    'openChatContextProfile': () => { openChatContextProfile(); },
    'toggleChatContextPinned': () => { toggleChatContextPinned(); },
    'toggleChatContextMuted': () => { toggleChatContextMuted(); },
    'acceptIncomingCall': () => { acceptIncomingCall(); },
    'rejectIncomingCall': () => { rejectIncomingCall(); },
    'toggleCallDockDetails': () => { toggleCallDockDetails(); },
    'toggleCallMic': () => { toggleCallMic(); },
    'toggleScreenSharePanel': () => { toggleScreenSharePanel(); },
    'endCall': () => { endCall(); },
    'closeScreenSharePanel': () => { closeScreenSharePanel(); },
    'submitScreenSharePanel': () => { submitScreenSharePanel(); },
  };
  for (const type of ["click", "change"] as const) {
    queryAll<HTMLElement>(`[data-ui-${type}]`).forEach(element => {
      const name = element.getAttribute(`data-ui-${type}`) || "";
      const action = Object.prototype.hasOwnProperty.call(actions, name) ? actions[name] : null;
      if (!action) throw new Error("Unknown static UI action");
      element.addEventListener(type, event => {
        if (element.dataset.uiSelf === "true" && event.target !== element) return;
        void action();
      });
    });
  }
})();
