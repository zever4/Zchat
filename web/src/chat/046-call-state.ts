// --- Call state ---
let callState: CallStateValue = "idle";
let callRoom: any = null;
let callType: CallMediaType | null = null;
let callChatID: number | null = null;
let callDurationInterval: number | null = null;
let callStartTime: number | null = null;
let incomingCallData: IncomingCallMessage | null = null;
let localTracks: { mic: boolean; camera: boolean; screen: boolean } = { mic: true, camera: false, screen: false };
let callParticipantDirectory: Record<string, any> = {};
let callParticipantVolumes: Record<string, number> = {};
let callParticipantAudioElements: CallParticipantAudioElementMap = {};
let activeCallParticipantMenuIdentity: string | null = null;
let callCleanupInProgress = false;
let callAttempt = 0;
let callJoinRequest: AbortController | null = null;
let callPeerLeftTimer: number | null = null;
let callRecoveryTimer: number | null = null;
let callInviteTimer: number | null = null;
// UI deduplication only; the server authorizes the sender of every signal.
const callUserID = accountStorageKey("call").slice("call:".length);
let callMicHoldActive = false;
let callMicPointerActive = false;
let callMicKeyboardActive = false;
let callAudioCtx: AudioContext | null = null;
let callSpeakingIdentities: Set<string> = new Set();
let callScreenShareDraft: { resolution: ScreenShareResolution; fps: number } = {
  resolution: DEFAULT_USER_SETTINGS.screen_share_resolution,
  fps: DEFAULT_USER_SETTINGS.screen_share_fps,
};
let callDockExpanded = false;
