declare const LivekitClient: any;

interface DesktopImageResult {
  file: File;
  mime: string;
  width: number;
  height: number;
  changed: boolean;
  reason: "compressed" | "original_smaller" | "animation" | "multipage";
}

interface DesktopE2eeStatus { context: number; phase: string; error?: string | null; runtimePhase?: string | null; incomingMessageRevision?: string | null; replacement?: { expectedGeneration: string; continuing: boolean; retargeting: boolean; } | null; }
interface DesktopE2eeText { conversationId: string; messageId: string; text: string; }
interface DesktopE2eeTextReply { context: number; queued: { conversationId: string; messageId: string; cryptoEpoch: string; createdAtUnixMs: string; storedOnServer: boolean; sealedUnsent: boolean; }; }
interface DesktopAttachmentTarget { readonly conversationId: string; readonly messageId: string; }
type DesktopPhotoChoice = { readonly mode: "clean"; readonly quality: number | null; readonly maxDimension: number; } | { readonly mode: "original"; readonly metadataWarningAcknowledged: true; };
interface DesktopE2eePhoto extends DesktopAttachmentTarget { readonly file: File; readonly caption: string; readonly choice: DesktopPhotoChoice; }
interface DesktopE2eeFile extends DesktopAttachmentTarget { readonly file: File; readonly caption: string; readonly expectedEpoch: string; }
type DesktopAttachmentInput = DesktopAttachmentTarget & ({ readonly kind: "vacant"; } | { readonly kind: "prepared"; readonly cryptoEpoch: string; } | { readonly kind: "outgoing"; readonly cryptoEpoch: string; readonly storedOnServer: boolean; readonly sealedUnsent: boolean; });
interface DesktopAttachmentReply { readonly context: number; readonly attachment: DesktopAttachmentInput; }
type DesktopMessageState = "pending" | "recipient_stored" | "recipient_rejected" | "expired" | "recipient_replaced";
interface DesktopDelivery { storedOnServer: boolean; state: DesktopMessageState | null; removal: "expired" | "recipient_replaced" | null; sealedUnsent: boolean; }
interface DesktopAttachmentMetadata {
  readonly kind: "file" | "photo" | "voice";
  readonly filename: string;
  readonly caption: string;
  readonly mimeType: string;
  readonly size: number;
  readonly photoMetadataStripped: boolean | null;
}
interface DesktopLocalAttachmentTarget extends DesktopAttachmentTarget { readonly generationId: string | null; }
interface DesktopLocalAttachmentReply<T> { readonly context: number; readonly target: DesktopLocalAttachmentTarget; readonly result: T; }
interface DesktopHistoryItem { messageId: string; conversationId: string; cryptoEpoch: string; createdAtUnixMs: string; senderUserId: string; text: string; outgoing: boolean; delivery: DesktopDelivery | null; readonly attachment?: DesktopAttachmentMetadata; cacheState?: "missing" | "saved" | "removed"; }
interface DesktopPendingAttachment { readonly messageId: string; readonly cryptoEpoch: string; readonly createdAtUnixMs: string; readonly attachment: DesktopAttachmentMetadata; }
interface DesktopPendingAttachmentsReply {
  readonly context: number;
  readonly page: { readonly kind: "pending_attachments"; readonly conversationId: string; readonly items: ReadonlyArray<DesktopPendingAttachment>; readonly nextCursor: string | null; };
}
type DesktopRecoveryDecision = "accept" | "decline" | "cancel";
interface DesktopRecoveryProposal { proposalId: string; state: "pending" | "accepted" | "declined" | "cancelled" | "expired" | "superseded" | null; ownProposal: boolean; decision: DesktopRecoveryDecision | null; sendingFrozen: boolean; }
interface DesktopRecoveryState { kind: "state"; conversationId: string; cryptoEpoch: string | null; receiveOnly: boolean; sendingFrozen: boolean; proposals: DesktopRecoveryProposal[]; }
interface DesktopRecoveryQueued { kind: "queued"; conversationId: string; cryptoEpoch: string; operationId: string; }
interface DesktopRecoveryReply<T> { context: number; recovery: T; }
interface DesktopRecoveryProposalIntent { conversationId: string; expectedEpoch: string; expectedOwnProposalId: string | null; }
interface DesktopRecoveryDecisionIntent { conversationId: string; expectedEpoch: string; proposalId: string; decision: DesktopRecoveryDecision; }
interface DesktopReadReply<T, K extends string> { context: number; page: { kind: K; items: T[]; nextCursor: string | null; }; }

interface DesktopE2eeConversationReply {
  context: number;
  conversation: { conversationId: string; peerUserId: string; state: "active" | "closed"; closeReason: "device_replaced" | null; closedAtUnixMs: string; };
}
// Local archive metadata describes saved state, not current send permission.
interface DesktopArchiveGeneration { generationId: string; replacedByGenerationId: string | null; historyReady: boolean; }
interface DesktopArchiveSegment { cryptoEpoch: string; sealed: boolean; createdAtUnixMs: string; sealedAtUnixMs: string | null; }
interface DesktopArchiveGenerationTarget { readonly generationId: string; }
interface DesktopArchiveConversationTarget extends DesktopArchiveGenerationTarget { readonly conversationId: string; }
interface DesktopArchiveHistoryTarget extends DesktopArchiveConversationTarget { readonly cryptoEpoch: string | null; }
interface DesktopArchiveReadReply<T, K extends string, S = {}> {
  readonly context: number;
  readonly page: { readonly kind: K; readonly items: ReadonlyArray<Readonly<T>>; readonly nextCursor: string | null; } & S;
}
type DesktopArchivesReply = DesktopArchiveReadReply<DesktopArchiveGeneration, "archives">;
type DesktopArchiveConversationsReply = DesktopArchiveReadReply<DesktopE2eeConversationReply["conversation"], "archive_dialogs", DesktopArchiveGenerationTarget>;
type DesktopArchiveSegmentsReply = DesktopArchiveReadReply<DesktopArchiveSegment, "segments", DesktopArchiveConversationTarget>;
type DesktopArchiveHistoryReply = DesktopArchiveReadReply<DesktopHistoryItem, "archive_history", DesktopArchiveHistoryTarget>;
interface DesktopE2eeApi {
  readonly compiled: boolean;
  preparePhoto(message: DesktopE2eePhoto, expectedContext: number): Promise<DesktopAttachmentReply>;
  prepareFile(message: DesktopE2eeFile, expectedContext: number, signal?: AbortSignal): Promise<DesktopAttachmentReply>;
  prepareVoice(message: DesktopE2eeFile, expectedContext: number, signal?: AbortSignal): Promise<DesktopAttachmentReply>;
  attachmentInput(target: DesktopAttachmentTarget, expectedContext: number, signal?: AbortSignal): Promise<DesktopAttachmentReply>;
  downloadAttachment(target: DesktopAttachmentTarget, expectedContext: number, signal?: AbortSignal): Promise<{ readonly context: number; readonly attachment: DesktopAttachmentTarget; }>;
  previewAttachment(target: DesktopLocalAttachmentTarget, expectedContext: number, signal?: AbortSignal): Promise<DesktopLocalAttachmentReply<{ readonly kind: "preview"; readonly png: string; }>>;
  listenAttachment(target: DesktopLocalAttachmentTarget, expectedContext: number, signal?: AbortSignal): Promise<DesktopLocalAttachmentReply<{ readonly kind: "voice"; readonly mimeType: "audio/ogg" | "audio/webm"; }> & { readonly audio: Uint8Array<ArrayBuffer>; }>;
  exportAttachment(target: DesktopLocalAttachmentTarget, expectedContext: number, suggestedName: string, signal?: AbortSignal): Promise<DesktopLocalAttachmentReply<{ readonly kind: "exported" | "cancelled"; }>>;
  pendingAttachments(conversationId: string, expectedContext: number, cursor?: string | null): Promise<DesktopPendingAttachmentsReply>;
  listArchives(expectedContext: number, cursor?: string | null): Promise<DesktopArchivesReply>;
  archiveConversations(target: DesktopArchiveGenerationTarget, expectedContext: number, cursor?: string | null): Promise<DesktopArchiveConversationsReply>;
  archiveSegments(target: DesktopArchiveConversationTarget, expectedContext: number, cursor?: string | null): Promise<DesktopArchiveSegmentsReply>;
  archiveHistory(target: DesktopArchiveHistoryTarget, expectedContext: number, cursor?: string | null): Promise<DesktopArchiveHistoryReply>;
  recoveryStatus(conversationId: string): Promise<DesktopRecoveryReply<DesktopRecoveryState>>;
  proposeRecovery(intent: DesktopRecoveryProposalIntent, expectedContext: number, confirmed: boolean): Promise<DesktopRecoveryReply<DesktopRecoveryQueued>>;
  decideRecovery(intent: DesktopRecoveryDecisionIntent, expectedContext: number, confirmed: boolean): Promise<DesktopRecoveryReply<DesktopRecoveryQueued>>;
  listConversations(cursor?: string | null): Promise<DesktopReadReply<DesktopE2eeConversationReply["conversation"], "dialogs">>;
  history(conversationId: string, cursor?: string | null): Promise<DesktopReadReply<DesktopHistoryItem, "history">>;
  messageStatuses(conversationId: string, messageIds: string[]): Promise<DesktopReadReply<{ messageId: string; cryptoEpoch: string; delivery: DesktopDelivery; }, "statuses">>;
  sendText(message: DesktopE2eeText, expectedContext: number): Promise<DesktopE2eeTextReply>;
  createConversation(peerUserId: string): Promise<DesktopE2eeConversationReply>;
  createConversationAfterClosed(peerUserId: string, closedConversationId: string, confirmed: boolean): Promise<DesktopE2eeConversationReply>;
  getConversation(conversationId: string): Promise<DesktopE2eeConversationReply>;
  status(): Promise<DesktopE2eeStatus>;
  setupDevice(): Promise<DesktopE2eeStatus>;
  retry(): Promise<DesktopE2eeStatus>;
  replaceDevice(password: string, confirmed: boolean): Promise<DesktopE2eeStatus>;
  endSession(): Promise<void>;
}
interface Window {
  zchatDesktop?: { e2ee?: DesktopE2eeApi; isDesktop: boolean; serverUrl: string; callGatewayURL: string; resetMicrophonePermission(): Promise<void>; prepareImage(file: File, quality: number, maxDimension: number): Promise<DesktopImageResult>; isApplicationURL(value: string): boolean; resourceURL(value: string): string; };
  AudioContext: typeof AudioContext;
  webkitAudioContext?: typeof AudioContext;
  switchChat?: (...args: any[]) => any;
  handleIncomingMessage?: (...args: any[]) => any;
  openLightbox?: (url: string) => void;
}
