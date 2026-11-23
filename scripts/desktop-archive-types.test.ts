// Compile-time public API examples. This file is not bundled into the app.
declare const archiveApi: DesktopE2eeApi;
declare const observedContext: number;
const generation = { generationId: '33333333-3333-4333-8333-333333333333' };
const conversation = { ...generation, conversationId: '11111111-1111-4111-8111-111111111111' };
const historyTarget = { ...conversation, cryptoEpoch: null };
const allArchives: Promise<DesktopArchivesReply> = archiveApi.listArchives(observedContext);
const dialogsInArchive: Promise<DesktopArchiveConversationsReply> = archiveApi.archiveConversations(generation, observedContext);
const segmentsInArchive: Promise<DesktopArchiveSegmentsReply> = archiveApi.archiveSegments(conversation, observedContext);
const textInArchive: Promise<DesktopArchiveHistoryReply> = archiveApi.archiveHistory(historyTarget, observedContext);
archiveApi.archiveHistory({ ...conversation, cryptoEpoch: '9007199254740993' }, observedContext, null);
// @ts-expect-error A caller must bind its read to an observed context.
archiveApi.listArchives();
// @ts-expect-error A stale view cannot silently use the current context.
archiveApi.archiveHistory(historyTarget);
// @ts-expect-error All-epoch selection is an explicit null, not an omitted field.
archiveApi.archiveHistory(conversation, observedContext);
// @ts-expect-error Native u64/i64 values use decimal strings, not JS floats.
archiveApi.archiveHistory({ ...conversation, cryptoEpoch: 9007199254740993 }, observedContext);
// @ts-expect-error Renderer targets cannot select another local account or path.
archiveApi.archiveConversations({ ...generation, accountId: 'other' }, observedContext);
// @ts-expect-error Renderer targets contain no cryptographic scope.
archiveApi.archiveSegments({ ...conversation, scope: 'private' }, observedContext);
async function consumeArchive() {
  const reply = await textInArchive;
  const kind: 'archive_history' = reply.page.kind;
  const epoch: string | null = reply.page.cryptoEpoch;
  const message: Readonly<DesktopHistoryItem> | undefined = reply.page.items[0];
  // @ts-expect-error An archive page cannot be confused with the active composer response.
  const activeKind: 'history' = reply.page.kind;
  // @ts-expect-error Archive items are a bounded read-only result, not a mutable outbox.
  reply.page.items.push({});
  // @ts-expect-error Cryptographic keys do not exist in the public response.
  const key = reply.page.identityKey;
  // @ts-expect-error Signal state and ACK proofs remain native.
  const ratchet = reply.page.items[0].ratchet;
  return { kind, epoch, message };
}
