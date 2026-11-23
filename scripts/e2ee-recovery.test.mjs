import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const source = ['025-e2ee-view.ts', '025-e2ee-composer.ts', '025-e2ee-recovery.ts', '025-e2ee-discovery.ts'].map(compile).join('\n');
const flush = () => new Promise(resolve => setImmediate(resolve));
const id = n => '00000000-0000-4000-8000-' + n.toString(16).padStart(12, '0');
const CHAT = id(1), PEER = id(2);
const metadata = (conversationId = CHAT, state = 'active') => ({ conversationId, peerUserId: PEER, state, closeReason: state === 'closed' ? 'device_replaced' : null, closedAtUnixMs: state === 'closed' ? '1' : '0' });
const message = (n, outgoing = false) => ({ messageId: id(n + 100), conversationId: CHAT, cryptoEpoch: '0', createdAtUnixMs: String(n), senderUserId: PEER, text: 'message ' + n, outgoing, delivery: outgoing ? { storedOnServer: false, state: null, removal: null, sealedUnsent: false } : null });
class Element {
  children = []; dataset = {}; style = {}; hidden = false; disabled = false; readOnly = false; textContent = ''; value = ''; scrollTop = 0; clientHeight = 100; listeners = new Map(); attrs = {}; checked = false; open = false;
  classList = { contains: () => false, remove() {} };
  append(...items) { this.children.push(...items); }
  prepend(item) { this.children.unshift(item); }
  replaceChildren(...items) { this.children = [...items]; }
  removeChild(item) { this.children = this.children.filter(n => n !== item); }
  get lastElementChild() { return this.children.at(-1); }
  get scrollHeight() { return this.children.length * 20; }
  setAttribute(k, v) { this.attrs[k] = v; }
  addEventListener(name, fn) { const group = this.listeners.get(name) || []; group.push(fn); this.listeners.set(name, group); }
  async emit(name, value = {}) { for (const fn of this.listeners.get(name) || []) fn(value); await flush(); }
  async click() { if (!this.disabled && !this.hidden) await this.emit('click'); }
}
function deferred() { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; }
async function fixture(options = {}) {
  const nodes = new Map(), listeners = new Map(), timers = new Map(), calls = [], writes = [];
  const byId = name => { if (!nodes.has(name)) nodes.set(name, new Element()); return nodes.get(name); };
  const listen = (name, fn) => { const group = listeners.get(name) || []; group.push(fn); listeners.set(name, group); };
  let session = 'session', serial = 0, now = 0, uuid = 9000, active = 0, maxActive = 0;
  let state = { context: 2, phase: 'active', runtimePhase: 'Running' };
  let dialogs = options.dialogs || [metadata()], history = options.history || [];
  const call = async (kind, args, fn) => { calls.push([kind, ...args]); active++; maxActive = Math.max(maxActive, active); try { return await fn(); } finally { active--; } };
  const page = (kind, items, cursor, context = state.context) => { const offset = Number(cursor || 0); return { context, page: { kind, items: items.slice(offset, offset + 8), nextCursor: offset + 8 < items.length ? String(offset + 8) : null } }; };
  let root = options.root || { kind: 'state', conversationId: CHAT, cryptoEpoch: '0', receiveOnly: false, sendingFrozen: false, proposals: [] };
  let recoveryReads = 0, maxRecoveryReads = 0;
  const clone = value => JSON.parse(JSON.stringify(value));
  const commit = (action, intent) => {
    if (action === 'propose') { root = { ...root, sendingFrozen: true, proposals: [{ proposalId: id(500), state: null, ownProposal: true, decision: null, sendingFrozen: true }] }; }
    else { root = { ...root, sendingFrozen: intent.decision !== 'decline', proposals: root.proposals.map(p => p.proposalId === intent.proposalId ? { ...p, decision: intent.decision, sendingFrozen: intent.decision !== 'decline' } : p) }; }
    return { context: state.context, recovery: { kind: 'queued', conversationId: intent.conversationId, cryptoEpoch: intent.expectedEpoch, operationId: id(500) } };
  };
  const mutate = async (action, intent, context, confirmed) => {
    calls.push([action, clone(intent), context, confirmed]);
    if (context !== state.context) throw 'stale';
    if (root.cryptoEpoch !== intent.expectedEpoch || action === 'propose' && (root.proposals.find(p => p.ownProposal)?.proposalId || null) !== intent.expectedOwnProposalId) throw 'conflict';
    return options.mutate ? options.mutate(action, intent, context, confirmed, () => commit(action, intent)) : commit(action, intent);
  };
  const api = { compiled: options.compiled !== false,
    recoveryStatus: chat => call('recovery', [chat], async () => {
      recoveryReads++; maxRecoveryReads = Math.max(maxRecoveryReads, recoveryReads);
      try { return options.recoveryRead ? await options.recoveryRead(chat, state, () => ({ context: state.context, recovery: clone({ ...root, conversationId: chat }) })) : { context: state.context, recovery: clone({ ...root, conversationId: chat }) }; }
      finally { recoveryReads--; }
    }),
    proposeRecovery: (intent, context, confirmed) => mutate('propose', intent, context, confirmed),
    decideRecovery: (intent, context, confirmed) => mutate('decide', intent, context, confirmed),
    status: () => call('status' , [], () => options.status ? options.status(state) : { ...state }),
    listConversations: cursor => call('list', [cursor], () => options.list ? options.list(cursor, state) : page('dialogs', dialogs, cursor)),
    history: (chat, cursor) => call('history', [chat, cursor], () => options.read ? options.read(chat, cursor, state) : page('history', history, cursor)),
    messageStatuses: (chat, ids) => call('statuses', [chat, [...ids]], () => options.statuses ? options.statuses(ids, state) : { context: state.context, page: { kind: 'statuses', items: ids.map(messageId => ({ messageId, cryptoEpoch: '0', delivery: { storedOnServer: true, state: 'recipient_stored', removal: 'expired', sealedUnsent: false } })), nextCursor: null } }),
    sendText: (text, expected) => call('send', [text, expected], () => { if (expected !== state.context) throw 'stale'; throw 'native_wait'; }),
    setupDevice() { throw Error('unexpected setup'); }, retry() { throw Error('unexpected owner retry'); }, getConversation() { throw Error('unexpected network lookup'); },
  };
  const ctx = { TextEncoder, AbortController, token: 'session', authExpiredHandled: false, currentChatID: 17, currentChatType: 'private', chatViewVersion: 1,
    setTimeout: (fn, delay) => { const n = ++serial; timers.set(n, { fn, time: now + delay, delay }); return n; }, clearTimeout: n => timers.delete(n),
    localStorage: { getItem: () => session, setItem: (...args) => writes.push(args) }, crypto: { randomUUID: () => id(++uuid) }, confirm: () => true,
    document: { visibilityState: 'visible', addEventListener: listen, querySelectorAll: () => [], createElement: () => new Element(), getElementById: byId }, byId,
    window: { zchatDesktop: options.browser ? undefined : { e2ee: api }, addEventListener: listen },
    apiFetch: (...args) => { calls.push(['http', ...args]); return options.friends ? options.friends(...args) : Promise.resolve({ ok: true, json: async () => [{ user_uuid: PEER, username: '<b>Alice</b>' }] }); },
    profilePeerUUID: value => typeof value === 'string' && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value),
    chatHistoryRequests: new Map(), clearTypingDisplay() {}, closeInfoPanel() {}, messagesEl: new Element(), chatNameEl: new Element(), chatStatusEl: new Element(), messageInput: new Element(),
  };
  byId('e2eeDialogsSection').hidden = true; byId('e2eeRecoveryPanel').hidden = true;
  runInNewContext(source + '\nthis.selected=()=>e2eeHistoryView; this.drafts=()=>[...e2eeDrafts.values()]; this.catalogue=()=>[...e2eeDialogs.values()];', ctx);
  const f = { ctx, nodes, byId, calls, timers, writes, page, api,
    async next() { const entry = [...timers.entries()].sort((a, b) => a[1].time - b[1].time)[0]; if (!entry) return false; timers.delete(entry[0]); now = entry[1].time; entry[1].fn(); await flush(); return true; },
    async emit(name, event = {}) { for (const fn of listeners.get(name) || []) fn(event); await flush(); },
    async visible(value) { ctx.document.visibilityState = value ? 'visible' : 'hidden'; await this.emit('visibilitychange'); },
    root: () => root, patch(value) { root = { ...root, ...clone(value) }; }, maxRecoveryReads: () => maxRecoveryReads,
    async check() { await ctx.refreshE2eeRecovery(ctx.selected(), () => true, true); },
    async prepare(action, proposal = null) { ctx.beginE2eeRecovery(action, proposal); await flush(); },
    async consent(value = true) { byId('e2eeRecoveryConfirm').checked = value; await byId('e2eeRecoveryConfirm').emit('change'); },
    async submit() { await byId('e2eeRecoverySubmit').click(); },
    setState(value) { state = { ...state, ...value }; }, setHistory(value) { history = value; }, setDialogs(value) { dialogs = value; },
    async open(chat = CHAT) { ctx.openE2eeConversation({ context: state.context, conversation: metadata(chat) }, 'Alice'); await flush(); },
    async type(text) { byId('e2eeTextInput').value = text; await byId('e2eeTextInput').emit('input'); },
    async account() { session = 'other'; await this.emit('storage', { key: 'token' }); }, maxActive: () => maxActive,
  };
  ctx.startE2eeDiscovery(); await flush(); if (!options.browser && options.compiled !== false) { await f.next(); await f.open(); }
  return f;
}
const kinds = (f, kind) => f.calls.filter(call => call[0] === kind);
const transcript = f => f.byId('e2eeHistory').children.map(row => row.children[1].textContent);

const proposal = (own = false, state = 'pending', decision = null) => ({ proposalId: id(500), state, ownProposal: own, decision, sendingFrozen: own || decision === 'accept' });
const actions = f => f.calls.filter(c => ['propose', 'decide'].includes(c[0]));
const panel = f => f.ctx.selected()?.recovery;
const current = f => ({ context: 2, recovery: JSON.parse(JSON.stringify(f.root())) });
const withPeer = () => ({ kind: 'state', conversationId: CHAT, cryptoEpoch: '0', receiveOnly: false, sendingFrozen: false, proposals: [proposal()] });

test('browser and default build have no recovery panel, reads, mutation or extra timer', async () => {
  for (const options of [{ browser: true }, { compiled: false }]) { const f = await fixture(options); assert.equal(f.byId('e2eeRecoveryPanel').hidden, true); assert.equal(kinds(f, 'recovery').length, 0); assert.equal(actions(f).length, 0); assert.equal(f.timers.size, 0); }
});
test('opening and polling recovery is readonly; receiving a peer request opens details without consent or mutation', async () => {
  const f = await fixture({ root: withPeer() }); assert.equal(f.byId('e2eeRecoveryPanel').open, true); assert.equal(panel(f).consent, null); assert.equal(actions(f).length, 0);
  for (let n = 0; n < 3; n++) await f.next(); assert.equal(actions(f).length, 0); assert.equal(kinds(f, 'http').length, 1); assert.equal(f.writes.length, 0);
});
test('known empty session cannot recover but first text can still be sent explicitly', async () => {
  const f = await fixture({ root: { ...withPeer(), cryptoEpoch: null, proposals: [] } }); await f.type('first text');
  assert.equal(f.byId('e2eeRecoveryPropose').disabled, true); assert.equal(f.byId('e2eeTextSend').disabled, false); await f.prepare('propose'); assert.equal(panel(f).consent, null);
  await f.ctx.sendE2eeText(); assert.equal(kinds(f, 'send').length, 1); assert.equal(actions(f).length, 0);
});
test('propose requires one-shot explicit consent and passes exactly captured context, epoch and null own reference', async () => {
  const f = await fixture(); await f.type('keep my draft'); await f.prepare('propose'); assert.match(f.byId('e2eeRecoveryPrompt').textContent, /приостановлена.*Старая история/s);
  await f.submit(); assert.equal(actions(f).length, 0); await f.consent(); await f.submit();
  const [action, intent, context, confirmed] = actions(f)[0]; assert.equal(action, 'propose'); assert.deepEqual(intent, { conversationId: CHAT, expectedEpoch: '0', expectedOwnProposalId: null }); assert.equal(context, 2); assert.equal(confirmed, true);
  assert.equal(f.byId('e2eeRecoveryConfirm').checked, false); assert.equal(f.byId('e2eeTextInput').value, 'keep my draft'); assert.equal(f.byId('e2eeTextSend').disabled, true); assert.equal(f.writes.length, 0);
});
test('all three decisions use the selected proposal and explicit confirmation, with no browser HTTP send', async () => {
  for (const decision of ['accept', 'decline', 'cancel']) {
    const own = decision === 'cancel'; const f = await fixture({ root: { ...withPeer(), sendingFrozen: own, proposals: [proposal(own)] } });
    await f.prepare(decision, id(500)); assert.ok(panel(f).consent); await f.submit(); assert.equal(actions(f).length, 0);
    await f.consent(); await f.submit(); assert.deepEqual(actions(f)[0].slice(1), [{ conversationId: CHAT, expectedEpoch: '0', proposalId: id(500), decision }, 2, true]);
    assert.equal(kinds(f, 'http').length, 1); assert.equal(f.writes.length, 0);
  }
});
test('wrong roles, missing proposals, terminal requests and duplicate competing propose never open a confirmation', async () => {
  const peer = await fixture({ root: withPeer() }); await peer.prepare('cancel', id(500)); assert.equal(panel(peer).consent, null); await peer.prepare('propose'); assert.equal(panel(peer).consent, null);
  const own = await fixture({ root: { ...withPeer(), sendingFrozen: true, proposals: [proposal(true)] } }); await own.prepare('accept', id(500)); assert.equal(panel(own).consent, null);
  for (const state of ['accepted', 'declined', 'cancelled', 'expired', 'superseded']) { const f = await fixture({ root: { ...withPeer(), proposals: [proposal(false, state)] } }); await f.prepare('accept', id(500)); assert.equal(panel(f).consent, null); assert.equal(actions(f).length, 0); }
});
test('a new explicit proposal after a terminal own request carries that exact reference', async () => {
  const f = await fixture({ root: { ...withPeer(), proposals: [proposal(true, 'declined')] } });
  await f.prepare('propose'); await f.consent(); await f.submit(); assert.equal(actions(f)[0][1].expectedOwnProposalId, id(500));
});
test('unchanged polling preserves confirmation; changed request or epoch resets it without retargeting', async () => {
  const f = await fixture(); await f.prepare('propose'); await f.consent(); const consent = panel(f).consent;
  await f.check(); assert.equal(panel(f).consent, consent); assert.equal(f.byId('e2eeRecoveryConfirm').checked, true);
  f.patch({ cryptoEpoch: '1' }); await f.check(); assert.equal(panel(f).consent, null); assert.equal(f.byId('e2eeRecoveryConfirm').checked, false); await f.submit(); assert.equal(actions(f).length, 0);
  const g = await fixture({ root: withPeer() }); await g.prepare('accept', id(500)); await g.consent(); g.patch({ proposals: [{ ...proposal(), proposalId: id(501) }] }); await g.check();
  await g.submit(); assert.equal(actions(g).length, 0); assert.equal(panel(g).consent, null);
});
test('a native epoch change not yet observed by UI conflicts on old captured fields and is never retried automatically', async () => {
  const f = await fixture(); await f.prepare('propose'); await f.consent(); f.patch({ cryptoEpoch: '1' }); await f.submit();
  assert.equal(actions(f).length, 1); assert.equal(actions(f)[0][1].expectedEpoch, '0'); assert.equal(panel(f).state.cryptoEpoch, '1'); assert.equal(panel(f).consent, null);
  await f.next(); assert.equal(actions(f).length, 1);
});
test('double click and cross-chat actions share one mutation lane while text and unknown UUID remain intact', async () => {
  const pending = deferred(); const f = await fixture({ mutate: () => pending.promise }); await f.type('uncertain message'); await f.ctx.sendE2eeText(); const draft = f.ctx.drafts()[0];
  await f.prepare('propose'); await f.consent(); await f.submit(); await f.ctx.submitE2eeRecovery(); assert.equal(actions(f).length, 1);
  assert.equal(f.ctx.drafts()[0].messageId, draft.messageId); assert.equal(f.byId('e2eeTextInput').value, draft.text);
  await f.open(id(9)); await f.prepare('propose'); assert.equal(panel(f).consent, null); assert.equal(actions(f).length, 1);
  pending.resolve({ context: 2, recovery: { kind: 'queued', conversationId: CHAT, cryptoEpoch: '0', operationId: id(500) } }); await flush();
  assert.equal(f.ctx.selected().reply.conversation.conversationId, id(9)); assert.equal(f.ctx.drafts()[0].messageId, draft.messageId);
});
test('unknown local result is resolved by readonly status without another mutation or replacing the text draft', async () => {
  const f = await fixture({ mutate: (action, intent, ctx, confirmed, commit) => { commit(); throw 'native_wait'; } }); await f.type('draft');
  await f.prepare('propose'); await f.consent(); await f.submit(); assert.equal(actions(f).length, 1); assert.equal(panel(f).state.proposals[0].proposalId, id(500)); assert.equal(panel(f).state.sendingFrozen, true);
  assert.equal(f.byId('e2eeTextInput').value, 'draft'); assert.equal(f.byId('e2eeTextSend').disabled, true); await f.next(); assert.equal(actions(f).length, 1);
});
test('queued and Accepted are not completed recovery; only a newer native epoch releases the observed recovery freeze', async () => {
  const f = await fixture({ root: withPeer() }); await f.type('preserve'); await f.prepare('accept', id(500)); await f.consent(); await f.submit();
  assert.equal(f.byId('e2eeTextSend').disabled, true); f.patch({ sendingFrozen: true, proposals: [{ ...proposal(false, 'accepted', 'accept'), sendingFrozen: true }] }); await f.check();
  assert.equal(f.byId('e2eeTextSend').disabled, true); assert.match(f.byId('e2eeRecoveryProposals').children[0].children[1].textContent, /Завершаем/);
  f.patch({ cryptoEpoch: '1', sendingFrozen: false, proposals: [] }); await f.check(); assert.equal(f.byId('e2eeTextSend').disabled, false); assert.equal(f.byId('e2eeTextInput').value, 'preserve'); assert.match(f.byId('e2eeRecoveryNotice').textContent, /Старая история сохранена/);
});
test('recovery state does not reopen history, move its scroll or clear an unknown message draft on epoch advance', async () => {
  const f = await fixture({ history: Array.from({ length: 16 }, (_, n) => message(16 - n)) }); await f.byId('e2eeHistoryMore').click(); f.byId('e2eeHistory').scrollTop = 3;
  await f.type('unknown send'); await f.ctx.sendE2eeText(); const draft = f.ctx.drafts()[0]; const view = f.ctx.selected(); const before = transcript(f);
  f.patch({ cryptoEpoch: '1' }); await f.check(); assert.equal(f.ctx.selected(), view); assert.deepEqual(transcript(f), before); assert.equal(f.byId('e2eeHistory').scrollTop, 3);
  assert.equal(f.ctx.drafts()[0].messageId, draft.messageId); assert.equal(f.byId('e2eeTextInput').readOnly, true);
});
test('closing, switching, auth expiry and token change discard stale consent and late mutations cannot rewrite a new selection', async () => {
  for (const action of ['account', 'close', 'switch']) {
    const f = await fixture(); await f.prepare('propose'); await f.consent();
    if (action === 'account') await f.account(); else if (action === 'close') await f.byId('e2eeViewClose').click(); else await f.open(id(9));
    await f.ctx.submitE2eeRecovery(); assert.equal(actions(f).length, 0); assert.equal(f.byId('e2eeRecoveryConfirm').checked, false);
  }
  const f = await fixture(); await f.prepare('propose'); await f.consent(); f.ctx.authExpiredHandled = true; await f.ctx.submitE2eeRecovery(); assert.equal(actions(f).length, 0);
});
test('closed metadata and receive-only native snapshots prevent recovery and new sends', async () => {
  const f = await fixture(); await f.type('keep'); await f.prepare('propose'); await f.consent();
  f.setDialogs([metadata(CHAT, 'closed')]); await f.next(); await f.submit(); assert.equal(actions(f).length, 0); assert.equal(f.byId('e2eeTextSend').disabled, true); assert.equal(f.byId('e2eeTextInput').value, 'keep');
  const g = await fixture(); g.patch({ receiveOnly: true }); await g.check(); await g.prepare('propose'); assert.equal(panel(g).consent, null); await g.type('x'); assert.equal(g.byId('e2eeTextSend').disabled, true);
});
test('visibility change clears confirmation and ignores hidden read replies until a fresh visible read', async () => {
  const pending = deferred(); let block = false; const f = await fixture({ recoveryRead: (chat, state, normal) => block ? pending.promise : normal() });
  await f.prepare('propose'); await f.consent(); block = true; const read = f.ctx.refreshE2eeRecovery(f.ctx.selected(), () => true, true); await flush();
  await f.visible(false); pending.resolve(current(f)); await read; assert.equal(panel(f).fresh, false); assert.equal(panel(f).consent, null); assert.equal(f.byId('e2eeRecoveryConfirm').checked, false);
  block = false; await f.visible(true); await f.next(); assert.equal(panel(f).fresh, true); assert.equal(actions(f).length, 0);
});
test('one recovery read lane survives repeated refreshes and selection changes without accepting an old view response', async () => {
  const pending = deferred(); let block = false; const f = await fixture({ recoveryRead: (chat, state, normal) => block ? pending.promise : normal() }); block = true;
  const read = f.ctx.refreshE2eeRecovery(f.ctx.selected(), () => true, true); await flush(); const count = kinds(f, 'recovery').length;
  await f.byId('e2eeRecoveryRefresh').click(); await f.open(id(9)); await f.byId('e2eeRecoveryRefresh').click(); assert.equal(kinds(f, 'recovery').length, count); assert.equal(f.maxRecoveryReads(), 1);
  pending.resolve(current(f)); await read; assert.equal(panel(f).state, null); block = false; await f.check(); assert.equal(panel(f).state.conversationId, id(9));
});
test('recovery read failures pause only sends/controls, retain history and use the common scheduler with bounded backoff', async () => {
  let fail = false; const f = await fixture({ history: [message(1)], recoveryRead: (chat, state, normal) => { if (fail) throw 'busy'; return normal(); } });
  await f.type('draft'); await f.prepare('propose'); await f.consent(); fail = true; await f.check();
  assert.equal(panel(f).fresh, false); assert.equal(panel(f).consent, null); assert.equal(f.byId('e2eeTextSend').disabled, true); assert.deepEqual(transcript(f), ['message 1']);
  const before = kinds(f, 'recovery').length; await f.next(); assert.equal(kinds(f, 'recovery').length, before); await f.next(); assert.equal(kinds(f, 'recovery').length, before + 1);
  fail = false; await f.check(); assert.equal(panel(f).fresh, true); assert.equal(f.byId('e2eeTextInput').value, 'draft'); assert.equal(f.timers.size, 1);
});
test('a pending text local acceptance prevents starting recovery; dismissing confirmation never cancels a native request', async () => {
  const f = await fixture(); await f.prepare('propose'); await f.consent(); await f.byId('e2eeRecoveryBack').click(); await f.submit(); assert.equal(actions(f).length, 0);
  await f.type('draft'); f.ctx.drafts()[0].busy = true; f.ctx.renderE2eeComposer(); await f.prepare('propose'); assert.equal(panel(f).consent, null); assert.equal(actions(f).length, 0);
});
test('epoch regression or context mismatch is rejected and cannot unlock the old consent', async () => {
  const f = await fixture({ root: { ...withPeer(), cryptoEpoch: '1', proposals: [] } }); await f.prepare('propose'); await f.consent(); f.patch({ cryptoEpoch: '0' }); await f.check(); assert.equal(panel(f).fresh, false); assert.ok(f.ctx.selected().suspended); await f.submit(); assert.equal(actions(f).length, 0);
  const g = await fixture(); await g.prepare('propose'); await g.consent(); g.setState({ context: 3 }); await g.next(); await g.submit(); assert.equal(actions(g).length, 0); assert.equal(g.byId('e2eeRecoveryConfirm').checked, false);
});
test('unchanged recovery reads retain actionable row identity and consent checkbox focus', async () => {
  const f = await fixture({ root: withPeer() }); const row = f.byId('e2eeRecoveryProposals').children[0]; const button = row.children[2].children[0];
  await f.prepare('accept', id(500)); await f.consent(); await f.check();
  assert.equal(f.byId('e2eeRecoveryProposals').children[0], row); assert.equal(row.children[2].children[0], button); assert.equal(f.byId('e2eeRecoveryConfirm').checked, true);
});
test('reopening the same chat during mutation waits for its result and fresh read without clearing the new draft', async () => {
  const pending = deferred(); const f = await fixture({ mutate: (action, intent, ctx, confirmed, commit) => { commit(); return pending.promise; } });
  await f.prepare('propose'); await f.consent(); await f.submit(); await f.open(); await f.type('new draft');
  assert.equal(panel(f).fresh, false); assert.equal(f.byId('e2eeTextSend').disabled, true);
  pending.resolve({ context: 2, recovery: { kind: 'queued', conversationId: CHAT, cryptoEpoch: '0', operationId: id(500) } }); await flush();
  assert.equal(panel(f).fresh, true); assert.equal(panel(f).state.sendingFrozen, true); assert.equal(f.byId('e2eeTextInput').value, 'new draft'); assert.equal(actions(f).length, 1);
});
test('a refresh already in progress cannot race a confirmation into a mutation', async () => {
  let block = false; const pending = deferred(); const f = await fixture({ recoveryRead: (chat, state, normal) => block ? pending.promise : normal() });
  await f.prepare('propose'); await f.consent(); block = true; const read = f.ctx.refreshE2eeRecovery(f.ctx.selected(), () => true, true); await flush();
  await f.ctx.submitE2eeRecovery(); assert.equal(actions(f).length, 0);
  f.patch({ proposals: [{ ...proposal(true, 'expired'), sendingFrozen: false }] }); pending.resolve(current(f)); await read;
  assert.equal(panel(f).consent, null); assert.equal(f.byId('e2eeRecoveryConfirm').checked, false);
});
test('late recovery acceptance after account retirement cannot repopulate UI or start a new read', async () => {
  const pending = deferred(); const f = await fixture({ mutate: () => pending.promise }); await f.prepare('propose'); await f.consent(); await f.submit();
  const reads = kinds(f, 'recovery').length; await f.account(); pending.resolve({ context: 2, recovery: { kind: 'queued', conversationId: CHAT, cryptoEpoch: '0', operationId: id(500) } }); await flush();
  assert.equal(f.ctx.selected(), null); assert.equal(f.byId('e2eeRecoveryPanel').hidden, true); assert.equal(f.byId('e2eeRecoveryProposals').children.length, 0); assert.equal(kinds(f, 'recovery').length, reads); assert.equal(f.timers.size, 0);
});
test('an unknown result with no persisted proposal allows only a newly confirmed explicit retry', async () => {
  const f = await fixture({ mutate: () => { throw 'native_wait'; } }); await f.prepare('propose'); await f.consent(); await f.submit();
  assert.equal(panel(f).fresh, true); assert.equal(panel(f).state.proposals.length, 0); await f.submit(); await f.next(); assert.equal(actions(f).length, 1);
  await f.prepare('propose'); await f.submit(); assert.equal(actions(f).length, 1); await f.consent(); await f.submit(); assert.equal(actions(f).length, 2);
});
test('local queued result cannot enable sending before its fresh native status read finishes', async () => {
  const pending = deferred(); let block = false; const f = await fixture({ recoveryRead: (chat, state, normal) => block ? pending.promise : normal() });
  await f.type('preserve'); await f.prepare('propose'); await f.consent(); block = true; await f.submit();
  assert.equal(panel(f).fresh, false); await f.ctx.sendE2eeText(); assert.equal(kinds(f, 'send').length, 0); assert.equal(f.ctx.drafts()[0].messageId, null);
  pending.resolve(current(f)); await flush(); assert.equal(panel(f).fresh, true); assert.equal(panel(f).state.sendingFrozen, true); assert.equal(f.byId('e2eeTextSend').disabled, true);
});
test('a foreign native recovery context is rejected without reattaching the old form', async () => {
  let foreign = false; const f = await fixture({ recoveryRead: (chat, state, normal) => ({ ...normal(), context: foreign ? 3 : 2 }) });
  await f.type('copy me'); await f.prepare('propose'); await f.consent(); foreign = true; await f.check();
  assert.equal(panel(f).fresh, false); assert.equal(panel(f).consent, null); assert.ok(f.ctx.selected().suspended); assert.equal(f.byId('e2eeTextInput').value, 'copy me'); await f.submit(); assert.equal(actions(f).length, 0);
});
test('pagehide retires an in-flight recovery UI and its late result cannot restore history, consent or timers', async () => {
  const pending = deferred(); const f = await fixture({ history: [message(1)], mutate: () => pending.promise }); await f.prepare('propose'); await f.consent(); await f.submit();
  const reads = kinds(f, 'recovery').length; await f.emit('pagehide');
  pending.resolve({ context: 2, recovery: { kind: 'queued', conversationId: CHAT, cryptoEpoch: '0', operationId: id(500) } }); await flush();
  assert.equal(f.ctx.selected(), null); assert.equal(f.byId('e2eeHistory').children.length, 0); assert.equal(f.byId('e2eeRecoveryPanel').hidden, true); assert.equal(f.byId('e2eeRecoveryConfirm').checked, false); assert.equal(kinds(f, 'recovery').length, reads); assert.equal(f.timers.size, 0);
});
