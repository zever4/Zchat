import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const source = ['025-e2ee-view.ts', '025-e2ee-composer.ts', '025-e2ee-discovery.ts'].map(compile).join('\n');
const flush = () => new Promise(resolve => setImmediate(resolve));
const id = n => '00000000-0000-4000-8000-' + n.toString(16).padStart(12, '0');
const CHAT = id(1), PEER = id(2);
const metadata = (conversationId = CHAT, state = 'active') => ({ conversationId, peerUserId: PEER, state, closeReason: state === 'closed' ? 'device_replaced' : null, closedAtUnixMs: state === 'closed' ? '1' : '0' });
const message = (n, outgoing = false) => ({ messageId: id(n + 100), conversationId: CHAT, cryptoEpoch: '0', createdAtUnixMs: String(n), senderUserId: PEER, text: 'message ' + n, outgoing, delivery: outgoing ? { storedOnServer: false, state: null, removal: null, sealedUnsent: false } : null });
class Element {
  children = []; dataset = {}; style = {}; hidden = false; disabled = false; readOnly = false; textContent = ''; value = ''; scrollTop = 0; clientHeight = 100; listeners = new Map(); attrs = {};
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
  const api = { compiled: options.compiled !== false,
    status: () => call('status', [], () => options.status ? options.status(state) : { ...state }),
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
  byId('e2eeDialogsSection').hidden = true;
  runInNewContext(source + '\nthis.selected=()=>e2eeHistoryView; this.drafts=()=>[...e2eeDrafts.values()]; this.catalogue=()=>[...e2eeDialogs.values()];', ctx);
  const f = { ctx, nodes, byId, calls, timers, writes, page, api,
    async next() { const entry = [...timers.entries()].sort((a, b) => a[1].time - b[1].time)[0]; if (!entry) return false; timers.delete(entry[0]); now = entry[1].time; entry[1].fn(); await flush(); return true; },
    async emit(name, event = {}) { for (const fn of listeners.get(name) || []) fn(event); await flush(); },
    async visible(value) { ctx.document.visibilityState = value ? 'visible' : 'hidden'; await this.emit('visibilitychange'); },
    setState(value) { state = { ...state, ...value }; }, setHistory(value) { history = value; }, setDialogs(value) { dialogs = value; },
    async open(chat = CHAT) { ctx.openE2eeConversation({ context: state.context, conversation: metadata(chat) }, 'Alice'); await flush(); },
    async type(text) { byId('e2eeTextInput').value = text; await byId('e2eeTextInput').emit('input'); },
    async account() { session = 'other'; await this.emit('storage', { key: 'token' }); }, maxActive: () => maxActive,
  };
  ctx.startE2eeDiscovery(); await flush();
  return f;
}
const kinds = (f, kind) => f.calls.filter(call => call[0] === kind);
const transcript = f => f.byId('e2eeHistory').children.map(row => row.children[1].textContent);

test('browser/default build has no sidebar, timers, HTTP or native reads', async () => {
  for (const options of [{ browser: true }, { compiled: false }]) { const f = await fixture(options); assert.equal(f.byId('e2eeDialogsSection').hidden, true); assert.equal(f.timers.size, 0); assert.equal(f.calls.length, 0); }
});
test('native empty and closed dialogs are discovered without profile creation; names are literal display-only data', async () => {
  const f = await fixture({ dialogs: [metadata(), metadata(id(3), 'closed')] }); await f.next();
  const rows = f.byId('e2eeDialogsList').children; assert.equal(rows.length, 2); assert.equal(rows[0].children[0].textContent, '<b>Alice</b>');
  assert.match(rows[1].children[1].textContent, /Закрыт/); await rows[1].click();
  assert.equal(f.ctx.currentChatID, null); assert.equal(f.byId('e2eeTextInput').readOnly, true); assert.equal(f.ctx.selected().reply.conversation.conversationId, id(3));
});
test('offline/unfriended existing dialog remains readable with a neutral label and no numeric ID cast', async () => {
  const f = await fixture({ friends: async () => { throw Error('offline'); } }); await f.next();
  const row = f.byId('e2eeDialogsList').children[0]; assert.equal(row.children[0].textContent, 'Защищённый диалог'); await row.click();
  assert.equal(kinds(f, 'history')[0][1], CHAT); assert.equal(f.ctx.currentChatType, 'e2ee'); assert.equal(f.writes.length, 0);
});
test('catalogue makes exactly one local page per cycle, wraps, deduplicates, and renders at most eight rows', async () => {
  const f = await fixture({ dialogs: Array.from({ length: 19 }, (_, n) => metadata(id(n + 1))) });
  for (let i = 0; i < 3; i++) { await f.next(); assert.equal(kinds(f, 'list').length, i + 1); assert.ok(f.byId('e2eeDialogsList').children.length <= 8); }
  assert.equal(f.ctx.catalogue().length, 19); assert.deepEqual(kinds(f, 'list').map(call => call[1]), [null, '8', '16']);
  await f.next(); assert.equal(kinds(f, 'list').at(-1)[1], null); assert.equal(f.ctx.catalogue().length, 19);
  await f.byId('e2eeDialogsNext').click(); assert.equal(f.byId('e2eeDialogsList').children[0].dataset.conversationId, id(9));
  assert.equal(kinds(f, 'http').length, 1);
});
test('selected native closure disables composer; a delayed active projection cannot reopen it', async () => {
  const f = await fixture(); await f.next(); await f.open(); await f.type('keep');
  f.setDialogs([metadata(CHAT, 'closed')]); await f.next(); assert.equal(f.byId('e2eeTextInput').readOnly, true);
  f.setDialogs([metadata()]); await f.next(); assert.equal(f.ctx.selected().reply.conversation.state, 'closed'); assert.equal(f.byId('e2eeTextInput').value, 'keep');
});
test('a hanging native read never overlaps another refresh, visibility resume or repeated manual refresh', async () => {
  const pending = deferred(); const f = await fixture({ list: () => pending.promise }); await f.next();
  await f.byId('e2eeDialogsRefresh').click(); await f.visible(false); await f.visible(true); await f.ctx.refreshE2eeDiscovery();
  assert.equal(kinds(f, 'list').length, 1); assert.equal(f.timers.size, 0);
  pending.resolve(f.page('dialogs', [metadata()], null)); await flush(); assert.equal(f.ctx.catalogue().length, 0); assert.equal(f.timers.size, 1); assert.equal(f.maxActive(), 1);
});
test('hidden/pagehide/account changes discard late replies and clear account metadata and plaintext', async () => {
  for (const end of ['account', 'pagehide']) {
    const pending = deferred(); const f = await fixture({ list: () => pending.promise }); await f.open(); await f.type('secret'); await f.next();
    if (end === 'account') await f.account(); else await f.emit('pagehide');
    pending.resolve(f.page('dialogs', [metadata()], null)); await flush();
    assert.equal(f.ctx.catalogue().length, 0); assert.equal(f.ctx.drafts().length, 0); assert.equal(f.byId('e2eeHistory').children.length, 0); assert.equal(f.timers.size, 0); assert.equal(f.byId('e2eeDialogsSection').hidden, true);
  }
});
test('a hidden window polls the native sound revision without requesting dialog names or history', async () => {
  const f = await fixture(); await f.next(); await f.open();
  const observed = [];
  f.ctx.observeProtectedMessageSound = state => observed.push(state.incomingMessageRevision);
  f.setState({ incomingMessageRevision: '1' });
  await f.visible(false);
  const before = f.calls.length;
  await f.next();
  assert.deepEqual(f.calls.slice(before), [['status']]);
  assert.deepEqual(observed, ['1']);
  assert.equal(f.timers.values().next().value.delay, 5000);
});
test('visible local refresh appends incoming literal text without reopening view or altering a draft', async () => {
  const f = await fixture({ history: [message(1)] }); await f.next(); await f.open(); await f.type('pending draft'); const view = f.ctx.selected();
  const incoming = message(2); incoming.text = '<img src=x onerror=bad()>\nplain'; f.setHistory([incoming, message(1)]); await f.next();
  assert.equal(f.ctx.selected(), view); assert.deepEqual(transcript(f), ['message 1', incoming.text]); assert.equal(f.byId('e2eeTextInput').value, 'pending draft'); assert.equal(f.writes.length, 0); assert.equal(kinds(f, 'send').length, 0);
});
test('backlog above one page is bridged in bounded steps rather than silently dropping middle messages', async () => {
  const f = await fixture({ history: [message(1)] }); await f.next(); await f.open();
  f.setHistory(Array.from({ length: 30 }, (_, n) => message(30 - n)));
  const start = kinds(f, 'history').length;
  for (let i = 0; i < 4; i++) { await f.next(); assert.equal(kinds(f, 'history').length, start + i + 1); }
  assert.deepEqual(transcript(f), Array.from({ length: 30 }, (_, n) => 'message ' + (n + 1))); assert.equal(f.ctx.selected().catchup, null);
});
test('reading older messages preserves scroll/DOM/draft while outgoing statuses refresh and new text is announced', async () => {
  const old = Array.from({ length: 20 }, (_, n) => message(20 - n, true)); const f = await fixture({ history: old }); await f.next(); await f.open();
  await f.byId('e2eeHistoryMore').click(); await f.type('draft'); const before = transcript(f); f.byId('e2eeHistory').scrollTop = 12;
  f.setHistory([message(21), ...old]); await f.next();
  assert.deepEqual(transcript(f), before); assert.equal(f.byId('e2eeHistory').scrollTop, 12); assert.equal(f.byId('e2eeTextInput').value, 'draft'); assert.match(f.byId('e2eeHistoryNotice').textContent, /Есть новые/);
  assert.match(f.byId('e2eeHistory').children.at(-1).children[2].textContent, /Сохранено/);
});
test('late ACK status rotates batches of at most eight and preserves the previous expiry fact', async () => {
  const old = Array.from({ length: 24 }, (_, n) => message(24 - n, true)); const f = await fixture({ history: old }); await f.next(); await f.open();
  await f.byId('e2eeHistoryMore').click(); await f.byId('e2eeHistoryMore').click();
  for (let n = 0; n < 3; n++) await f.next();
  const batches = kinds(f, 'statuses'); assert.equal(batches.length, 3); assert.ok(batches.every(call => call[2].length <= 8)); assert.equal(new Set(batches.flatMap(call => call[2])).size, 24);
  for (const row of f.byId('e2eeHistory').children) { assert.equal(row.children[2].textContent, 'Сохранено получателем'); assert.match(row.children[2].title, /истёк/); }
});
test('new owner context freezes the old draft before any send and never silently binds it to new keys', async () => {
  const f = await fixture(); await f.next(); await f.open(); await f.type('copy me'); f.setState({ context: 3 }); await f.next();
  assert.equal(f.byId('e2eeTextInput').value, 'copy me'); assert.equal(f.byId('e2eeTextInput').readOnly, true); await f.ctx.sendE2eeText(); assert.equal(kinds(f, 'send').length, 0); assert.equal(f.ctx.drafts()[0].context, 2);
  await f.byId('e2eeDialogsList').children[0].click(); assert.equal(f.ctx.selected().reply.context, 3); assert.equal(f.byId('e2eeTextInput').value, '');
});
test('temporarily unavailable owner pauses reads and send; same context resumes without resetting draft', async () => {
  const f = await fixture(); await f.next(); await f.open(); await f.type('offline'); f.setState({ runtimePhase: 'Starting' }); const reads = kinds(f, 'list').length; await f.next();
  assert.equal(kinds(f, 'list').length, reads); assert.equal(f.byId('e2eeTextSend').disabled, true); await f.ctx.sendE2eeText(); assert.equal(kinds(f, 'send').length, 0);
  f.setState({ runtimePhase: 'Running' }); await f.next(); assert.equal(f.byId('e2eeTextInput').value, 'offline'); assert.equal(f.byId('e2eeTextSend').disabled, false);
});
test('unknown draft keeps exact UUID and text across local polling and manual retry', async () => {
  const f = await fixture(); await f.next(); await f.open(); await f.type('same body'); await f.ctx.sendE2eeText(); const first = kinds(f, 'send')[0];
  f.setHistory([message(2)]); await f.next(); assert.equal(f.byId('e2eeTextInput').readOnly, true); assert.equal(f.byId('e2eeTextInput').value, 'same body');
  await f.ctx.sendE2eeText(); assert.deepEqual(kinds(f, 'send')[1], first); assert.equal(f.writes.length, 0);
});
test('sweep stale cursor resets to latest with bounded exponential backoff, successful reads restore two seconds', async () => {
  let fail = true; const f = await fixture({ list: () => { if (fail) throw 'stale'; return f.page('dialogs', [metadata()], null); } });
  for (const delay of [4000, 8000, 16000, 16000]) { await f.next(); assert.equal([...f.timers.values()][0].delay, delay); }
  fail = false; await f.next(); assert.equal([...f.timers.values()][0].delay, 2000); assert.equal(kinds(f, 'list').at(-1)[1], null);
});
test('large catch-up keeps whole pages below 128 and its cursor reaches all remaining native history', async () => {
  const f = await fixture({ history: [message(1)] }); await f.next(); await f.open();
  f.setHistory(Array.from({ length: 250 }, (_, n) => message(250 - n)));
  for (let n = 0; n < 15; n++) { await f.next(); assert.ok((f.ctx.selected().catchup?.items.length || 0) <= 128); }
  assert.equal(f.byId('e2eeHistory').children.length, 120); assert.equal(transcript(f)[0], 'message 131');
  const observed = new Set(transcript(f));
  while (f.ctx.selected().cursor) { await f.byId('e2eeHistoryMore').click(); assert.ok(f.byId('e2eeHistory').children.length <= 128); transcript(f).forEach(text => observed.add(text)); }
  assert.equal(observed.size, 250); assert.ok(observed.has('message 1'));
});
test('late insertion within latest page keeps native epoch/time/UUID ordering', async () => {
  const f = await fixture({ history: [message(3), message(1)] }); await f.next(); await f.open(); f.setHistory([message(3), message(2), message(1)]); await f.next();
  assert.deepEqual(transcript(f), ['message 1', 'message 2', 'message 3']);
});
test('initial view request for a previous selection cannot be installed by background refresh', async () => {
  const pending = deferred(); let block = false;
  const f = await fixture({ read: (chat, cursor, state) => block && chat === CHAT ? pending.promise : f.page('history', [message(1)], cursor, state.context) });
  await f.next(); await f.open(); block = true; await f.next(); await f.open(id(3)); const view = f.ctx.selected();
  pending.resolve(f.page('history', [message(99)], null)); await flush(); assert.equal(f.ctx.selected(), view); assert.deepEqual(transcript(f), ['message 1']);
});
test('scrolling away during a staged catch-up discards it without jumping or replacing the old window', async () => {
  const f = await fixture({ history: Array.from({ length: 8 }, (_, n) => message(8 - n)) }); await f.next(); await f.open();
  f.setHistory(Array.from({ length: 20 }, (_, n) => message(20 - n))); await f.next(); assert.ok(f.ctx.selected().catchup);
  const before = transcript(f); f.byId('e2eeHistory').scrollTop = 0; await f.next();
  assert.deepEqual(transcript(f), before); assert.equal(f.byId('e2eeHistory').scrollTop, 0); assert.equal(f.ctx.selected().catchup, null);
});
test('a periodic bounded window audit finds a delayed insertion below the latest page', async () => {
  const old = Array.from({ length: 24 }, (_, n) => message(24 - n)); const f = await fixture({ history: old }); await f.next(); await f.open();
  const late = message(1000); late.createdAtUnixMs = '10'; late.text = 'late older text'; f.setHistory([...old.slice(0, 14), late, ...old.slice(14)]);
  for (let n = 0; n < 19; n++) await f.next();
  assert.ok(transcript(f).includes(late.text)); assert.ok(f.byId('e2eeHistory').children.length <= 128); assert.equal(f.ctx.selected().catchup, null);
});
test('initial transient history failure is retried by the local scheduler without automatic send or setup', async () => {
  let failing = true;
  const f = await fixture({ read: (chat, cursor) => { if (failing) throw 'busy'; return f.page('history', [message(1)], cursor); } });
  await f.next(); await f.open(); assert.equal(f.ctx.selected().loaded, false); failing = false; await f.next();
  assert.equal(f.ctx.selected().loaded, true); assert.deepEqual(transcript(f), ['message 1']); assert.equal(kinds(f, 'send').length, 0);
});
test('a hidden initial plaintext reply is ignored and retried only after visibility resumes', async () => {
  const pending = deferred(); let blocked = true;
  const f = await fixture({ read: (chat, cursor) => blocked ? pending.promise : f.page('history', [message(1)], cursor) });
  await f.next(); await f.open(); await f.visible(false); pending.resolve(f.page('history', [message(99)], null)); await flush();
  assert.equal(f.byId('e2eeHistory').children.length, 0); assert.equal(f.ctx.selected().loaded, false); blocked = false;
  await f.visible(true); await f.next(); assert.deepEqual(transcript(f), ['message 1']);
});
test('a staged history audit cannot roll back an already observed recipient ACK to its old pending snapshot', async () => {
  const old = Array.from({ length: 24 }, (_, n) => message(24 - n, true)); const f = await fixture({ history: old }); await f.next(); await f.open();
  for (let n = 0; n < 19; n++) await f.next();
  assert.equal(f.byId('e2eeHistory').children.length, 24);
  // The oldest batch may still wait its first status refresh; all initially
  // visible outgoing IDs had a known ACK before the staged audit started.
  const originalIDs = new Set(old.slice(0, 8).map(item => item.messageId));
  for (const row of f.byId('e2eeHistory').children.filter(row => originalIDs.has(row.dataset.messageId))) assert.equal(row.children[2].textContent, 'Сохранено получателем');
});
test('an in-flight status reply is discarded on account change, with no residual timer or rendered ACK', async () => {
  const pending = deferred(); const f = await fixture({ history: [message(1, true)], statuses: () => pending.promise }); await f.next(); await f.open(); await f.next();
  assert.equal(kinds(f, 'statuses').length, 1); await f.account();
  pending.resolve({ context: 2, page: { kind: 'statuses', items: [{ messageId: message(1).messageId, cryptoEpoch: '0', delivery: { state: 'recipient_stored', removal: null } }], nextCursor: null } }); await flush();
  assert.equal(f.byId('e2eeHistory').children.length, 0); assert.equal(f.ctx.catalogue().length, 0); assert.equal(f.timers.size, 0);
});
test('same-tab auth expiry clears discovery even without a storage event', async () => {
  const f = await fixture(); await f.next(); await f.open(); await f.type('secret'); f.ctx.authExpiredHandled = true; f.ctx.leaveE2eeView();
  assert.equal(f.ctx.catalogue().length, 0); assert.equal(f.ctx.drafts().length, 0); assert.equal(f.byId('e2eeDialogsSection').hidden, true); assert.equal(f.timers.size, 0);
});
test('unchanged polling preserves sidebar row identity for keyboard focus', async () => {
  const f = await fixture(); await f.next(); const row = f.byId('e2eeDialogsList').children[0]; await f.next();
  assert.equal(f.byId('e2eeDialogsList').children[0], row); await row.click(); const selected = f.byId('e2eeDialogsList').children[0];
  await f.next(); assert.equal(f.byId('e2eeDialogsList').children[0], selected); assert.equal(selected.attrs['aria-pressed'], 'true');
});
