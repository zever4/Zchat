import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const code = ts.transpile(readFileSync(new URL('../web/src/chat/025-profile-chat-choice.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const profileCode = ts.transpile(readFileSync(new URL('../web/src/chat/025-user-profile-modal-read-only.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const escapeCode = ts.transpile(readFileSync(new URL('../web/src/chat/026-escape-key-close-all-modals.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const PEER = '11111111-1111-4111-8111-111111111111';
const OLD = '22222222-2222-4222-8222-222222222222';
const NEW = '33333333-3333-4333-8333-333333333333';
const tick = () => new Promise(resolve => setImmediate(resolve));
class Element {
  children = []; listeners = new Map(); dataset = {}; style = {}; hidden = false; disabled = false; checked = false; textContent = ''; className = '';
  append(...items) { this.children.push(...items); }
  appendChild(item) { this.append(item); return item; }
  setAttribute(name, value) { this[name] = value; }
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  async click() { if (!this.disabled && !this.hidden) this.listeners.get('click')?.(); await tick(); }
  change() { this.listeners.get('change')?.(); }
}
function find(root, choice) { return root.children.find(n => n.dataset.chatChoice === choice) || root.children.map(n => find(n, choice)).find(Boolean); }
function reply(closed = false, peer = PEER) { return { context: 2, conversation: { conversationId: closed ? OLD : NEW, peerUserId: peer, state: closed ? 'closed' : 'active', closeReason: closed ? 'device_replaced' : null, closedAtUnixMs: closed ? '1' : '0' } }; }
async function fixture(options = {}) {
  const root = new Element(), apiCalls = [], http = [], switches = [], writes = [], views = [];
  let live = true, session = 'session';
  const api = { compiled: options.compiled !== false,
    status: async () => { apiCalls.push(['status']); return options.status ? options.status() : { context: 2, phase: 'active', runtimePhase: 'Running' }; },
    createConversation: async peer => { apiCalls.push(['create', peer]); return options.create ? options.create() : reply(); },
    createConversationAfterClosed: async (...args) => { apiCalls.push(['successor', ...args]); return options.successor ? options.successor() : reply(); },
  };
  const ctx = { AbortController, token: 'session', authExpiredHandled: false,
    document: { createElement: () => new Element() },
    localStorage: { getItem: () => session, setItem: (...args) => writes.push(args) },
    chats: options.noPlain ? [] : [{ id: 17, type: 'private', other_user_id: '42', other_username: 'Bob' }],
    window: options.browser ? {} : { zchatDesktop: { e2ee: api } },
    apiFetch: async (url, init) => {
      http.push([url, init]);
      if (url.endsWith('/private-chat')) return options.ordinaryRequest();
      return options.friendsRequest ? options.friendsRequest() : { ok: true, json: async () => options.friends ?? [{ id: 42, user_uuid: PEER }] };
    },
    loadChats: async () => {},
    closeUserProfileModal: () => { live = false; }, switchChat: (...args) => switches.push(args),
    openE2eeConversation: (...args) => views.push(args),
    e2eeVoiceRecording: () => !!options.recording,
  };
  runInNewContext(code, ctx);
  const current = () => live && ctx.localStorage.getItem('token') === ctx.token;
  await ctx.mountProfileChatChoices(root, { id: '42', username: 'Bob' }, current, new AbortController().signal);
  return { root, ctx, apiCalls, http, switches, writes, views, action: name => find(root, name), status: () => root.children[0].children.find(n => n.className === 'profile-chat-status' && !n.dataset.chatChoice).textContent,
    close: () => { live = false; }, changeAccount: () => { session = 'other'; } };
}
test('browser/default show desktop availability and never invoke native or silently open a plain chat', async () => {
  for (const options of [{ browser: true }, { compiled: false }]) {
    const f = await fixture(options);
    assert.equal(f.action('encrypted').disabled, true);
    assert.match(f.status(), /шифрование/);
    assert.equal(f.http.length, 0); assert.equal(f.apiCalls.length, 0); assert.equal(f.switches.length, 0);
    await f.action('ordinary').click(); assert.deepEqual(f.switches, [[17, 'Bob', 'private']]);
  }
});
test('a regular chat requires an explicit click and uses HTTP with the selected peer', async () => {
  const f = await fixture({ browser: true, noPlain: true, ordinaryRequest: () => ({
    ok: true, json: async () => ({ id: 21, type: 'private', other_user_id: 42, other_username: 'Bob' }),
  }) });
  assert.equal(f.http.length, 0);
  await f.action('ordinary').click();
  assert.equal(f.http[0][0], '/api/users/42/private-chat');
  assert.equal(f.http[0][1].method, 'POST');
  assert.deepEqual(f.switches, [[21, 'Bob', 'private']]);
  assert.equal(f.apiCalls.length, 0);
});
test('ordinary privacy denial is shown and a late reply cannot change the selected chat', async () => {
  let finish;
  const f = await fixture({ browser: true, noPlain: true, ordinaryRequest: () => new Promise(resolve => { finish = resolve; }) });
  await f.action('ordinary').click();
  await f.action('ordinary').click();
  assert.equal(f.http.length, 1);
  finish({ status: 403, ok: false }); await tick();
  assert.match(f.action('ordinary-status').textContent, /Настройки собеседника/);
  assert.deepEqual(f.switches, []);
  await f.action('ordinary').click();
  f.changeAccount();
  finish({ ok: true, json: async () => ({ id: 21, type: 'private', other_user_id: 42 }) }); await tick();
  assert.deepEqual(f.switches, []);
});
test('desktop requires a unique authenticated friendship and canonical public identity before any mutation', async () => {
  for (const friends of [[], [{ id: 42, user_uuid: '' }], [{ id: 42, user_uuid: PEER }, { id: '42', user_uuid: PEER }], [{ id: '042', user_uuid: PEER }]]) {
    const f = await fixture({ friends }); await f.action('encrypted').click();
    assert.equal(f.action('encrypted').disabled, true); assert.equal(f.apiCalls.length, 0);
    assert.equal(f.http[0][0], '/api/friends');
  }
});
test('profile IDs preserve exact strings and reject rounded numbers, paths and noncanonical values', async () => {
  const f = await fixture();
  for (const value of [0, -1, '0', '042', '../path', 9007199254740992, '9223372036854775808', {}, null]) assert.equal(f.ctx.profilePeerId(value), null);
  assert.equal(f.ctx.profilePeerId('9007199254740993'), '9007199254740993');
});
test('create uses only the selected numeric friend; no setup/replacement/plain outbox or automatic ordinary fallback', async () => {
  const f = await fixture(); await f.action('encrypted').click();
  assert.deepEqual(f.apiCalls, [['status'], ['create', '42']]);
  assert.match(f.status(), /диалог готов/); assert.equal(f.switches.length, 0); assert.equal(f.writes.length, 0);
  assert.equal(f.views.length, 1); assert.equal(f.views[0][0].conversation.conversationId, NEW);
});
test('key setup is explicit and an initializing runtime cannot trigger a Create', async () => {
  for (const state of [{ phase: 'setup_required' }, { phase: 'active', runtimePhase: 'HistoryIndex' }]) {
    const f = await fixture({ status: () => state }); await f.action('encrypted').click();
    assert.deepEqual(f.apiCalls, [['status']]); assert.match(f.status(), state.phase === 'active' ? /Подождите/ : /настройку ключей/);
  }
});
test('an active voice recording blocks encrypted creation before the microphone stop control can be hidden', async () => {
  const f = await fixture({ recording: true }); await f.action('encrypted').click();
  assert.match(f.status(), /Завершите запись/); assert.equal(f.apiCalls.length, 0); assert.equal(f.views.length, 0);
});
test('closed result requires a separate checked consent and same closed ID on every successor retry', async () => {
  let attempts = 0;
  const f = await fixture({ create: () => reply(true), successor: () => { if (++attempts === 1) throw 'remote'; return reply(); } });
  await f.action('encrypted').click();
  assert.match(f.status(), /диалог закрыт/); assert.equal(f.action('successor').disabled, true);
  await f.action('successor').click(); assert.equal(f.apiCalls.some(c => c[0] === 'successor'), false);
  for (let i = 0; i < 2; i++) {
    f.action('confirm').checked = true; f.action('confirm').change(); await f.action('successor').click();
    assert.equal(f.action('confirm').checked, false);
  }
  assert.deepEqual(f.apiCalls.filter(c => c[0] === 'successor'), [['successor', '42', OLD, true], ['successor', '42', OLD, true]]);
  assert.match(f.status(), /диалог готов/); assert.equal(f.action('successor').hidden, true);
});
test('foreign native peer cannot be labelled with the selected profile or authorize successor', async () => {
  const f = await fixture({ create: () => reply(true, NEW) }); await f.action('encrypted').click();
  assert.match(f.status(), /подтвердить собеседника/); assert.equal(f.action('successor').hidden, true);
  assert.equal(f.views.length, 0);
  assert.equal(f.switches.length, 0); assert.equal(f.writes.length, 0);
});
test('closed history can be opened without consenting to a new conversation', async () => {
  const f = await fixture({ create: () => reply(true) }); await f.action('encrypted').click();
  assert.equal(f.action('history').hidden, false); await f.action('history').click();
  assert.equal(f.views.length, 1); assert.equal(f.views[0][0].conversation.conversationId, OLD);
  assert.equal(f.apiCalls.some(c => c[0] === 'successor'), false);
});
test('unknown result has an explicit same-request retry and never a plaintext fallback', async () => {
  let n = 0;
  const f = await fixture({ create: () => { if (++n === 1) throw 'remote'; return reply(); } });
  await f.action('encrypted').click(); assert.match(f.status(), /сохранённая операция продолжится/);
  await f.action('encrypted').click(); assert.match(f.status(), /диалог готов/);
  assert.deepEqual(f.apiCalls.filter(c => c[0] === 'create'), [['create', '42'], ['create', '42']]); assert.equal(f.switches.length, 0);
});
test('busy double click is bounded; account change or closed modal discards native completion', async () => {
  for (const leave of ['close', 'changeAccount']) {
    let finish;
    const f = await fixture({ create: () => new Promise(resolve => { finish = resolve; }) });
    await f.action('encrypted').click(); const before = f.status();
    await f.action('encrypted').click(); assert.equal(f.apiCalls.filter(c => c[0] === 'create').length, 1);
    f[leave](); finish(reply()); await tick();
    assert.equal(f.status(), before); assert.equal(f.switches.length, 0);
  }
});
test('closing a modal while checking runtime prevents even starting the mutation', async () => {
  let finish;
  const f = await fixture({ status: () => new Promise(resolve => { finish = resolve; }) });
  await f.action('encrypted').click(); f.close(); finish({ phase: 'active', runtimePhase: 'Running' }); await tick();
  assert.deepEqual(f.apiCalls, [['status']]);
});
test('profile response after close/account change/reopen cannot reopen or relabel a stale modal', async () => {
  const fields = new Map(['userProfileModal', 'userProfileAvatar', 'userProfileUsername', 'userProfileBio'].map(k => [k, new Element()]));
  const modal = fields.get('userProfileModal'), content = new Element(), close = new Element();
  modal.querySelector = () => content; content.querySelector = () => close;
  let session = 'session'; const pending = [], mounted = [], listeners = new Map();
  const ctx = { AbortController, token: 'session', authExpiredHandled: false, localStorage: { getItem: () => session },
    document: { createElement: () => new Element(), querySelector: () => null, addEventListener: (name, fn) => listeners.set(name, fn) }, window: { addEventListener() {} }, chats: [],
    byId: id => fields.get(id), optionalById: id => fields.get(id), displayAssetURL: v => v, console, notifPanelOpen: false, infoPanelOpen: false,
    apiFetch: url => new Promise(resolve => pending.push({ url, resolve })),
  };
  runInNewContext(code, ctx); runInNewContext(profileCode, ctx);
  runInNewContext(escapeCode, ctx);
  ctx.mountProfileChatChoices = async (_, peer) => { mounted.push(peer.id); };
  const one = ctx.openUserProfileModal('42'); const two = ctx.openUserProfileModal('43');
  pending[1].resolve({ ok: true, json: async () => ({ id: '43', username: 'Carol' }) }); await two;
  pending[0].resolve({ ok: true, json: async () => ({ id: '42', username: 'Bob' }) }); await one;
  assert.equal(fields.get('userProfileUsername').textContent, 'Carol'); assert.deepEqual(mounted, ['43']);
  const closed = ctx.openUserProfileModal('42'); listeners.get('keydown')({ key: 'Escape' }); pending[2].resolve({ ok: true, json: async () => ({ id: '42', username: 'late' }) }); await closed;
  assert.equal(modal.style.display, 'none');
  const changed = ctx.openUserProfileModal('42'); session = 'other'; pending[3].resolve({ ok: true, json: async () => ({ id: '42', username: 'foreign' }) }); await changed;
  assert.equal(modal.style.display, 'none'); assert.deepEqual(mounted, ['43']);
});
