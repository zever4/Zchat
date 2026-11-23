import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const source = compile('008-message-outbox.ts');
const sendSource = compile('018-send-message.ts');
const mediaSource = compile('039-helper-send-media-message-through-ws.ts');

class Element {
  children = []; listeners = {}; className = ''; textContent = ''; value = '';
  classList = { remove() {} };
  append(...children) { for (const child of children) { child.parent = this; this.children.push(child); } }
  remove() { this.parent.children = this.parent.children.filter(child => child !== this); }
  querySelectorAll(selector) { return this.children.filter(child => '.' + child.className === selector); }
  setAttribute() {}
  addEventListener(name, fn) { this.listeners[name] = fn; }
  focus() {}
}

function fixture({ data = new Map(), account = '1', server = 'https://one.invalid', supported = true } = {}) {
  let now = 100000, next = 0;
  const timers = new Map(), listeners = new Map(), frames = [], alerts = [], history = [];
  class Clock extends Date {
    constructor(value) { super(value ?? now); }
    static now() { return now; }
  }
  const setTimeout = (fn, ms) => { const id = ++next; timers.set(id, { at: now + ms, fn }); return id; };
  const clearTimeout = id => timers.delete(id);
  const storage = { get length() { return data.size; }, key: index => [...data.keys()][index],
    getItem: key => data.get(key) ?? null, setItem: (key, value) => data.set(key, value), removeItem: key => data.delete(key) };
  const ctx = {
    crypto: webcrypto, TextEncoder, Uint8Array, Date: Clock, console, clearTimeout,
    window: { zchatDesktop: { serverUrl: server }, setTimeout, addEventListener: (name, fn) => listeners.set(name, fn) },
    location: { origin: 'http://tauri.localhost' }, localStorage: storage,
    accountStorageKey: base => base + ':' + account, canReconnectChat: () => true,
    socket: { readyState: 1, send: frame => frames.push(JSON.parse(frame)) }, WebSocket: { OPEN: 1 },
    messageReceiptsSupported: supported, currentChatID: 42, messageCache: {},
    document: { createElement: () => new Element() }, messagesEl: new Element(),
    messageInput: new Element(), sendBtn: new Element(), isTyping: false, typingTimeout: null,
    alert: message => alerts.push(message), confirm: () => true,
    loadChatHistory: (id, cursor) => history.push([id, cursor]),
  };
  runInNewContext(source + sendSource + mediaSource + `
    this.api = { queue: queueOrdinaryMessage, flush: flushMessageOutbox, pause: pauseMessageOutbox,
      receipt: handleMessageReceipt, entries: () => [...messageOutbox.values()], prefix: outboxPrefix,
      render: renderPendingMessages, send: sendMessage, media: sendMediaMessage, file: sendFileMessage };`, ctx);
  const advance = ms => {
    const end = now + ms;
    for (;;) {
      const timer = [...timers].filter(([, value]) => value.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!timer) break;
      timers.delete(timer[0]); now = timer[1].at; timer[1].fn();
    }
    now = end;
  };
  const ack = (frame = frames[0]) => ctx.api.receipt({ type: 'message_ack', client_message_id: frame.client_message_id, chat_id: frame.chat_id, id: '7' });
  return { ctx, api: ctx.api, frames, data, alerts, history, advance, timers, listeners, ack };
}

test('draft is cleared only after durable local enqueue; socket.send does not remove the pending row', async () => {
  const f = fixture(); f.ctx.messageInput.value = '<script>draft</script>';
  await f.api.send();
  assert.equal(f.ctx.messageInput.value, ''); assert.equal(f.data.size, 1);
  assert.equal(f.api.entries()[0].state, 'sending'); assert.equal(f.frames.length, 1);
  assert.equal(f.ctx.messagesEl.children[0].children[0].textContent, '<script>draft</script>');
  assert.equal(f.ctx.messagesEl.children[0].children[1].textContent, 'Сохраняется…');
  f.ack(); assert.equal(f.data.size, 0); assert.equal(f.api.entries().length, 0); assert.equal(f.timers.size, 0);
  assert.deepEqual(f.history, [[42, 0]]);
});

test('storage failure and oversize Unicode preserve the draft and send nothing', async () => {
  const f = fixture(); f.ctx.messageInput.value = 'draft';
  f.ctx.localStorage.setItem = () => { throw Error('quota'); };
  await f.api.send(); assert.equal(f.ctx.messageInput.value, 'draft'); assert.equal(f.frames.length, 0);
  const g = fixture(); g.ctx.messageInput.value = 'я'.repeat(750); await g.api.send();
  assert.equal(g.ctx.messageInput.value.length, 750); assert.equal(g.frames.length, 0); assert.equal(g.alerts.length, 1);
  const h = fixture(); h.ctx.messageInput.value = 'x' + '\n'.repeat(1400) + 'x'; await h.api.send();
  assert.equal(h.frames.length, 0); assert.equal(h.data.size, 0); assert.equal(h.alerts.length, 1);
});

test('lost ACK retries at most three times with the same ID and exposes a manual retry', () => {
  const f = fixture(); f.api.queue(42, 'hello'); const id = f.frames[0].client_message_id;
  f.advance(30000);
  assert.equal(f.frames.length, 3); assert.ok(f.frames.every(frame => frame.client_message_id === id));
  assert.equal(f.api.entries()[0].state, 'failed'); assert.equal(f.timers.size, 0);
  const retry = f.ctx.messagesEl.children[0].children.find(child => child.textContent === 'Повторить');
  retry.listeners.click(); assert.equal(f.frames.length, 4); assert.equal(f.frames[3].client_message_id, id);
  f.ack(); f.advance(60000); assert.equal(f.frames.length, 4);
});

test('restart and reconnect reuse the saved ID and attempt budget', () => {
  const f = fixture(); f.api.queue(42, 'saved queue');
  const id = f.frames[0].client_message_id;
  f.ctx.socket.readyState = 3; f.api.pause(); assert.equal(f.timers.size, 0);
  const g = fixture({ data: f.data }); g.api.flush();
  assert.equal(g.frames[0].client_message_id, id); assert.equal(g.api.entries()[0].attempts, 2);
  g.advance(30000); assert.equal(g.frames.length, 2); assert.equal(g.api.entries()[0].state, 'failed');
  const h = fixture({ data: f.data }); h.api.flush(); assert.equal(h.frames.length, 0);
});

test('offline enqueue survives until connection is ready; revoked session does not transmit', () => {
  const f = fixture(); f.ctx.socket.readyState = 3; f.api.queue(42, 'offline');
  assert.equal(f.frames.length, 0); assert.equal(f.api.entries()[0].attempts, 0);
  f.ctx.canReconnectChat = () => false; f.ctx.socket.readyState = 1; f.api.flush(); assert.equal(f.frames.length, 0);
  f.ctx.canReconnectChat = () => true; f.api.flush(); assert.equal(f.frames.length, 1);
});

test('correlated rejection stops retries and never renders as a successful message', () => {
  const f = fixture(); f.api.queue(42, 'denied');
  f.api.receipt({ type: 'error', chat_id: 42, client_message_id: f.frames[0].client_message_id, retryable: false, message: 'Нет доступа' });
  f.advance(30000); assert.equal(f.frames.length, 1); assert.equal(f.api.entries()[0].retryable, false);
  assert.equal(f.ctx.messagesEl.children[0].children.some(child => child.textContent === 'Повторить'), false);
  assert.deepEqual(f.history, []);
});

test('temporary error retries safely; mismatched and late receipts cannot remove another entry', () => {
  const f = fixture(); f.api.queue(42, 'retry'); const id = f.frames[0].client_message_id;
  f.api.receipt({ type: 'message_ack', chat_id: 99, client_message_id: id, id: '7' });
  assert.equal(f.api.entries().length, 1);
  f.api.receipt({ type: 'error', chat_id: 42, client_message_id: id, retryable: true });
  f.advance(1000); assert.equal(f.frames.length, 2); f.ack();
  f.api.receipt({ type: 'error', chat_id: 42, client_message_id: id, retryable: true });
  f.advance(30000); assert.equal(f.frames.length, 2); assert.equal(f.data.size, 0);
});

test('account and desktop server namespaces are isolated; malformed state is ignored', () => {
  const f = fixture(); f.api.queue(42, 'private draft');
  assert.equal(fixture({ data: f.data, account: '2' }).api.entries().length, 0);
  assert.equal(fixture({ data: f.data, server: 'https://two.invalid' }).api.entries().length, 0);
  f.data.set(f.api.prefix + 'bad', '{bad json');
  assert.equal(fixture({ data: f.data }).api.entries().length, 1);
});

test('file/image/voice sends enter the same durable queue', () => {
  const f = fixture();
  f.api.media('img', '/uploads/photo.png', 42); f.api.media('voice', '/uploads/voice.ogg', 42);
  f.api.file({ name: 'x:y.txt', size: 12, url: '/uploads/document' }, 42);
  assert.equal(f.data.size, 3); assert.equal(f.frames.length, 3);
  assert.equal(f.frames[2].content, '[file:x_y.txt:12]/uploads/document[/file]');
  assert.equal(new Set(f.frames.map(frame => frame.client_message_id)).size, 3);
});

test('old server is never retried as if it supported idempotency', () => {
  const f = fixture({ supported: false }); f.api.queue(42, 'safe'); f.advance(30000);
  assert.equal(f.frames.length, 0); assert.equal(f.api.entries()[0].state, 'failed');
  f.ctx.messageReceiptsSupported = true;
  f.ctx.messagesEl.children[0].children.find(child => child.textContent === 'Повторить').listeners.click();
  assert.equal(f.frames.length, 1);
});

test('queue is bounded to four in-flight requests and 100 saved messages', () => {
  const f = fixture();
  for (let index = 0; index < 100; index++) assert.equal(f.api.queue(42, 'm' + index), true);
  assert.equal(f.frames.length, 4); assert.equal(f.api.queue(42, 'overflow'), false);
  f.ack(); assert.equal(f.frames.length, 5);
});

test('two tabs do not overwrite entries; ACK removal cancels the other tab timer', () => {
  const a = fixture(), b = fixture({ data: a.data });
  a.api.queue(42, 'a'); b.api.queue(42, 'b'); assert.equal(a.data.size, 2);
  const frame = a.frames[0], key = a.api.prefix + frame.client_message_id;
  b.listeners.get('storage')({ key, newValue: a.data.get(key) });
  assert.equal(b.api.entries().length, 2);
  a.ack(); b.listeners.get('storage')({ key, newValue: null });
  assert.equal(b.api.entries().length, 1); assert.equal(b.timers.size, 1);
});

test('real WS handler consumes correlated errors before legacy UI wrappers', async () => {
  const f = fixture(), connections = [];
  class Socket {
    static OPEN = 1; static CONNECTING = 0; static CLOSING = 2; static CLOSED = 3;
    readyState = 0;
    constructor() { connections.push(this); }
    send(frame) { f.frames.push(JSON.parse(frame)); }
    close() { this.readyState = 3; }
  }
  Object.assign(f.ctx, { WebSocket: Socket, AbortController, token: 'session', authExpiredHandled: false,
    loadChats() {}, loadFriendRequests() {}, cleanupCall() {}, abortSessionRequests() {},
    apiFetch: async () => ({ ok: true, json: async () => ({ ticket: 'a'.repeat(43) }) }), legacyCalls: 0 });
  f.ctx.window.location = { protocol: 'http:', host: 'localhost' };
  f.ctx.localStorage.setItem('token', 'session');
  runInNewContext(compile('016-websocket.ts'), f.ctx);
  for (let index = 0; index < 10 && !connections[0]?.onopen; index++) await Promise.resolve();
  const connection = connections[0]; connection.readyState = 1; connection.onopen();
  // Represents the delayed group-permission wrapper observed in the full UI.
  runInNewContext('messageReceiptsSupported=true;handleIncomingMessage=()=>{legacyCalls++};', f.ctx);
  f.api.queue(42, 'denied');
  connection.onmessage({ data: JSON.stringify({ type: 'error', chat_id: 42,
    client_message_id: f.frames[0].client_message_id, retryable: false, message: 'denied' }) });
  assert.equal(f.ctx.legacyCalls, 0); assert.equal(f.api.entries()[0].state, 'failed');
  connection.onmessage({ data: JSON.stringify({ type: 'private', id: 9, chat_id: 42, content: 'ordinary' }) });
  assert.equal(f.ctx.legacyCalls, 1);
});
