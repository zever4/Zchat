import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import { webcrypto } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const source = readFileSync(new URL('../desktop/renderer/desktop-runtime.js', import.meta.url), 'utf8');
function storage(entries = []) { const map = new Map(entries); return { get length() { return map.size; }, key: index => [...map.keys()][index], getItem: k => map.get(k) ?? null, setItem: (k, v) => map.set(k, v), removeItem: k => map.delete(k), clear: () => map.clear() }; }
async function fixture(server = 'http://127.0.0.1:18080', previous = server, entries = [], options = {}) {
  const calls = [], requests = [], channels = [], navigation = [], imageResult = { response: null };
  let context = 1;
  const listeners = new Map();
  let boot;
  const ready = new Promise(resolve => { boot = resolve; });
  const ctx = {
    URL, Request, Response, Headers, AbortController, DOMException, EventTarget, Event, MessageEvent, TextEncoder, TextDecoder, DataView, Uint8Array, ArrayBuffer, Blob, File,
    CloseEvent: class extends Event { constructor(type, init) { super(type); Object.assign(this, init); } },
    crypto: webcrypto, location: new URL(options.chat ? 'http://tauri.localhost/chat.html' : 'http://tauri.localhost/login.html'),
    performance: { now: options.now || (() => performance.now()) },
    addEventListener: (type, fn) => { const group = listeners.get(type) || []; group.push(fn); listeners.set(type, group); },
    localStorage: storage([['zchat_desktop_server', previous], ['token', 'old-session'], ['avatar_url', '/uploads/avatars/old.png'], ...entries]), sessionStorage: storage(),
    document: { addEventListener() {}, createElement: () => ({}), body: { appendChild: boot, replaceChildren: boot } },
    fetch: async (url, options) => { requests.push({ url, options }); return new Response('{}'); },
    __TAURI__: { core: { Channel: class { constructor() { channels.push(this); } }, invoke: async (name, args, invokeOptions) => {
      calls.push({ name, args, options: invokeOptions });
      if (options.invoke) { const result = await options.invoke(name, args); if (result !== undefined) return result; }
      if (name === 'get_config') return { serverUrl: server, epoch: 7, e2eeCompiled: !!options.e2ee, e2eeContext: context };
      if (name === 'e2ee_session') return { context: ++context, phase: args.token ? 'starting' : 'idle', error: null, replyLease: String(calls.length) };
      if (name === 'e2ee_setup' || name === 'e2ee_retry' || name === 'e2ee_replace') return { context: ++context, phase: 'starting', error: null, replyLease: String(calls.length) };
      if (name === 'e2ee_status') return { context, phase: 'failed', error: 'not_configured', replyLease: String(calls.length) };
      if (name === 'prepare_image') return imageResult.response;
    } } },
  };
  ctx.location.replace = target => { navigation.push(target); boot(); };
  ctx.window = ctx;
  runInNewContext(source, ctx);
  if (options.beforeReady) await options.beforeReady({ ctx, calls, listeners });
  await ready;
  return { ctx, calls, requests, channels, imageResult, listeners, navigation };
}

const photoChat = '11111111-1111-4111-8111-111111111111';
const photoId = '22222222-2222-4222-8222-222222222222';
const photoTarget = () => ({ conversationId: photoChat, messageId: photoId });
const privatePhoto = (file = new File([new Uint8Array([1, 2, 3, 4])], 'камера.png', { type: 'image/png' })) => ({ ...photoTarget(), file, caption: 'PRIVATE-CAPTION', choice: { mode: 'clean', quality: null, maxDimension: 0 } });
function photoFrame(bytes) {
  assert.ok(bytes instanceof Uint8Array);
  const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
  return { metadata: JSON.parse(new TextDecoder().decode(bytes.subarray(4, 4 + length))), source: bytes.subarray(4 + length) };
}
function photoReply(context, target, state = { kind: 'prepared', cryptoEpoch: '9007199254740993' }) { return { context, attachment: { ...target, ...state }, replyLease: '900' }; }
async function photoFixture(handler = () => undefined) {
  return fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: async (name, args) => {
    const override = await handler(name, args); if (override !== undefined) return override;
    if (name === 'e2ee_attachment_input') return photoReply(args.context, args.query, { kind: 'vacant' });
    if (name === 'e2ee_prepare_attachment') { const { metadata: m } = photoFrame(args); return photoReply(m.context, { conversationId: m.conversationId, messageId: m.messageId }); }
  } });
}

test('E2EE photo uses bounded binary frames, explicit native context and independent retained lookup', async () => {
  const f = await photoFixture(); const api = f.ctx.zchatDesktop.e2ee;
  for (const choice of [{ mode: 'clean', quality: null, maxDimension: 0 }, { mode: 'clean', quality: 73, maxDimension: 1920 }, { mode: 'original', metadataWarningAcknowledged: true }]) {
    const message = { ...privatePhoto(), choice }; const reply = await api.preparePhoto(message, 2);
    assert.equal(reply.attachment.kind, 'prepared'); assert.equal(reply.attachment.cryptoEpoch, '9007199254740993');
    const call = f.calls.filter(c => c.name === 'e2ee_prepare_attachment').at(-1); const { metadata, source } = photoFrame(call.args);
    assert.deepEqual(Object.keys(metadata).sort(), ['caption', 'choice', 'context', 'conversationId', 'epoch', 'filename', 'messageId', 'requestId']);
    assert.equal(metadata.epoch, 7); assert.equal(metadata.context, 2); assert.equal(metadata.filename, 'камера.png');
    assert.deepEqual(metadata.choice, choice); assert.deepEqual([...source], [1, 2, 3, 4]);
    assert.equal(metadata.messageId, message.messageId); assert.notEqual(metadata.requestId, metadata.messageId);
  }
  await api.attachmentInput(photoTarget(), 2);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_prepare_attachment').length, 3);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_attachment_input').length, 4);
});
test('E2EE photo rejects stale contexts, default builds and invalid source/consent before IPC', async () => {
  const f = await photoFixture(); const api = f.ctx.zchatDesktop.e2ee;
  for (const ctx of [undefined, null, '2', 1, 2.5, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(api.preparePhoto(privatePhoto(), ctx), e => e.name === 'AbortError');
    await assert.rejects(api.attachmentInput(photoTarget(), ctx), e => e.name === 'AbortError');
  }
  for (const input of [
    { ...privatePhoto(), path: '/private.png' }, { ...privatePhoto(), photoMetadataStripped: true },
    { ...privatePhoto(), choice: { mode: 'original' } }, { ...privatePhoto(), choice: { mode: 'original', metadataWarningAcknowledged: false } },
    { ...privatePhoto(), choice: { mode: 'clean', quality: 0, maxDimension: 0 } }, { ...privatePhoto(), choice: { mode: 'clean', quality: 90, maxDimension: 63 } },
    { ...privatePhoto(), caption: 'я'.repeat(1001) }, privatePhoto(new File([], 'empty.png')),
    privatePhoto(new File([new Uint8Array(20 * 1024 * 1024 + 1)], 'large.png')),
  ]) await assert.rejects(api.preparePhoto(input, 2));
  assert.equal(f.calls.some(c => c.name === 'e2ee_prepare_attachment' || c.name === 'e2ee_attachment_input'), false);
  const off = await fixture(); await assert.rejects(off.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(), 1), e => e.name === 'AbortError');
});
test('E2EE photo refuses occupied UUIDs before reading source and preserves retained status', async () => {
  for (const state of [{ kind: 'prepared', cryptoEpoch: '0' }, { kind: 'outgoing', cryptoEpoch: '1', storedOnServer: true, sealedUnsent: false }]) {
    const f = await photoFixture((name, args) => name === 'e2ee_attachment_input' ? photoReply(args.context, args.query, state) : undefined);
    class Unread extends File { async arrayBuffer() { assert.fail('known UUID must not read source'); } }
    await assert.rejects(f.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(new Unread(['x'], 'photo.png')), 2), e => e === 'conflict');
    assert.equal((await f.ctx.zchatDesktop.e2ee.attachmentInput(photoTarget(), 2)).attachment.kind, state.kind);
    assert.equal(f.calls.some(c => c.name === 'e2ee_prepare_attachment'), false);
  }
});
test('E2EE photo binds mutable metadata before file reads and admits only one pending source', async () => {
  const f = await photoFixture(); let enter, release; const gate = new Promise(resolve => { enter = resolve; });
  class Delayed extends File { arrayBuffer() { enter(); return new Promise(resolve => { release = resolve; }); } }
  const message = privatePhoto(new Delayed(['data'], 'original.png')); const pending = f.ctx.zchatDesktop.e2ee.preparePhoto(message, 2); await gate;
  await assert.rejects(f.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(), 2), e => e === 'busy');
  message.conversationId = photoId; message.messageId = photoChat; message.caption = 'changed'; message.choice.quality = 10;
  Object.defineProperty(message.file, 'name', { value: 'changed.png' });
  release(new Uint8Array([1, 2, 3, 4]).buffer); await pending;
  const { metadata: m } = photoFrame(f.calls.find(c => c.name === 'e2ee_prepare_attachment').args);
  assert.equal(m.filename, 'original.png'); assert.equal(m.conversationId, photoChat); assert.equal(m.messageId, photoId); assert.equal(m.caption, 'PRIVATE-CAPTION'); assert.equal(m.choice.quality, null);
});
test('E2EE photo drops file reads after account/context changes without submitting bytes', async () => {
  for (const change of ['account', 'context']) {
    const f = await photoFixture(); let enter, release; const gate = new Promise(resolve => { enter = resolve; });
    class Delayed extends File { arrayBuffer() { enter(); return new Promise(resolve => { release = resolve; }); } }
    const pending = f.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(new Delayed(['x'], 'photo.png')), 2); await gate;
    if (change === 'account') f.ctx.localStorage.setItem('token', 'another-account'); else await f.ctx.zchatDesktop.e2ee.retry();
    release(new Uint8Array([1]).buffer); await assert.rejects(pending, e => e.name === 'AbortError');
    assert.equal(f.calls.some(c => c.name === 'e2ee_prepare_attachment'), false);
  }
});
test('E2EE photo validates native reply shape/identity and ACKs discarded replies', async () => {
  for (const change of [v => { v.attachment.messageId = photoChat; }, v => { v.attachment.key = 'forbidden'; }, v => { v.attachment.filename = 'private.png'; }, v => { v.attachment.cryptoEpoch = 9007199254740992; }, v => { v.attachment = { ...photoTarget(), kind: 'vacant' }; }]) {
    const f = await photoFixture((name, args) => { if (name !== 'e2ee_prepare_attachment') return; const { metadata: m } = photoFrame(args); const reply = photoReply(m.context, photoTarget()); change(reply); return reply; });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(), 2), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 3);
  }
});
test('E2EE photo unknown result resolves only by UUID without automatic preprocessing or new Send', async () => {
  let prepared = false;
  const f = await photoFixture((name, args) => {
    if (name === 'e2ee_prepare_attachment') { prepared = true; throw 'native_wait'; }
    if (name === 'e2ee_attachment_input' && prepared) return photoReply(args.context, args.query);
  });
  await assert.rejects(f.ctx.zchatDesktop.e2ee.preparePhoto(privatePhoto(), 2), e => e === 'native_wait');
  assert.equal((await f.ctx.zchatDesktop.e2ee.attachmentInput(photoTarget(), 2)).attachment.kind, 'prepared');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_prepare_attachment').length, 1);
});

const privateFile = (file = new File([new Uint8Array([0, 255, 42])], 'архив.zip', { type: 'application/zip' })) => ({
  ...photoTarget(), file, caption: 'PRIVATE-CAPTION', expectedEpoch: '9007199254740993',
});

test('E2EE file sends exact bytes and binds the canonical crypto epoch', async () => {
  const f = await photoFixture();
  const api = f.ctx.zchatDesktop.e2ee;
  const reply = await api.prepareFile(privateFile(), 2);
  assert.equal(reply.attachment.kind, 'prepared');
  const call = f.calls.find(c => c.name === 'e2ee_prepare_attachment');
  const { metadata, source } = photoFrame(call.args);
  assert.equal(metadata.filename, 'архив.zip');
  assert.equal(metadata.caption, 'PRIVATE-CAPTION');
  assert.deepEqual(metadata.choice, { mode: 'file', expectedEpoch: '9007199254740993' });
  assert.deepEqual([...source], [0, 255, 42]);
  assert.equal(Object.hasOwn(metadata, 'mimeType'), false);
  await assert.rejects(api.prepareFile({ ...privateFile(), expectedEpoch: '1' }, 2), { name: 'AbortError' });
  await assert.rejects(api.prepareFile({ ...privateFile(), expectedEpoch: '01' }, 2), { name: 'TypeError' });
  class TooLarge extends File { get size() { return 50 * 1024 * 1024 + 1; } }
  await assert.rejects(api.prepareFile(privateFile(new TooLarge(['x'], 'big.zip')), 2), { name: 'TypeError' });
  assert.equal(f.calls.filter(c => c.name === 'e2ee_prepare_attachment').length, 2);
});

test('E2EE file and photo share one source read, cancellation during the file read prevents IPC', async () => {
  const f = await photoFixture();
  const api = f.ctx.zchatDesktop.e2ee;
  let began, complete;
  const started = new Promise(resolve => { began = resolve; });
  class Slow extends File {
    arrayBuffer() { began(); return new Promise(resolve => { complete = resolve; }); }
  }
  const controller = new AbortController();
  const pending = api.prepareFile(privateFile(new Slow(['x'], 'a.bin')), 2, controller.signal);
  await started;
  await assert.rejects(api.preparePhoto(privatePhoto(), 2), e => e === 'busy');
  controller.abort();
  complete(new Uint8Array([1]).buffer);
  await assert.rejects(pending, { name: 'AbortError' });
  assert.equal(f.calls.filter(c => c.name === 'e2ee_prepare_attachment').length, 0);
});

test('E2EE voice uses bounded raw prepare and binds binary audio to the local attachment', async () => {
  const audio = new Uint8Array(readFileSync(new URL('../rust-processor/tests/fixtures/webview2-voice-silence.webm', import.meta.url)));
  let rewrite = value => value;
  const f = await photoFixture((name, args) => {
    if (name !== 'e2ee_local_attachment') return;
    const header = rewrite({ context: args.context, target: args.target, replyLease: '980', result: { kind: 'voice', mimeType: 'audio/webm' } });
    const encoded = new TextEncoder().encode(JSON.stringify(header));
    const frame = new Uint8Array(4 + encoded.length + audio.length);
    new DataView(frame.buffer).setUint32(0, encoded.length, true);
    frame.set(encoded, 4); frame.set(audio, 4 + encoded.length);
    return frame.buffer;
  });
  const api = f.ctx.zchatDesktop.e2ee;
  const source = new File([audio], 'voice.webm', { type: 'audio/webm' });
  await api.prepareVoice(privateFile(source), 2);
  const framed = photoFrame(f.calls.find(c => c.name === 'e2ee_prepare_attachment').args);
  assert.deepEqual(framed.metadata.choice, { mode: 'voice', expectedEpoch: '9007199254740993' });
  assert.deepEqual(framed.source, audio);
  class TooLarge extends File { get size() { return 10 * 1024 * 1024 + 1; } }
  await assert.rejects(api.prepareVoice(privateFile(new TooLarge(['x'], 'voice.webm')), 2), { name: 'TypeError' });
  const target = { ...photoTarget(), generationId: null };
  const reply = await api.listenAttachment(target, 2);
  assert.equal(reply.result.mimeType, 'audio/webm');
  assert.deepEqual(reply.audio, audio);
  for (rewrite of [v => ({ ...v, result: { ...v.result, mimeType: 'video/webm' } }), v => ({ ...v, target: { ...v.target, messageId: photoChat } })]) {
    await assert.rejects(api.listenAttachment(target, 2), { name: 'AbortError' });
  }
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack' && c.args.replyLease === '980').length, 3);
});

test('E2EE download binds context and both IDs and rejects metadata outside the reply contract', async () => {
  let override = {};
  const f = await photoFixture((name, args) => name === 'e2ee_download_attachment' ? {
    context: args.context, attachment: { ...args.target, ...override }, replyLease: '900',
  } : undefined);
  const api = f.ctx.zchatDesktop.e2ee;
  await assert.rejects(api.downloadAttachment(photoTarget(), 1), { name: 'AbortError' });
  await assert.rejects(api.downloadAttachment({ ...photoTarget(), generationId: photoId }, 2), { name: 'TypeError' });
  assert.equal(f.calls.filter(c => c.name === 'e2ee_download_attachment').length, 0);
  assert.equal((await api.downloadAttachment(photoTarget(), 2)).attachment.messageId, photoId);
  const request = f.calls.find(c => c.name === 'e2ee_download_attachment');
  assert.deepEqual(Object.keys(request.args.target).sort(), ['conversationId', 'messageId']);
  for (override of [{ conversationId: photoId }, { messageId: photoChat }, { key: 'private' }]) {
    await assert.rejects(api.downloadAttachment(photoTarget(), 2), { name: 'AbortError' });
  }
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack' && c.args.replyLease === '900').length, 4);
});

test('cancelled E2EE download cancels its exact native lease and ACKs a late reply', async () => {
  let complete;
  const f = await photoFixture((name, args) => name === 'e2ee_download_attachment' ? new Promise(resolve => {
    complete = () => resolve({ context: args.context, attachment: args.target, replyLease: '901' });
  }) : undefined);
  const api = f.ctx.zchatDesktop.e2ee, controller = new AbortController();
  const pending = api.downloadAttachment(photoTarget(), 2, controller.signal);
  controller.abort();
  const request = f.calls.find(c => c.name === 'e2ee_download_attachment');
  const cancel = f.calls.find(c => c.name === 'e2ee_cancel');
  assert.equal(cancel.args.requestId, request.args.requestId);
  assert.equal(cancel.args.context, request.args.context);
  complete();
  await assert.rejects(pending, { name: 'AbortError' });
  await assert.rejects(api.downloadAttachment(photoTarget(), 2, controller.signal), { name: 'AbortError' });
  assert.equal(f.calls.filter(c => c.name === 'e2ee_download_attachment').length, 1);
  assert.equal(f.calls.find(c => c.name === 'e2ee_ack' && c.args.replyLease === '901').args.requestId, request.args.requestId);
});

test('local attachment SDK scopes native export and only accepts bounded PNG previews', async () => {
  const png = readFileSync(new URL('../src-tauri/icons/32x32.png', import.meta.url)).toString('base64');
  const archived = { ...photoTarget(), generationId: '33333333-3333-4333-8333-333333333333' };
  let patch = value => value;
  const f = await photoFixture((name, args) => name === 'e2ee_local_attachment' ? patch({
    context: args.context, target: args.target, result: args.operation.action === 'preview' ? { kind: 'preview', png } : { kind: 'exported' }, replyLease: '910',
  }) : undefined);
  const api = f.ctx.zchatDesktop.e2ee;
  await assert.rejects(api.previewAttachment(archived, 1), { name: 'AbortError' });
  await assert.rejects(api.exportAttachment({ ...archived, path: 'C:/arbitrary' }, 2, 'a.png'), { name: 'TypeError' });
  assert.equal(f.calls.filter(c => c.name === 'e2ee_local_attachment').length, 0);
  assert.equal((await api.previewAttachment(archived, 2)).result.png, png);
  assert.equal((await api.exportAttachment({ ...archived, generationId: null }, 2, 'фото.png')).result.kind, 'exported');
  const exportRequest = f.calls.filter(c => c.name === 'e2ee_local_attachment').at(-1).args;
  assert.equal(exportRequest.operation.suggestedName, 'фото.png');
  assert.equal(exportRequest.target.generationId, null);
  for (patch of [
    v => ({ ...v, target: { ...v.target, generationId: null } }),
    v => ({ ...v, result: { ...v.result, png: 'PHN2Zz4=' } }),
    v => ({ ...v, result: { ...v.result, png: png.repeat(1000) } }),
    v => ({ ...v, result: { ...v.result, url: 'file:///local' } }),
  ]) await assert.rejects(api.previewAttachment(archived, 2), { name: 'AbortError' });
});

test('API traffic uses only native protocol; credentials never follow an external URL', async () => {
  const f = await fixture();
  await f.ctx.fetch('/api/profile', { headers: { Authorization: 'Bearer synthetic-only' } });
  assert.equal(f.requests[0].url, 'http://zchat-api.localhost/7/api/profile');
  assert.equal(f.requests[0].options.headers.get('authorization'), 'Bearer synthetic-only');
  assert.equal(f.requests[0].options.redirect, 'error');
  assert.equal(f.requests[0].options.credentials, 'omit');
  await assert.rejects(f.ctx.fetch('https://evil.test/api/profile'), /configured server/);
  await assert.rejects(f.ctx.fetch('https://api.giphy.com/v1/gifs/search', { headers: { Authorization: 'Bearer synthetic-only' } }), /credentials/);
  assert.equal(f.requests.length, 1);
});

test('microphone recovery invokes only the scoped command and preserves account storage', async () => {
  const {ctx,calls}=await fixture();
  await ctx.zchatDesktop.resetMicrophonePermission();
  const call=calls.find(c=>c.name==='reset_microphone_permission');
  assert.ok(call);assert.equal(call.args,undefined);
  assert.equal(ctx.localStorage.getItem('token'),'old-session');
  assert.equal(ctx.localStorage.getItem('zchat_desktop_server'),'http://127.0.0.1:18080');
});

test('switching server clears its predecessor session and caches; reload preserves current login', async () => {
  assert.equal((await fixture()).ctx.localStorage.getItem('token'), 'old-session');
  const f = await fixture('http://127.0.0.1:28081', 'http://old.test');
  assert.equal(f.ctx.localStorage.getItem('token'), null);
  assert.equal(f.ctx.localStorage.getItem('avatar_url'), null);
  assert.equal(f.ctx.localStorage.getItem('zchat_desktop_server'), 'http://127.0.0.1:28081');
});

test('switching server preserves namespaced pending queues while removing credentials and unrelated caches', async () => {
  const queue = 'zchat_outbox_v1:' + encodeURIComponent('https://old.test') + ':1:message-id';
  const f = await fixture('http://127.0.0.1:18080', 'https://old.test', [[queue, 'unsent'], ['other_cache', 'private']]);
  assert.equal(f.ctx.localStorage.getItem(queue), 'unsent');
  assert.equal(f.ctx.localStorage.getItem('token'), null);
  assert.equal(f.ctx.localStorage.getItem('other_cache'), null);
});

test('avatar paths are versioned and cannot name an external or private attachment endpoint', async () => {
  const { ctx } = await fixture();
  assert.equal(ctx.zchatDesktop.resourceURL('/uploads/avatar_test.png'), 'http://zchat-api.localhost/7/uploads/avatar_test.png');
  for (const value of ['https://evil.test/uploads/avatar_test.png', '/uploads/files/private.txt', '/uploads/avatar_test.png?token=x']) {
    assert.equal(ctx.zchatDesktop.resourceURL(value), '');
  }
});

test('native sockets preserve chat events, binary signaling and acknowledgements', async () => {
  const f = await fixture();
  const ws = new f.ctx.WebSocket('ws://tauri.localhost/ws?ticket=synthetic-only');
  const received = [];
  ws.onmessage = event => received.push(event.data);
  const channel = f.channels[0];
  channel.onmessage({ type: 'open' });
  assert.equal(ws.readyState, ws.OPEN);
  channel.onmessage({ type: 'text', data: 'hello', sequence: 1 });
  ws.binaryType = 'arraybuffer';
  channel.onmessage({ type: 'binary', data: [1, 2, 3], sequence: 2 });
  assert.equal(received[0], 'hello');
  assert.deepEqual([...new Uint8Array(received[1])], [1, 2, 3]);
  assert.equal(f.calls.filter(c => c.name === 'socket_ack').length, 2);
  ws.send('reply');
  assert.ok(f.calls.some(c => c.name === 'socket_send' && c.args.binary === false));
  channel.onmessage({ type: 'close', code: 1000 });
  assert.equal(ws.readyState, ws.CLOSED);
});

test('unconfigured socket destinations are rejected before invoking Rust', async () => {
  const f = await fixture();
  assert.throws(() => new f.ctx.WebSocket('wss://evil.test/ws'), /destination/);
  assert.ok(!f.calls.some(c => c.name === 'socket_connect'));
});

test('desktop calls use the selected Go gateway, including TLS and a nondefault port', async () => {
  for (const server of ['http://127.0.0.1:28081', 'https://chat.example:9443']) {
    const f = await fixture(server);
    const gateway = f.ctx.zchatDesktop.callGatewayURL;
    assert.equal(gateway, server.replace(/^http/, 'ws') + '/rtc/');
    new f.ctx.WebSocket(gateway + 'rtc/v1?access_token=synthetic-only');
    const connection = f.calls.find(c => c.name === 'socket_connect');
    assert.equal(connection.args.path, '/rtc/rtc/v1?access_token=synthetic-only');
    assert.equal(connection.args.epoch, 7);
    const validate = server + '/rtc/rtc/v1/validate?access_token=synthetic-only';
    await f.ctx.fetch(validate);
    assert.equal(f.requests[0].url, 'http://zchat-api.localhost/7/rtc/rtc/v1/validate?access_token=synthetic-only');
    assert.throws(() => new f.ctx.WebSocket('ws://localhost:28080/rtc/rtc'), /destination/);
  }
});

function imagePacket(bytes, fields = {}) {
  const metadata = new TextEncoder().encode(JSON.stringify({ version: 1, mime: 'image/jpeg', extension: 'jpg',
    width: 2, height: 2, changed: true, reason: 'compressed', ...fields }));
  const packet = new Uint8Array(4 + metadata.length + bytes.length);
  new DataView(packet.buffer).setUint32(0, metadata.length, true);
  packet.set(metadata, 4); packet.set(bytes, 4 + metadata.length);
  return packet.buffer;
}

test('image processing uses binary IPC and the actual output MIME and filename', async () => {
  const f = await fixture(), input = new File([new Uint8Array([1,2,3])], 'photo.png', { type: 'image/png' });
  f.imageResult.response = imagePacket([0xff,0xd8,0xff,1]);
  const result = await f.ctx.zchatDesktop.prepareImage(input, 80, 1600);
  const call = f.calls.find(c => c.name === 'prepare_image');
  assert.ok(call.args instanceof Uint8Array);
  assert.deepEqual([...call.args], [1,2,3]);
  assert.equal(call.options.headers['x-zchat-quality'], '80');
  assert.equal(call.options.headers['x-zchat-max-dimension'], '1600');
  assert.equal(result.file.name, 'photo.jpg'); assert.equal(result.file.type, 'image/jpeg');
  assert.deepEqual([...new Uint8Array(await result.file.arrayBuffer())], [0xff,0xd8,0xff,1]);
  assert.equal(f.requests.length, 0, 'Preparing an image must not upload it');
});

test('unchanged output retains the exact original File and its bytes', async () => {
  const f = await fixture(), original = new File([new Uint8Array([1,2,3])], 'original.png', { type: 'image/png' });
  f.imageResult.response = imagePacket([1,2,3], { changed: false, reason: 'original_smaller', mime: 'image/png', extension: 'png' });
  const result = await f.ctx.zchatDesktop.prepareImage(original, 80, 0);
  assert.equal(result.file, original);
  assert.deepEqual([...new Uint8Array(await result.file.arrayBuffer())], [1,2,3]);
});

test('invalid processing input is rejected before IPC and malformed responses cannot become files', async () => {
  const f = await fixture(), file = new File(['test'], 'photo.png', { type: 'image/png' });
  await assert.rejects(f.ctx.zchatDesktop.prepareImage(file, 0, 0), /параметры/);
  const oversized = new File(['test'], 'large.png'); Object.defineProperty(oversized, 'size', { value: 20*1024*1024+1 });
  await assert.rejects(f.ctx.zchatDesktop.prepareImage(oversized, 80, 0), /20 МБ/);
  assert.ok(!f.calls.some(c => c.name === 'prepare_image'));
  f.imageResult.response = imagePacket([1,2,3], { mime: 'text/html', extension: 'html' });
  await assert.rejects(f.ctx.zchatDesktop.prepareImage(file, 80, 0), /Некорректный/);
  f.imageResult.response = new Uint8Array([255,255,255,255,1]).buffer;
  await assert.rejects(f.ctx.zchatDesktop.prepareImage(file, 80, 0), /Некорректный/);
});

test('Tauri internal IPC preserves raw body and headers instead of falling back to JSON', async () => {
  const f = await fixture(), bytes = new Uint8Array([1,2,3]);
  const options = { method: 'POST', body: bytes, headers: new Headers({ 'Content-Type': 'application/octet-stream', 'Tauri-Callback': '42' }) };
  await f.ctx.fetch('http://ipc.localhost/prepare_image', options);
  assert.equal(f.requests[0].url, 'http://ipc.localhost/prepare_image');
  assert.equal(f.requests[0].options, options);
  assert.equal(f.requests[0].options.body, bytes);
  assert.equal(f.requests[0].options.headers.get('x-zchat-request-id'), null);
  await f.ctx.fetch('ipc://localhost/prepare_image', options);
  assert.equal(f.requests[1].url, 'ipc://localhost/prepare_image');
  await assert.rejects(f.ctx.fetch('http://ipc.localhost.evil.test/prepare_image', options), /configured server/);
});


test('experimental desktop binds only a token and native contexts, acknowledges replies, ordinary boot survives missing E2EE config', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  const bind = f.calls.find(c => c.name === 'e2ee_session');
  assert.equal(bind.args.token, 'old-session');
  assert.equal(bind.args.context, 1);
  assert.equal(bind.args.epoch, 7);
  assert.deepEqual(Object.keys(bind.args).sort(), ['context', 'epoch', 'requestId', 'token']);
  const status = await f.ctx.zchatDesktop.e2ee.status();
  assert.equal(status.phase, 'failed');
  assert.equal(status.error, 'not_configured');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  assert.equal(f.ctx.localStorage.getItem('token'), 'old-session');
  await f.ctx.fetch('/api/profile');
  assert.equal(f.requests.length, 1);
});

test('default desktop never sends a token to the experimental runtime', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true });
  assert.equal(f.ctx.zchatDesktop.e2ee.compiled, false);
  assert.equal((await f.ctx.zchatDesktop.e2ee.status()).phase, 'disabled');
  await f.ctx.zchatDesktop.e2ee.endSession();
  assert.equal(f.calls.some(c => c.name.startsWith('e2ee_')), false);
});

test('late native status is acknowledged but rejected after session retirement', async () => {
  let reply, entered;
  const pending = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_status') { entered(); return new Promise(resolve => { reply = () => resolve({ context: 2, phase: 'active', replyLease: '1234' }); }); }
  } });
  const status = f.ctx.zchatDesktop.e2ee.status();
  await pending;
  await f.ctx.zchatDesktop.e2ee.endSession();
  reply();
  await assert.rejects(status, error => error.name === 'AbortError');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 3);
  assert.equal(f.ctx.localStorage.getItem('token'), 'old-session');
});

test('pagehide and account change retire only the captured native session once', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  f.ctx.localStorage.setItem('token', 'new-session');
  f.listeners.get('storage')[0]({ key: 'token' });
  f.listeners.get('pagehide')[0]();
  await f.ctx.zchatDesktop.e2ee.endSession();
  assert.equal(f.calls.filter(c => c.name === 'e2ee_session' && c.args.token === null).length, 1);
  await assert.rejects(f.ctx.zchatDesktop.e2ee.status(), error => error.name === 'AbortError');
  assert.equal(f.ctx.localStorage.getItem('token'), 'new-session');
});


test('session retirement while an ACK is pending also invalidates the completed status', async () => {
  let request, release, entered;
  const ack = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_status') request = args.requestId;
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = () => resolve(true); }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.status();
  await ack;
  await f.ctx.zchatDesktop.e2ee.endSession();
  release();
  await assert.rejects(pending, error => error.name === 'AbortError');
});


const authCode = ts.transpile(readFileSync(new URL('../web/src/chat/004-api-helper.ts', import.meta.url), 'utf8').split('async function apiFetch')[0], { target: ts.ScriptTarget.ES2022 });
function authFixture(native) {
  const data = storage([['token', 'old-session']]);
  let stopped = 0, cleared = 0;
  const ctx = { authExpiredHandled: false, token: 'old-session', navigator: {}, localStorage: data, sessionStorage: storage(),
    SETTINGS_CACHE_KEY: 'settings', NOTIFICATIONS_STORAGE_KEY: 'notifications',
    leaveE2eeView: () => { cleared++; },
    stopChatConnection: () => { stopped++; }, window: { location: { href: '/chat.html' }, zchatDesktop: { e2ee: native } } };
  runInNewContext(authCode + ';this.expire = handleAuthExpired;', ctx);
  return { ctx, stopped: () => stopped, cleared: () => cleared };
}
test('desktop auth expiry stops chat immediately and awaits native retirement before clearing credentials', async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const f = authFixture({ compiled: true, endSession: () => pending });
  f.ctx.expire('session ended');
  assert.equal(f.cleared(), 1);
  assert.equal(f.stopped(), 1);
  assert.equal(f.ctx.localStorage.getItem('token'), 'old-session');
  assert.equal(f.ctx.window.location.href, '/chat.html');
  release(); await pending;
  assert.equal(f.ctx.localStorage.getItem('token'), null);
  assert.equal(f.ctx.window.location.href, '/login.html');
  assert.equal(f.ctx.sessionStorage.getItem('zchat_login_notice'), 'session ended');
});
test('a new login during native retirement preserves the new credentials', async () => {
  let release;
  const pending = new Promise(resolve => { release = resolve; });
  const f = authFixture({ compiled: true, endSession: () => pending });
  f.ctx.expire(); f.ctx.localStorage.setItem('token', 'new-session');
  release(); await pending;
  assert.equal(f.ctx.localStorage.getItem('token'), 'new-session');
});
test('browser and default desktop auth expiry retain immediate credential cleanup', () => {
  const f = authFixture({ compiled: false, endSession: () => { throw Error('disabled feature invoked'); } });
  f.ctx.expire();
  assert.equal(f.ctx.localStorage.getItem('token'), null);
  assert.equal(f.ctx.window.location.href, '/login.html');
});


test('setup and retry use current CAS while retirement keeps the pre-bind authentication lease', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  await f.ctx.zchatDesktop.e2ee.setupDevice();
  await f.ctx.zchatDesktop.e2ee.retry();
  assert.equal(f.calls.find(c => c.name === 'e2ee_setup').args.context, 2);
  assert.equal(f.calls.find(c => c.name === 'e2ee_retry').args.context, 3);
  await f.ctx.zchatDesktop.e2ee.endSession();
  assert.equal(f.calls.find(c => c.name === 'e2ee_session' && c.args.token === null).args.context, 1);
  await assert.rejects(f.ctx.zchatDesktop.e2ee.setupDevice(), error => error.name === 'AbortError');
});
test('bound status recovers an unknown operation reply without silently repeating setup', async () => {
  let lost = true;
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_setup' && lost) { lost = false; throw Error('native_wait'); }
    if (name === 'e2ee_status') { assert.equal(args.context, 1); return { context: 3, phase: 'failed', error: 'remote', replyLease: '42' }; }
    if (name === 'e2ee_retry') { assert.equal(args.context, 3); return { context: 4, phase: 'starting', replyLease: '43' }; }
  } });
  await assert.rejects(f.ctx.zchatDesktop.e2ee.setupDevice());
  assert.equal((await f.ctx.zchatDesktop.e2ee.status()).context, 3);
  await f.ctx.zchatDesktop.e2ee.retry();
  assert.equal(f.calls.filter(c => c.name === 'e2ee_setup').length, 1);
});

const settingsCode = ts.transpile(readFileSync(new URL('../web/src/chat/033-e2ee-settings.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
function settingsFixture({ compiled = true, phase = 'setup_required', status, setup, replace } = {}) {
  const elements = new Map();
  for (const name of ['settingsModal', 'settingsTabSecurity', 'desktopE2eeSettings', 'desktopE2eeStatus', 'desktopE2eeError', 'desktopE2eeSetup', 'desktopE2eeRetry', 'desktopE2eeReplacement', 'desktopE2eeTarget', 'desktopE2eePassword', 'desktopE2eeConfirm', 'desktopE2eeReplace']) elements.set(name, {
    hidden: name.startsWith('desktop') && name !== 'desktopE2eeReplace', disabled: false, value: '', checked: false, style: { display: 'flex' }, textContent: '', listeners: {},
    classList: { contains: value => value === 'active' }, addEventListener(type, fn) { this.listeners[type] = fn; },
  });
  const timers = new Map(), calls = { status: 0, setup: 0, retry: 0, replace: 0 };
  let current = { context: 2, phase }, next = 0;
  const native = { compiled, status: async () => { calls.status++; return status ? status() : current; },
    setupDevice: async () => { calls.setup++; if (setup) return setup(); current = { context: 3, phase: 'active' }; return current; },
    replaceDevice: async (password, confirmed) => { calls.replace++; return replace(password, confirmed); },
    retry: async () => { calls.retry++; current = { context: 3, phase: 'active' }; return current; } };
  const ctx = { authExpiredHandled: false, token: 'session', localStorage: storage([['token', 'session']]), window: { zchatDesktop: { e2ee: native } },
    document: { getElementById: id => elements.get(id) }, byId: id => elements.get(id),
    setTimeout: fn => { timers.set(++next, fn); return next; }, clearTimeout: id => timers.delete(id) };
  runInNewContext(settingsCode + ';this.panel = { refresh: refreshE2eeSettings, stop: stopE2eeSettings, action: runE2eeSettingsAction };', ctx);
  return { ctx, elements, timers, calls, set: value => { current = value; } };
}
test('key settings are hidden in default builds and do not call native setup during status reads', async () => {
  const off = settingsFixture({ compiled: false }); await off.ctx.panel.refresh();
  assert.equal(off.elements.get('desktopE2eeSettings').hidden, true); assert.equal(off.calls.status, 0);
  const f = settingsFixture(); await f.ctx.panel.refresh();
  assert.equal(f.calls.setup, 0); assert.equal(f.elements.get('desktopE2eeSetup').hidden, false);
  f.elements.get('desktopE2eeSetup').listeners.click(); await new Promise(setImmediate);
  assert.equal(f.calls.setup, 1); assert.match(f.elements.get('desktopE2eeStatus').textContent, /настроены/);
  assert.equal(f.timers.size, 1);
});
test('foreign installation cannot invoke setup through its hidden button', async () => {
  const f = settingsFixture({ phase: 'replacement_required' }); await f.ctx.panel.refresh();
  assert.equal(f.elements.get('desktopE2eeSetup').hidden, true);
  f.elements.get('desktopE2eeSetup').listeners.click(); await new Promise(setImmediate);
  assert.equal(f.calls.setup, 0);
});
test('closing key settings drops a late status and leaves no polling timer', async () => {
  let resolve;
  const f = settingsFixture({ status: () => new Promise(done => { resolve = done; }) });
  const pending = f.ctx.panel.refresh(); f.ctx.panel.stop(); f.elements.get('settingsModal').style.display = 'none';
  resolve({ context: 2, phase: 'active' }); await pending;
  assert.equal(f.elements.get('desktopE2eeStatus').textContent, ''); assert.equal(f.timers.size, 0);
});
test('explicit key action rejects duplicate clicks and safe retry does not invoke setup', async () => {
  let resolve;
  const f = settingsFixture({ setup: () => new Promise(done => { resolve = done; }) }); await f.ctx.panel.refresh();
  const pending = f.ctx.panel.action('setupDevice'); await f.ctx.panel.action('setupDevice');
  assert.equal(f.calls.setup, 1);
  f.set({ context: 3, phase: 'failed', error: 'remote' }); resolve({ context: 3, phase: 'failed', error: 'remote' }); await pending;
  await f.ctx.panel.refresh(); await f.ctx.panel.action('retry');
  assert.equal(f.calls.setup, 1); assert.equal(f.calls.retry, 1);
});


test('account change before the bootstrap reply retires the binding and preserves the new login', async () => {
  let enter, release;
  const pending = new Promise(resolve => { enter = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true,
    invoke: (name, args) => {
      if (name === 'e2ee_session' && args.token) { enter(); return new Promise(resolve => { release = () => resolve({ context: 2, phase: 'starting', replyLease: '1' }); }); }
    },
    beforeReady: async ({ ctx, calls, listeners }) => {
      await pending;
      assert.ok(listeners.has('storage'));
      ctx.localStorage.setItem('token', 'new-session');
      listeners.get('storage')[0]({ key: 'token' });
      release();
    },
  });
  assert.equal(f.ctx.localStorage.getItem('token'), 'new-session');
  assert.deepEqual(f.navigation, ['/login.html']);
  const retire = f.calls.find(c => c.name === 'e2ee_session' && c.args.token === null);
  assert.equal(retire.args.context, 1);
});

test('replacement requires explicit password/confirmation and sends no renderer CAS target', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  for (const [password, confirmed] of [['synthetic', false], ['', true], ['é'.repeat(37), true]]) {
    await assert.rejects(f.ctx.zchatDesktop.e2ee.replaceDevice(password, confirmed));
  }
  assert.equal(f.calls.some(c => c.name === 'e2ee_replace'), false);
  await f.ctx.zchatDesktop.e2ee.replaceDevice('synthetic', true);
  const r = f.calls.find(c => c.name === 'e2ee_replace');
  assert.deepEqual(Object.keys(r.args).sort(), ['consent', 'context', 'epoch', 'requestId']);
  assert.equal(r.args.context, 2);
  assert.deepEqual(Object.keys(r.args.consent).sort(), ['confirmed', 'password']);
  await f.ctx.zchatDesktop.e2ee.endSession();
  await assert.rejects(f.ctx.zchatDesktop.e2ee.replaceDevice('synthetic', true), e => e.name === 'AbortError');
});
test('replacement form clears password before awaiting native IPC and rejects duplicate clicks', async () => {
  let resolve;
  const f = settingsFixture({ replace: (password, confirmed) => {
    assert.equal(password, 'synthetic'); assert.equal(confirmed, true);
    assert.equal(f.elements.get('desktopE2eePassword').value, '');
    assert.equal(f.elements.get('desktopE2eeConfirm').checked, false);
    return new Promise(done => { resolve = done; });
  } });
  f.set({ context: 2, phase: 'replacement_required', replacement: { expectedGeneration: 'native-generation', continuing: false, retargeting: false } });
  await f.ctx.panel.refresh();
  assert.equal(f.elements.get('desktopE2eeReplacement').hidden, false);
  assert.equal(f.elements.get('desktopE2eeReplace').disabled, true);
  f.elements.get('desktopE2eePassword').value = 'synthetic'; f.elements.get('desktopE2eeConfirm').checked = true;
  f.elements.get('desktopE2eePassword').listeners.input();
  const pending = f.ctx.panel.action('replaceDevice'); await f.ctx.panel.action('replaceDevice');
  assert.equal(f.calls.replace, 1);
  f.set({ context: 3, phase: 'failed', error: 'password_invalid' }); resolve({ context: 3, phase: 'failed', error: 'password_invalid' }); await pending;
  assert.match(f.elements.get('desktopE2eeStatus').textContent, /Неверный пароль/);
  assert.equal(f.elements.get('desktopE2eePassword').value, '');
});
test('changing replacement context or closing settings discards password and confirmation', async () => {
  const f = settingsFixture();
  const offer = { expectedGeneration: 'native-generation', continuing: true, retargeting: false };
  f.set({ context: 2, phase: 'replacement_pending', replacement: offer }); await f.ctx.panel.refresh();
  f.elements.get('desktopE2eePassword').value = 'synthetic'; f.elements.get('desktopE2eeConfirm').checked = true;
  await f.ctx.panel.refresh(); assert.equal(f.elements.get('desktopE2eePassword').value, 'synthetic');
  f.set({ context: 3, phase: 'replacement_pending', replacement: offer }); await f.ctx.panel.refresh();
  assert.equal(f.elements.get('desktopE2eePassword').value, ''); assert.equal(f.elements.get('desktopE2eeConfirm').checked, false);
  f.elements.get('desktopE2eePassword').value = 'synthetic'; f.ctx.panel.stop();
  assert.equal(f.elements.get('desktopE2eePassword').value, '');
});

const dialogView = () => ({ conversationId: '11111111-1111-4111-8111-111111111111', peerUserId: '22222222-2222-4222-8222-222222222222', state: 'active', closeReason: null, closedAtUnixMs: '0' });
test('conversation API sends only selected peer/chat and accepts committed public metadata', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_conversation' ? { context: 2, conversation: dialogView(), replyLease: '900' } : undefined });
  const r = await f.ctx.zchatDesktop.e2ee.createConversation('9007199254740993');
  assert.equal(r.conversation.conversationId, dialogView().conversationId);
  await f.ctx.zchatDesktop.e2ee.getConversation(r.conversation.conversationId);
  const calls = f.calls.filter(c => c.name === 'e2ee_conversation');
  assert.equal(JSON.stringify(calls[0].args.operation), JSON.stringify({ action: 'create', peerUserId: '9007199254740993' }));
  assert.equal(JSON.stringify(calls[1].args.operation), JSON.stringify({ action: 'get', conversationId: dialogView().conversationId }));
  assert.equal(calls.every(c => c.args.context === 2), true);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 3);
});
test('invalid IDs/default builds cannot create or lookup a conversation', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  for (const id of [1, '', '0', '01', '-1', '9223372036854775808', '../other-account']) await assert.rejects(f.ctx.zchatDesktop.e2ee.createConversation(id));
  await assert.rejects(f.ctx.zchatDesktop.e2ee.getConversation('../other-account'));
  assert.equal(f.calls.some(c => c.name === 'e2ee_conversation'), false);
  const off = await fixture(); await assert.rejects(off.ctx.zchatDesktop.e2ee.createConversation('1'));
  assert.equal(off.calls.some(c => c.name === 'e2ee_conversation'), false);
});

const queuedText = () => ({ conversationId: '11111111-1111-4111-8111-111111111111', messageId: '22222222-2222-4222-8222-222222222222', cryptoEpoch: '0', createdAtUnixMs: '1800000000000', storedOnServer: false, sealedUnsent: false });
test('explicit successor selects only a closed chat and peer; retries do not invent a request journal', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_conversation' ? { context: 2, conversation: dialogView(), replyLease: '900' } : undefined });
  const api = f.ctx.zchatDesktop.e2ee, chat = queuedText().messageId;
  const first = await api.createConversationAfterClosed('42', chat, true);
  assert.equal(first.conversation.state, 'active');
  await api.createConversationAfterClosed('42', chat, true);
  const calls = f.calls.filter(c => c.name === 'e2ee_conversation');
  assert.equal(JSON.stringify(calls[0].args.operation), JSON.stringify({ action: 'create_after_closed', peerUserId: '42', closedConversationId: chat, confirmed: true }));
  assert.equal(JSON.stringify(calls[0].args.operation), JSON.stringify(calls[1].args.operation));
  assert.notEqual(calls[0].args.requestId, calls[1].args.requestId, 'IPC ID is disposable, not the protected creation UUID');
  assert.equal(f.ctx.localStorage.length, 3, 'no ordinary outbox/draft journal');
});
test('successor without exact consent or canonical peer/chat/default build never reaches native IPC', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  for (const consent of [undefined, false, 1, 'true']) await assert.rejects(f.ctx.zchatDesktop.e2ee.createConversationAfterClosed('42', queuedText().messageId, consent));
  await assert.rejects(f.ctx.zchatDesktop.e2ee.createConversationAfterClosed('042', queuedText().messageId, true));
  await assert.rejects(f.ctx.zchatDesktop.e2ee.createConversationAfterClosed('42', '../other', true));
  const off = await fixture(); await assert.rejects(off.ctx.zchatDesktop.e2ee.createConversationAfterClosed('42', queuedText().messageId, true));
  assert.equal(f.calls.some(c => c.name === 'e2ee_conversation'), false);
  assert.equal(off.calls.some(c => c.name === 'e2ee_conversation'), false);
});
test('successor committed reply is discarded when account changes during native ACK', async () => {
  let entered, release, request;
  const gate = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_conversation') { request = args.requestId; return { context: 2, conversation: dialogView(), replyLease: '900' }; }
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.createConversationAfterClosed('42', queuedText().messageId, true);
  await gate; f.ctx.localStorage.setItem('token', 'different-account'); release();
  await assert.rejects(pending, e => e.name === 'AbortError');
});
test('native text SDK preserves caller UUID across retries and never touches ordinary plaintext storage', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_send_text' ? { context: 2, queued: queuedText(), replyLease: '901' } : undefined });
  const input = { conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'я'.repeat(1000) };
  const before = f.ctx.localStorage.length;
  await f.ctx.zchatDesktop.e2ee.sendText(input, 2); await f.ctx.zchatDesktop.e2ee.sendText(input, 2);
  const calls = f.calls.filter(c => c.name === 'e2ee_send_text');
  assert.equal(calls.length, 2); assert.notEqual(calls[0].args.requestId, calls[1].args.requestId);
  assert.equal(JSON.stringify(calls[0].args.message), JSON.stringify(input));
  assert.equal(JSON.stringify(calls[1].args.message), JSON.stringify(input));
  assert.equal(f.ctx.localStorage.length, before); assert.equal(f.requests.length, 0);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 3);
});
test('text SDK validates UTF8 bytes, stable IDs and exact public fields before IPC', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  const valid = { conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'x' };
  for (const input of [null, { ...valid, text: '' }, { ...valid, text: 'я'.repeat(1001) }, { ...valid, text: '🦀'.repeat(501) }, { ...valid, messageId: '../other' }, { ...valid, cryptoEpoch: '5' }]) await assert.rejects(f.ctx.zchatDesktop.e2ee.sendText(input, 2));
  assert.equal(f.calls.some(c => c.name === 'e2ee_send_text'), false);
  const off = await fixture(); await assert.rejects(off.ctx.zchatDesktop.e2ee.sendText(valid, 2));
  assert.equal(off.calls.some(c => c.name === 'e2ee_send_text'), false);
});
test('text acceptance rejects mismatched bindings, unsafe counters and private/native data', async () => {
  const input = { conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'x' };
  for (const queued of [{ ...queuedText(), messageId: input.conversationId }, { ...queuedText(), cryptoEpoch: 0 }, { ...queuedText(), createdAtUnixMs: '9223372036854775808' }, { ...queuedText(), ciphertext: 'must-not-export' }, { ...queuedText(), storedOnServer: true, sealedUnsent: true }]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_send_text' ? { context: 2, queued, replyLease: '901' } : undefined });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.sendText(input, 2), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
});
test('text result is discarded on account switch during asynchronous native ACK', async () => {
  let entered, release, request;
  const gate = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_send_text') { request = args.requestId; return { context: 2, queued: queuedText(), replyLease: '901' }; }
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.sendText({ conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'x' }, 2); await gate;
  await f.ctx.zchatDesktop.e2ee.endSession(); release();
  await assert.rejects(pending, e => e.name === 'AbortError');
});

const deliveryView = () => ({ storedOnServer: false, state: null, removal: null, sealedUnsent: false });
const historyRow = () => ({ messageId: queuedText().messageId, conversationId: queuedText().conversationId, cryptoEpoch: '0', createdAtUnixMs: '1800000000000', senderUserId: '33333333-3333-4333-8333-333333333333', text: 'protected local history', outgoing: true, delivery: deliveryView() });

test('attachment presentation accepts bounded metadata in history and pending pages and rejects private fields', async () => {
  const metadata = { kind: 'photo', filename: 'photo.png', caption: '\u0001'.repeat(2000), mimeType: 'image/png', size: 1024, photoMetadataStripped: true };
  let attachment = metadata;
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name !== 'e2ee_read') return;
    const { kind, conversationId } = args.query;
    const row = { ...historyRow(), conversationId, attachment };
    const items = kind === 'pending_attachments' ? [{
      messageId: row.messageId, createdAtUnixMs: row.createdAtUnixMs, cryptoEpoch: row.cryptoEpoch, attachment,
    }] : [row];
    return { context: args.context, replyLease: '900', page: {
      kind, ...(kind === 'pending_attachments' ? { conversationId } : {}), items, nextCursor: null,
    } };
  } });
  const api = f.ctx.zchatDesktop.e2ee;
  await assert.rejects(api.pendingAttachments(photoChat, 1), { name: 'AbortError' });
  assert.equal(f.calls.some(c => c.name === 'e2ee_read'), false);
  assert.equal((await api.pendingAttachments(photoChat, 2)).page.items[0].attachment.caption.length, 2000);
  assert.equal((await api.history(photoChat)).page.items[0].attachment.filename, 'photo.png');
  for (attachment of [{ ...metadata, privateDescriptor: 'forbidden' }, { ...metadata, size: 10 * 1024 * 1024 + 1 },
      { ...metadata, mimeType: 'image/svg+xml' }, { ...metadata, photoMetadataStripped: null }]) {
    await assert.rejects(api.pendingAttachments(photoChat, 2), { name: 'AbortError' });
    await assert.rejects(api.history(photoChat), { name: 'AbortError' });
  }
});
test('history accepts local availability on attachments and download state only on incoming attachments', async () => {
  const attachment = { kind: 'file', filename: 'a.txt', caption: '', mimeType: 'application/octet-stream', size: 1, photoMetadataStripped: null };
  const received = { ...historyRow(), conversationId: photoChat, attachment, outgoing: false, delivery: null };
  let row = { ...received, cacheState: 'saved' };
  const f = await photoFixture((name, args) => name === 'e2ee_read' ? {
    context: args.context, replyLease: '900', page: { kind: 'history', items: [row], nextCursor: null },
  } : undefined);
  const api = f.ctx.zchatDesktop.e2ee;
  assert.equal((await api.history(photoChat)).page.items[0].cacheState, 'saved');
  row = { ...received, outgoing: true, delivery: deliveryView(), cacheState: 'saved' };
  assert.equal((await api.history(photoChat)).page.items[0].cacheState, 'saved');
  for (row of [{ ...received, cacheState: 'unverified' }, { ...received, attachment: undefined, cacheState: 'saved' },
      { ...received, outgoing: true, delivery: deliveryView(), cacheState: 'missing' }]) {
    await assert.rejects(api.history(photoChat), { name: 'AbortError' });
  }
});
test('native text requires the exact captured dialog context before any IPC mutation', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true });
  const message = { conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'old draft' };
  for (const context of [undefined, null, '2', -1, 0, 1.5, 3, NaN, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(f.ctx.zchatDesktop.e2ee.sendText(message, context), e => e.name === 'AbortError');
  }
  assert.equal(f.calls.some(c => c.name === 'e2ee_send_text'), false);
});
test('after native retry advances context, an old draft is refused before encryption while new-context sends remain usable', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => name === 'e2ee_send_text' ? { context: args.context, queued: queuedText(), replyLease: '900' } : undefined });
  const message = { conversationId: queuedText().conversationId, messageId: queuedText().messageId, text: 'draft' };
  const status = await f.ctx.zchatDesktop.e2ee.retry(); assert.equal(status.context, 3);
  await assert.rejects(f.ctx.zchatDesktop.e2ee.sendText(message, 2), e => e.name === 'AbortError');
  assert.equal(f.calls.some(c => c.name === 'e2ee_send_text'), false);
  await f.ctx.zchatDesktop.e2ee.sendText(message, 3);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_send_text').length, 1);
});
const pageReply = page => ({ context: 2, page, replyLease: '902' });
test('local page SDK selects only chat/cursor/IDs and keeps plaintext out of ordinary storage', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name !== 'e2ee_read') return;
    const items = args.query.kind === 'dialogs' ? [dialogView()] : args.query.kind === 'history' ? [historyRow()] : [{ messageId: queuedText().messageId, cryptoEpoch: '0', delivery: deliveryView() }];
    return pageReply({ kind: args.query.kind, items, nextCursor: null });
  } });
  const before = f.ctx.localStorage.length;
  await f.ctx.zchatDesktop.e2ee.listConversations();
  const h = await f.ctx.zchatDesktop.e2ee.history(queuedText().conversationId);
  assert.equal(h.page.items[0].text, historyRow().text);
  await f.ctx.zchatDesktop.e2ee.messageStatuses(queuedText().conversationId, [queuedText().messageId]);
  const calls = f.calls.filter(c => c.name === 'e2ee_read');
  assert.equal(JSON.stringify(calls[0].args.query), JSON.stringify({ kind: 'dialogs', cursor: null }));
  assert.equal(JSON.stringify(calls[1].args.query), JSON.stringify({ kind: 'history', conversationId: queuedText().conversationId, cursor: null }));
  assert.equal(f.ctx.localStorage.length, before); assert.equal(f.requests.length, 0);
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 4);
});
test('history accepts a whole escaped text above4KiB and rejects responses above16KiB', async () => {
  const row = { ...historyRow(), text: '\u0001'.repeat(2000) };
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_read' ? pageReply({ kind: 'history', items: [row], nextCursor: queuedText().messageId }) : undefined });
  assert.ok(new TextEncoder().encode(JSON.stringify(pageReply({ kind: 'history', items: [row], nextCursor: null }))).length > 4096);
  assert.equal((await f.ctx.zchatDesktop.e2ee.history(row.conversationId)).page.items[0].text, row.text);
  const big = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_read' ? pageReply({ kind: 'history', items: [row, { ...row, messageId: row.conversationId }], nextCursor: null }) : undefined });
  await assert.rejects(big.ctx.zchatDesktop.e2ee.history(row.conversationId), e => e.name === 'AbortError');
});
test('page IDs and bounded unique status selection are validated before IPC', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true }); const api = f.ctx.zchatDesktop.e2ee;
  await assert.rejects(api.listConversations('raw-scope')); await assert.rejects(api.history('../other'));
  for (const ids of [[], [queuedText().messageId, queuedText().messageId], Array(9).fill(queuedText().messageId), ['raw-scope']]) await assert.rejects(api.messageStatuses(queuedText().conversationId, ids));
  assert.equal(f.calls.some(c => c.name === 'e2ee_read'), false);
  const off = await fixture(); await assert.rejects(off.ctx.zchatDesktop.e2ee.listConversations()); await assert.rejects(off.ctx.zchatDesktop.e2ee.history(queuedText().conversationId));
});
test('page replies reject other-chat text, private fields, service content, wrong kind, duplicates and cursor shapes', async () => {
  for (const page of [
    { kind: 'history', items: [{ ...historyRow(), conversationId: queuedText().messageId }], nextCursor: null },
    { kind: 'history', items: [{ ...historyRow(), privateKey: 'forbidden' }], nextCursor: null },
    { kind: 'history', items: [{ ...historyRow(), text: '', confirmation: true }], nextCursor: null },
    { kind: 'dialogs', items: [], nextCursor: null },
    { kind: 'history', items: [historyRow(), historyRow()], nextCursor: null },
    { kind: 'history', items: [], nextCursor: queuedText().messageId },
    { kind: 'history', items: [historyRow()], nextCursor: 'raw-key' },
    { kind: 'history', items: [null], nextCursor: null },
  ]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_read' ? pageReply(page) : undefined });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.history(queuedText().conversationId), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
});
test('account switch during page ACK discards already-read plaintext', async () => {
  let entered, release, request; const gate = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_read') { request = args.requestId; return pageReply({ kind: 'history', items: [historyRow()], nextCursor: null }); }
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.history(queuedText().conversationId); await gate; await f.ctx.zchatDesktop.e2ee.endSession(); release();
  await assert.rejects(pending, e => e.name === 'AbortError');
});
test('malformed/private conversation fields are rejected and native reply capacity is acknowledged', async () => {
  for (const conversation of [{ ...dialogView(), identityKey: 'must-not-be-exposed' }, { ...dialogView(), state: 'closed' }, { ...dialogView(), closedAtUnixMs: 0 }]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_conversation' ? { context: 2, conversation, replyLease: '900' } : undefined });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.createConversation('1'), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
});
test('logout while acknowledging a conversation discards the previous account reply', async () => {
  let release, entered; const gate = new Promise(resolve => { entered = resolve; }); let request;
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_conversation') { request = args.requestId; return { context: 2, conversation: dialogView(), replyLease: '900' }; }
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.getConversation(dialogView().conversationId); await gate;
  await f.ctx.zchatDesktop.e2ee.endSession(); release();
  await assert.rejects(pending, e => e.name === 'AbortError');
});

const recoveryState = () => ({ kind: 'state', conversationId: dialogView().conversationId, cryptoEpoch: '0', receiveOnly: false, sendingFrozen: false, proposals: [] });
const recoveryIntent = () => ({ conversationId: dialogView().conversationId, expectedEpoch: '0', expectedOwnProposalId: null });
const recoveryDecision = () => ({ conversationId: dialogView().conversationId, expectedEpoch: '0', proposalId: dialogView().peerUserId, decision: 'accept' });
const recoveryQueued = () => ({ kind: 'queued', conversationId: dialogView().conversationId, cryptoEpoch: '0', operationId: dialogView().peerUserId });
test('recovery SDK exposes only bounded local metadata and explicit native intent acceptance', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => name === 'e2ee_recovery' ? { context: 2, recovery: args.operation.action === 'get' ? recoveryState() : recoveryQueued(), replyLease: '1700' } : undefined });
  const api = f.ctx.zchatDesktop.e2ee;
  assert.equal((await api.recoveryStatus(dialogView().conversationId)).recovery.kind, 'state');
  assert.equal((await api.proposeRecovery(recoveryIntent(), 2, true)).recovery.kind, 'queued');
  await api.decideRecovery(recoveryDecision(), 2, true);
  const calls = f.calls.filter(c => c.name === 'e2ee_recovery');
  assert.equal(calls.length, 3); assert.equal(calls[1].args.operation.expectedOwnProposalId, null);
  assert.equal(calls[2].args.operation.proposalId, dialogView().peerUserId); assert.equal(calls[2].args.operation.confirmed, true);
  assert.equal(f.requests.length, 0); assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 4);
});
test('recovery mutations require current expected context and explicit confirmation before any invoke', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true }); const api = f.ctx.zchatDesktop.e2ee;
  for (const context of [undefined, null, -1, 1, 3, '2', NaN, 2.5, Number.MAX_SAFE_INTEGER + 1]) {
    await assert.rejects(api.proposeRecovery(recoveryIntent(), context, true), e => e.name === 'AbortError');
    await assert.rejects(api.decideRecovery(recoveryDecision(), context, true), e => e.name === 'AbortError');
  }
  for (const confirmed of [false, undefined, 'true', 1]) {
    await assert.rejects(api.proposeRecovery(recoveryIntent(), 2, confirmed));
    await assert.rejects(api.decideRecovery(recoveryDecision(), 2, confirmed));
  }
  assert.equal(f.calls.some(c => c.name === 'e2ee_recovery'), false);
});
test('recovery validates exact fields, canonical integer strings, UUIDs and action enums before IPC', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true }); const api = f.ctx.zchatDesktop.e2ee;
  for (const extra of ['scope', 'generation', 'signature', 'requestId']) await assert.rejects(api.proposeRecovery({ ...recoveryIntent(), [extra]: 'forged' }, 2, true));
  for (const expectedEpoch of [0, '01', '-1', '9223372036854775808', '1.0']) await assert.rejects(api.proposeRecovery({ ...recoveryIntent(), expectedEpoch }, 2, true));
  const missing = recoveryIntent(); delete missing.expectedOwnProposalId; await assert.rejects(api.proposeRecovery(missing, 2, true));
  for (const expectedOwnProposalId of ['', '1', '../scope']) await assert.rejects(api.proposeRecovery({ ...recoveryIntent(), expectedOwnProposalId }, 2, true));
  await assert.rejects(api.decideRecovery({ ...recoveryDecision(), decision: 'reset' }, 2, true)); await assert.rejects(api.recoveryStatus('1'));
  assert.equal(f.calls.some(c => c.name === 'e2ee_recovery'), false);
});
test('recovery rejects malformed, foreign, oversized and private replies but releases native reply slots', async () => {
  const proposal = { proposalId: dialogView().peerUserId, state: 'pending', ownProposal: false, decision: null, sendingFrozen: false };
  for (const recovery of [
    { ...recoveryState(), signature: 'private' }, { ...recoveryState(), conversationId: dialogView().peerUserId },
    { ...recoveryState(), cryptoEpoch: 0 }, { ...recoveryState(), cryptoEpoch: null, sendingFrozen: true },
    { ...recoveryState(), proposals: [proposal, proposal] }, { ...recoveryState(), proposals: [{ ...proposal, state: null }] },
    { ...recoveryState(), proposals: [{ ...proposal, decision: 'cancel' }] },
    { ...recoveryState(), proposals: [{ ...proposal, sendingFrozen: true }] },
    { ...recoveryState(), proposals: [{ ...proposal, state: 'invalid'.repeat(1000) }] },
    recoveryQueued(),
  ]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_recovery' ? { context: 2, recovery, replyLease: '1700' } : undefined });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.recoveryStatus(dialogView().conversationId), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
});
test('recovery accepts an uninitialized empty dialog and both current proposal roles without performing setup', async () => {
  const own = { proposalId: dialogView().conversationId, state: null, ownProposal: true, decision: 'cancel', sendingFrozen: true };
  const peer = { proposalId: dialogView().peerUserId, state: 'pending', ownProposal: false, decision: 'decline', sendingFrozen: true };
  for (const recovery of [{ ...recoveryState(), cryptoEpoch: null }, { ...recoveryState(), sendingFrozen: true, proposals: [own, peer] }]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_recovery' ? { context: 2, recovery, replyLease: '1700' } : undefined });
    assert.equal((await f.ctx.zchatDesktop.e2ee.recoveryStatus(dialogView().conversationId)).recovery.cryptoEpoch, recovery.cryptoEpoch);
    assert.equal(f.calls.some(c => ['e2ee_setup', 'e2ee_replace', 'e2ee_retry'].includes(c.name)), false);
  }
});
test('a stale recovery form cannot adopt the SDK context after retry; the caller must observe and confirm again', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => name === 'e2ee_recovery' ? { context: args.context, recovery: recoveryQueued(), replyLease: '1700' } : undefined });
  const api = f.ctx.zchatDesktop.e2ee; await api.retry();
  await assert.rejects(api.proposeRecovery(recoveryIntent(), 2, true), e => e.name === 'AbortError');
  await assert.rejects(api.decideRecovery(recoveryDecision(), 2, true), e => e.name === 'AbortError');
  assert.equal(f.calls.some(c => c.name === 'e2ee_recovery'), false);
  await api.proposeRecovery(recoveryIntent(), 3, true); assert.equal(f.calls.find(c => c.name === 'e2ee_recovery').args.context, 3);
});
test('account switch while acknowledging a recovery response discards its old result', async () => {
  let release, entered, request; const gate = new Promise(resolve => { entered = resolve; });
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: (name, args) => {
    if (name === 'e2ee_recovery') { request = args.requestId; return { context: 2, recovery: recoveryQueued(), replyLease: '1700' }; }
    if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
  } });
  const pending = f.ctx.zchatDesktop.e2ee.proposeRecovery(recoveryIntent(), 2, true); await gate;
  f.ctx.localStorage.setItem('token', 'other-account'); release(); await assert.rejects(pending, e => e.name === 'AbortError');
});
test('native recovery conflict is not automatically retried with a fresh epoch or reference', async () => {
  const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => { if (name === 'e2ee_recovery') throw 'conflict'; } });
  await assert.rejects(f.ctx.zchatDesktop.e2ee.proposeRecovery(recoveryIntent(), 2, true), error => error === 'conflict');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_recovery').length, 1); assert.equal(f.calls.some(c => c.name === 'e2ee_setup'), false);
});
test('recovery mutation replies must match the exact chat/epoch and contain no extra authority fields', async () => {
  for (const recovery of [
    { ...recoveryQueued(), cryptoEpoch: '1' }, { ...recoveryQueued(), conversationId: dialogView().peerUserId },
    { ...recoveryQueued(), operationId: 'bad' }, { ...recoveryQueued(), activated: true }, recoveryState(),
  ]) {
    const f = await fixture(undefined, undefined, [], { chat: true, e2ee: true, invoke: name => name === 'e2ee_recovery' ? { context: 2, recovery, replyLease: '1700' } : undefined });
    await assert.rejects(f.ctx.zchatDesktop.e2ee.proposeRecovery(recoveryIntent(), 2, true), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
});

const archiveGen = '44444444-4444-4444-8444-444444444444';
const archiveNextGen = '55555555-5555-4555-8555-555555555555';
const archiveCursor = '99999999-9999-4999-8999-999999999999';
const archiveId = n => `00000000-0000-4000-8000-${n.toString(16).padStart(12, '0')}`;
const archiveTarget = () => ({ generationId: archiveGen, conversationId: dialogView().conversationId });
const archiveHistoryTarget = epoch => ({ ...archiveTarget(), cryptoEpoch: epoch === undefined ? null : epoch });
const archiveGeneration = (id = archiveGen) => ({ generationId: id, replacedByGenerationId: id === archiveNextGen ? archiveCursor : archiveNextGen, historyReady: true });
const archiveSegment = (epoch = '0', sealed = false) => ({ cryptoEpoch: epoch, sealed, createdAtUnixMs: String(1000n + BigInt(epoch)), sealedAtUnixMs: sealed ? String(1001n + BigInt(epoch)) : null });
function archivePageFor(query, items, nextCursor = null) {
  const scope = query.kind === 'archives' ? {} : query.kind === 'archive_dialogs' ? { generationId: query.generationId } : query.kind === 'segments' ?
    { generationId: query.generationId, conversationId: query.conversationId } : { generationId: query.generationId, conversationId: query.conversationId, cryptoEpoch: query.cryptoEpoch };
  return { kind: query.kind, ...scope, items, nextCursor };
}
function archiveFixture(read, options = {}) {
  return fixture(undefined, undefined, [], { chat: true, e2ee: true, ...options, invoke: async (name, args) => {
    if (options.invoke) { const value = await options.invoke(name, args); if (value !== undefined) return value; }
    if (name === 'e2ee_read') return read(args.query, args);
  } });
}
const archiveCall = (api, kind, context = 2, cursor = null, target) => kind === 'archives' ? api.listArchives(context, cursor) :
  kind === 'archive_dialogs' ? api.archiveConversations(target || { generationId: archiveGen }, context, cursor) : kind === 'segments' ?
  api.archiveSegments(target || archiveTarget(), context, cursor) : api.archiveHistory(target || archiveHistoryTarget(), context, cursor);
const archiveRows = kind => kind === 'archives' ? [archiveGeneration()] : kind === 'archive_dialogs' ? [dialogView()] : kind === 'segments' ? [archiveSegment()] : [historyRow()];

test('archive SDK reads all four native page kinds in a blocked installation without mutations or plaintext storage', async () => {
  const f = await archiveFixture(q => pageReply(archivePageFor(q, archiveRows(q.kind))), { invoke: name => name === 'e2ee_status' ?
    { context: 2, phase: 'device_unavailable', runtimePhase: 'ArchiveReadOnly', replyLease: '903' } : undefined });
  const api = f.ctx.zchatDesktop.e2ee;
  assert.equal((await api.status()).runtimePhase, 'ArchiveReadOnly');
  const firstReadCall = f.calls.length;
  const before = JSON.stringify([f.ctx.localStorage, f.ctx.sessionStorage].map(s => Array.from({ length: s.length }, (_, i) => [s.key(i), s.getItem(s.key(i))])));
  for (const kind of ['archives', 'archive_dialogs', 'segments', 'archive_history']) assert.equal((await archiveCall(api, kind)).page.kind, kind);
  const queries = f.calls.filter(c => c.name === 'e2ee_read').map(c => JSON.parse(JSON.stringify(c.args.query)));
  assert.deepEqual(queries, [{ kind: 'archives', cursor: null }, { kind: 'archive_dialogs', generationId: archiveGen, cursor: null },
    { kind: 'segments', ...archiveTarget(), cursor: null }, { kind: 'archive_history', ...archiveHistoryTarget(), cursor: null }]);
  assert.equal(JSON.stringify([f.ctx.localStorage, f.ctx.sessionStorage].map(s => Array.from({ length: s.length }, (_, i) => [s.key(i), s.getItem(s.key(i))]))), before);
  assert.equal(f.requests.length, 0);
  assert.ok(f.calls.slice(firstReadCall).every(c => ['e2ee_read', 'e2ee_ack'].includes(c.name)));
  assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 6);
});
test('empty/unprepared archive metadata is readable without creating keys or an epoch', async () => {
  const f = await archiveFixture(q => pageReply(archivePageFor(q, q.kind === 'archives' ? [{ ...archiveGeneration(), historyReady: false }] : [])));
  assert.equal((await f.ctx.zchatDesktop.e2ee.listArchives(2)).page.items[0].historyReady, false);
  for (const kind of ['archive_dialogs', 'segments', 'archive_history']) assert.equal((await archiveCall(f.ctx.zchatDesktop.e2ee, kind)).page.items.length, 0);
  assert.equal(f.calls.some(c => ['e2ee_setup', 'e2ee_retry', 'e2ee_replace', 'e2ee_recovery', 'e2ee_send_text'].includes(c.name)), false);
});
test('every archive query requires the observed context before IPC and remains disabled in default builds', async () => {
  const f = await archiveFixture(() => assert.fail('invalid context must not invoke native reads'));
  for (const kind of ['archives', 'archive_dialogs', 'segments', 'archive_history']) {
    for (const context of [null, -1, 0, 1, 3, '2', NaN, 2.5, Number.MAX_SAFE_INTEGER + 1]) await assert.rejects(archiveCall(f.ctx.zchatDesktop.e2ee, kind, context), e => e.name === 'AbortError');
  }
  await assert.rejects(f.ctx.zchatDesktop.e2ee.listArchives(), e => e.name === 'AbortError');
  const off = await archiveFixture(() => assert.fail('default build cannot read archives'), { e2ee: false });
  for (const kind of ['archives', 'archive_dialogs', 'segments', 'archive_history']) await assert.rejects(archiveCall(off.ctx.zchatDesktop.e2ee, kind, 1), e => e.name === 'AbortError');
  assert.equal(f.calls.some(c => c.name === 'e2ee_read'), false);
});
test('archive targets reject noncanonical IDs/epochs, missing fields, arrays, extra authority and malformed cursors', async () => {
  const f = await archiveFixture(() => assert.fail('invalid input must stay in SDK')); const api = f.ctx.zchatDesktop.e2ee;
  for (const target of [{}, { generationId: 'raw-scope' }, { generationId: archiveGen, accountId: archiveGen }, Object.assign([], { generationId: archiveGen })]) await assert.rejects(api.archiveConversations(target, 2), e => e.name === 'TypeError');
  for (const target of [{ generationId: archiveGen }, { ...archiveTarget(), conversationId: '../other' }, { ...archiveTarget(), path: '/private' }]) await assert.rejects(api.archiveSegments(target, 2), e => e.name === 'TypeError');
  for (const epoch of [undefined, 0, '00', '-0', '-1', '+1', '1.0', '9223372036854775808']) await assert.rejects(api.archiveHistory({ ...archiveTarget(), cryptoEpoch: epoch }, 2), e => e.name === 'TypeError');
  await assert.rejects(api.archiveHistory(archiveTarget(), 2), e => e.name === 'TypeError');
  for (const kind of ['archives', 'archive_dialogs', 'segments', 'archive_history']) await assert.rejects(archiveCall(api, kind, 2, 'raw-position'), e => e.name === 'TypeError');
  assert.equal(f.calls.some(c => c.name === 'e2ee_read'), false);
});
test('archive input is snapshotted once before IPC and a caller cannot mutate the selected generation during a read', async () => {
  let release, query, entered; const gate = new Promise(resolve => { entered = resolve; }); let reads = 0;
  const f = await archiveFixture(q => { query = q; entered(); return new Promise(resolve => { release = () => resolve(pageReply(archivePageFor(q, [dialogView()]))); }); });
  const target = { get generationId() { reads++; return archiveGen; } };
  const pending = f.ctx.zchatDesktop.e2ee.archiveConversations(target, 2); await gate;
  assert.equal(reads, 1); assert.equal(Object.isFrozen(query), true);
  release(); assert.equal((await pending).page.generationId, archiveGen);
  const mutable = { generationId: archiveGen }; const second = f.ctx.zchatDesktop.e2ee.archiveConversations(mutable, 2);
  mutable.generationId = archiveNextGen; release(); assert.equal((await second).page.generationId, archiveGen);
});
test('a reentrant target getter cannot send an archive read after ending the session', async () => {
  const f = await archiveFixture(() => assert.fail('read after reentrant logout')); let ended;
  const api = f.ctx.zchatDesktop.e2ee;
  await assert.rejects(api.archiveConversations({ get generationId() { ended = api.endSession(); return archiveGen; } }, 2), e => e.name === 'AbortError');
  await ended; assert.equal(f.calls.some(c => c.name === 'e2ee_read'), false);
});
test('archive replies must echo exact scope and kind and cannot contain extra/private fields', async () => {
  const query = { kind: 'archive_history', ...archiveHistoryTarget('0'), cursor: null };
  const valid = archivePageFor(query, [historyRow()]);
  for (const page of [
    Object.assign([], valid), { ...valid, generationId: archiveNextGen }, { ...valid, conversationId: archiveNextGen }, { ...valid, cryptoEpoch: null },
    { ...valid, kind: 'history' }, { ...valid, privateKey: 'forbidden' }, { ...valid, nextCursor: 'raw' }, { ...valid, items: [], nextCursor: archiveCursor },
    { ...valid, items: [{ ...historyRow(), cryptoEpoch: '1' }] }, { ...valid, items: [{ ...historyRow(), credential: 'forbidden' }] },
    { ...valid, items: [{ ...historyRow(), conversationId: archiveNextGen }] }, { ...valid, items: [{ ...historyRow(), text: null }] },
    { ...valid, items: [{ ...historyRow(), delivery: { ...deliveryView(), privateScope: 'forbidden' } }] },
  ]) {
    const f = await archiveFixture(() => pageReply(page));
    await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget('0'), 2), e => e.name === 'AbortError');
    assert.equal(f.calls.filter(c => c.name === 'e2ee_ack').length, 2);
  }
  for (const reply of [{ ...pageReply(valid), rawCiphertext: 'forbidden' }, Object.assign([], pageReply(valid))]) {
    const f = await archiveFixture(() => reply);
    await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget('0'), 2), e => e.name === 'AbortError');
  }
});
test('generation inventory rejects duplicates, wrong order, self replacement, multiple current generations and excess items', async () => {
  const g = archiveGeneration();
  for (const items of [
    [g, g], [archiveGeneration(archiveNextGen), g], [{ ...g, generationId: 'bad' }], [{ ...g, replacedByGenerationId: archiveGen }],
    [{ ...g, historyReady: 1 }], [{ ...g, keys: [] }], [{ ...g, replacedByGenerationId: null }, { ...archiveGeneration(archiveNextGen), replacedByGenerationId: null }],
    Array.from({ length: 9 }, (_, i) => archiveGeneration(archiveId(i + 1))),
  ]) {
    const f = await archiveFixture(q => pageReply(archivePageFor(q, items)));
    await assert.rejects(f.ctx.zchatDesktop.e2ee.listArchives(2), e => e.name === 'AbortError');
  }
});
test('archive dialog metadata retains strict lifecycle validation, uniqueness and UUID order', async () => {
  for (const items of [[dialogView(), dialogView()], [{ ...dialogView(), state: 'closed' }], [{ ...dialogView(), keys: [] }],
    [{ ...dialogView(), conversationId: archiveNextGen }, dialogView()]]) {
    const f = await archiveFixture(q => pageReply(archivePageFor(q, items)));
    await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveConversations({ generationId: archiveGen }, 2), e => e.name === 'AbortError');
  }
});
test('segment metadata rejects gaps, bad sealing/timestamps, duplicate epochs and incomplete terminal pages', async () => {
  for (const items of [
    [{ ...archiveSegment(), sealed: true }], [{ ...archiveSegment(), sealedAtUnixMs: '1001' }], [{ ...archiveSegment(), createdAtUnixMs: '0' }],
    [{ ...archiveSegment(), cryptoEpoch: '00' }], [{ ...archiveSegment(), createdAtUnixMs: '9223372036854775808' }],
    [archiveSegment('2'), archiveSegment('0', true)], [archiveSegment('1'), archiveSegment('1', true)], [archiveSegment('1'), archiveSegment('0')],
    [archiveSegment('1')], [{ ...archiveSegment('0', true), sealedAtUnixMs: '999' }],
  ]) {
    const f = await archiveFixture(q => pageReply(archivePageFor(q, items)));
    await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveSegments(archiveTarget(), 2), e => e.name === 'AbortError');
  }
});
test('archive epochs remain exact decimal strings above JS safe integer precision', async () => {
  const epoch = '9007199254740993';
  const f = await archiveFixture(q => pageReply(archivePageFor(q, [archiveSegment(epoch), archiveSegment('9007199254740992', true)], archiveCursor)));
  assert.equal((await f.ctx.zchatDesktop.e2ee.archiveSegments(archiveTarget(), 2)).page.items[0].cryptoEpoch, epoch);
  const history = await archiveFixture(q => pageReply(archivePageFor(q, [{ ...historyRow(), cryptoEpoch: epoch }])));
  assert.equal((await history.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(epoch), 2)).page.cryptoEpoch, epoch);
  const wrong = await archiveFixture(q => pageReply(archivePageFor(q, [{ ...historyRow(), cryptoEpoch: '9007199254740992' }])));
  await assert.rejects(wrong.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(epoch), 2), e => e.name === 'AbortError');
});
test('all-epoch archive history is descending by epoch/time/UUID and has unique messages', async () => {
  const rows = [
    { ...historyRow(), messageId: archiveId(4), cryptoEpoch: '1', createdAtUnixMs: '1' },
    { ...historyRow(), messageId: archiveId(3), createdAtUnixMs: '3' },
    { ...historyRow(), messageId: archiveId(2), createdAtUnixMs: '3' },
    { ...historyRow(), messageId: archiveId(1), createdAtUnixMs: '2' },
  ];
  const good = await archiveFixture(q => pageReply(archivePageFor(q, rows))); assert.equal((await good.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2)).page.items.length, 4);
  for (const items of [[...rows].reverse(), [rows[1], rows[2], rows[1]], [{ ...historyRow(), outgoing: false }]]) {
    const f = await archiveFixture(q => pageReply(archivePageFor(q, items)));
    await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2), e => e.name === 'AbortError');
  }
});
test('archive text uses UTF8 bounds and permits escaped history above4KiB while retaining the exact16KiB response cap', async () => {
  const long = { ...historyRow(), text: '\u0001'.repeat(2000) };
  const f = await archiveFixture(q => pageReply(archivePageFor(q, [long])));
  assert.equal((await f.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2)).page.items[0].text, long.text);
  for (const text of ['🙂'.repeat(501), '']) {
    const bad = await archiveFixture(q => pageReply(archivePageFor(q, [{ ...historyRow(), text }])));
    await assert.rejects(bad.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2), e => e.name === 'AbortError');
  }
  const q = { kind: 'archive_history', ...archiveHistoryTarget(), cursor: null };
  const rows = Array.from({ length: 8 }, (_, i) => ({ ...historyRow(), messageId: archiveId(i + 1), createdAtUnixMs: String(2000 - i), text: 'x' }));
  const page = pageReply(archivePageFor(q, rows)); let remaining = 16384 - Buffer.byteLength(JSON.stringify(page));
  for (const row of rows) { const extra = Math.min(1999, remaining); row.text += 'x'.repeat(extra); remaining -= extra; }
  assert.equal(remaining, 0); assert.equal(Buffer.byteLength(JSON.stringify(page)), 16384);
  const exact = await archiveFixture(() => page); assert.equal((await exact.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2)).page.items.length, 8);
  rows.find(v => v.text.length < 2000).text += 'x'; assert.equal(Buffer.byteLength(JSON.stringify(page)), 16385);
  const tooBig = await archiveFixture(() => page); await assert.rejects(tooBig.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2), e => e.name === 'AbortError');
});
test('issued archive cursors paginate every query family with independent scope and ordering', async () => {
  for (const kind of ['archives', 'archive_dialogs', 'segments', 'archive_history']) {
    const first = kind === 'archives' ? [archiveGeneration(archiveId(1))] : kind === 'archive_dialogs' ? [{ ...dialogView(), conversationId: archiveId(1) }] : kind === 'segments' ? [archiveSegment('1')] : [{ ...historyRow(), createdAtUnixMs: '2' }];
    const last = kind === 'archives' ? [archiveGeneration(archiveId(2))] : kind === 'archive_dialogs' ? [{ ...dialogView(), conversationId: archiveId(2) }] : kind === 'segments' ? [archiveSegment('0', true)] : [{ ...historyRow(), createdAtUnixMs: '1', messageId: archiveId(2) }];
    const f = await archiveFixture(q => pageReply(archivePageFor(q, q.cursor ? last : first, q.cursor ? null : archiveCursor)));
    const api = f.ctx.zchatDesktop.e2ee; const reply = await archiveCall(api, kind);
    assert.equal((await archiveCall(api, kind, reply.context, reply.page.nextCursor)).page.nextCursor, null);
  }
});
test('archive cursor provenance rejects another generation/chat/epoch/query and unknown tokens before IPC', async () => {
  const f = await archiveFixture(q => pageReply(archivePageFor(q, [historyRow()], archiveCursor))); const api = f.ctx.zchatDesktop.e2ee;
  await api.archiveHistory(archiveHistoryTarget('0'), 2);
  for (const target of [{ ...archiveHistoryTarget('0'), generationId: archiveNextGen }, { ...archiveHistoryTarget('0'), conversationId: archiveNextGen }, archiveHistoryTarget('1'), archiveHistoryTarget(null)]) await assert.rejects(api.archiveHistory(target, 2, archiveCursor), e => e.name === 'InvalidStateError');
  await assert.rejects(api.listArchives(2, archiveCursor), e => e.name === 'InvalidStateError');
  await assert.rejects(api.archiveHistory(archiveHistoryTarget('0'), 2, archiveId(1)), e => e.name === 'InvalidStateError');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_read').length, 1);
});
test('cursor positions are copied metadata: mutating returned rows cannot authorize replay or alter the paging boundary', async () => {
  const f = await archiveFixture(q => pageReply(archivePageFor(q, [{ ...historyRow(), messageId: q.cursor ? archiveId(2) : historyRow().messageId, createdAtUnixMs: q.cursor ? '1' : '2' }], q.cursor ? null : archiveCursor)));
  const api = f.ctx.zchatDesktop.e2ee; const first = await api.archiveHistory(archiveHistoryTarget(), 2); first.page.items[0].createdAtUnixMs = '0';
  assert.equal((await api.archiveHistory(archiveHistoryTarget(), 2, first.page.nextCursor)).page.items[0].createdAtUnixMs, '1');
  const replay = await archiveFixture(q => pageReply(archivePageFor(q, [historyRow()], archiveCursor))); const r = await replay.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2);
  await assert.rejects(replay.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2, r.page.nextCursor), e => e.name === 'AbortError');
});
test('archive continuation rejects an earlier page replay or a skipped segment even with a new cursor token', async () => {
  for (const kind of ['archive_history', 'segments']) {
    const f = await archiveFixture(q => pageReply(archivePageFor(q, kind === 'segments' ? [archiveSegment(q.cursor ? '0' : '2', !!q.cursor)] : [historyRow()], q.cursor ? null : archiveCursor)));
    const api = f.ctx.zchatDesktop.e2ee; await archiveCall(api, kind);
    await assert.rejects(archiveCall(api, kind, 2, archiveCursor), e => e.name === 'AbortError');
  }
});
test('archive cursor cache expires monotonically after120s idle and refreshes an explicit use', async () => {
  let now = 0; const f = await archiveFixture(q => pageReply(archivePageFor(q, [archiveGeneration(q.cursor ? archiveId(2) : archiveId(1))], q.cursor ? null : archiveCursor)), { now: () => now });
  const api = f.ctx.zchatDesktop.e2ee; await api.listArchives(2); now = 119999; await api.listArchives(2, archiveCursor);
  now = 239999; await assert.rejects(api.listArchives(2, archiveCursor), e => e.name === 'InvalidStateError');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_read').length, 2);
});
test('archive cursor cache holds at most32positions and evicts the least recently used without retaining history', async () => {
  let issued = 0;
  const f = await archiveFixture(q => pageReply(archivePageFor(q, [archiveGeneration(q.cursor ? archiveId(2) : archiveId(1))], q.cursor ? null : archiveId(100 + ++issued))));
  const api = f.ctx.zchatDesktop.e2ee; const tokens = [];
  for (let i = 0; i < 33; i++) tokens.push((await api.listArchives(2)).page.nextCursor);
  await assert.rejects(api.listArchives(2, tokens[0]), e => e.name === 'InvalidStateError');
  await api.listArchives(2, tokens[1]); await api.listArchives(2);
  await assert.rejects(api.listArchives(2, tokens[2]), e => e.name === 'InvalidStateError'); await api.listArchives(2, tokens[1]);
});
test('a native cursor token reused for another query cannot replace its original SDK binding', async () => {
  const f = await archiveFixture(q => pageReply(archivePageFor(q, q.kind === 'archives' ? [archiveGeneration()] : [{ ...historyRow(), messageId: q.cursor ? archiveId(2) : historyRow().messageId, createdAtUnixMs: q.cursor ? '1' : '2' }], q.cursor ? null : archiveCursor)));
  const api = f.ctx.zchatDesktop.e2ee; await api.archiveHistory(archiveHistoryTarget(), 2);
  await assert.rejects(api.listArchives(2), e => e.name === 'AbortError');
  assert.equal((await api.archiveHistory(archiveHistoryTarget(), 2, archiveCursor)).page.items[0].createdAtUnixMs, '1');
});
test('archive data waiting for ACK is discarded after retry, logout or token change and cannot register a late cursor', async () => {
  for (const change of ['retry', 'logout', 'token']) {
    let release, entered, request; const gate = new Promise(resolve => { entered = resolve; });
    const f = await archiveFixture((q, args) => { request = args.requestId; return pageReply(archivePageFor(q, [historyRow()], archiveCursor)); }, { invoke: (name, args) => {
      if (name === 'e2ee_ack' && args.requestId === request) { entered(); return new Promise(resolve => { release = resolve; }); }
    } });
    const api = f.ctx.zchatDesktop.e2ee; const pending = api.archiveHistory(archiveHistoryTarget(), 2); await gate;
    if (change === 'retry') await api.retry(); else if (change === 'logout') await api.endSession(); else f.ctx.localStorage.setItem('token', 'other-account');
    release(); await assert.rejects(pending, e => e.name === 'AbortError');
    if (change === 'token') f.ctx.localStorage.setItem('token', 'old-session');
    if (change !== 'logout') await assert.rejects(api.archiveHistory(archiveHistoryTarget(), change === 'retry' ? 3 : 2, archiveCursor), e => e.name === 'InvalidStateError');
  }
});
test('a stale archive view cannot adopt the SDK context after retry even if it has no cursor', async () => {
  const f = await archiveFixture((q, args) => ({ ...pageReply(archivePageFor(q, [archiveGeneration(q.cursor ? archiveId(2) : archiveId(1))], q.cursor ? null : archiveCursor)), context: args.context })); const api = f.ctx.zchatDesktop.e2ee;
  await api.listArchives(2); await api.retry(); const fresh = await api.listArchives(3); const count = f.calls.filter(c => c.name === 'e2ee_read').length;
  await assert.rejects(api.listArchives(2), e => e.name === 'AbortError'); assert.equal(f.calls.filter(c => c.name === 'e2ee_read').length, count);
  assert.equal((await api.listArchives(3, fresh.page.nextCursor)).context, 3);
});
test('native archive unavailability is surfaced once without implicit refresh, setup, repair or delivery actions', async () => {
  const f = await archiveFixture(() => { throw 'not_ready'; });
  await assert.rejects(f.ctx.zchatDesktop.e2ee.archiveHistory(archiveHistoryTarget(), 2), e => e === 'not_ready');
  assert.equal(f.calls.filter(c => c.name === 'e2ee_read').length, 1);
  assert.equal(f.calls.some(c => ['e2ee_setup', 'e2ee_retry', 'e2ee_replace', 'e2ee_recovery', 'e2ee_send_text'].includes(c.name)), false);
});
test('archive TypeScript contracts require observed context and exact targets and expose no private material', () => {
  const roots = ['../web/src/chat/global.d.ts', './desktop-archive-types.test.ts', './desktop-photo-types.test.ts'].map(path => fileURLToPath(new URL(path, import.meta.url)));
  const program = ts.createProgram(roots, { noEmit: true, strict: true, skipLibCheck: true, types: [], target: ts.ScriptTarget.ES2022, lib: ['lib.es2022.d.ts', 'lib.dom.d.ts', 'lib.dom.iterable.d.ts'] });
  assert.equal(ts.formatDiagnostics(ts.getPreEmitDiagnostics(program), { getCanonicalFileName: p => p, getCurrentDirectory: () => process.cwd(), getNewLine: () => '\n' }), '');
});

test('late native archive errors cannot be reported as errors of a newly selected context', async () => {
  let reject, entered; const gate = new Promise(resolve => { entered = resolve; });
  const f = await archiveFixture(() => { entered(); return new Promise((_, fail) => { reject = fail; }); });
  const api = f.ctx.zchatDesktop.e2ee; const pending = api.listArchives(2); await gate;
  await api.retry(); reject('not_ready'); await assert.rejects(pending, e => e.name === 'AbortError');
});
