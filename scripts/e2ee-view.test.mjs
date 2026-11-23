import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
const code = ts.transpile(readFileSync(new URL('../web/src/chat/025-e2ee-view.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const photoCode = ts.transpile(readFileSync(new URL('../web/src/chat/025-e2ee-photo.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const fileCode = ts.transpile(readFileSync(new URL('../web/src/chat/025-e2ee-file.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const voiceCode = ts.transpile(readFileSync(new URL('../web/src/chat/025-e2ee-voice.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const recorderCode = ts.transpile(readFileSync(new URL('../web/src/chat/043-voice-messages-mediarecorder.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const send = ts.transpile(readFileSync(new URL('../web/src/chat/018-send-message.ts', import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const CHAT = '11111111-1111-4111-8111-111111111111', CURSOR = '22222222-2222-4222-8222-222222222222';
const tick = () => new Promise(resolve => setImmediate(resolve));
class Element {
  children = []; dataset = {}; style = {}; hidden = false; disabled = false; listeners = new Map(); textContent = ''; scrollTop = 0;
  open = false; value = ''; checked = false;
  get valueAsNumber() { return Number(this.value); }
  showModal() { this.open = true; }
  close() { this.open = false; }
  classList = { remove() {}, contains() { return false; } };
  append(...items) { this.children.push(...items); }
  prepend(item) { this.children.unshift(item); }
  replaceChildren(...items) { this.children = [...items]; }
  get lastElementChild() { return this.children.at(-1); }
  removeChild(item) { this.children = this.children.filter(n => n !== item); }
  get scrollHeight() { return this.children.length * 20; }
  get clientHeight() { return 80; }
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  setAttribute() {}
  removeAttribute(name) { delete this[name]; }
  get isConnected() { return true; }
  async click() { if (!this.disabled) this.listeners.get('click')?.(); await tick(); }
  focus() {}
}
function metadata(state = 'active') { return { context: 2, conversation: { conversationId: CHAT, state } }; }
function item(n, text = 'local text', outgoing = false, delivery = null) {
  return { conversationId: CHAT, messageId: '00000000-0000-4000-8000-' + String(n).padStart(12, '0'), text, outgoing, delivery };
}
function page(items = [], cursor = null, context = 2) { return { context, page: { kind: 'history', items, nextCursor: cursor } }; }
function fixture(history = async () => page(), photo = null) {
  const nodes = new Map(['voiceBtn', 'chatArea'].map(id => [id, new Element()]));
  const byId = id => {
    if (!nodes.has(id)) nodes.set(id, Object.assign(new Element(), { id }));
    return nodes.get(id);
  };
  const calls = [], queued = [], listeners = new Map(), writes = [];
  let session = 'session', aborted = false;
  const ctx = { AbortController, token: 'session', authExpiredHandled: false, currentChatID: 17, currentChatType: 'private', chatViewVersion: 1,
    localStorage: { getItem: () => session, setItem: (...args) => writes.push(args) },
    window: { zchatDesktop: { e2ee: { compiled: true, history: async (...args) => { calls.push(args); return history(...args); } } }, addEventListener: (name, fn) => listeners.set(name, fn) },
    document: { createElement: () => new Element(), getElementById: byId }, byId,
    chatHistoryRequests: new Map([[17, { controller: { abort: () => { aborted = true; } } }]]), clearTypingDisplay() {}, closeInfoPanel() {},
    messagesEl: new Element(), chatNameEl: new Element(), chatStatusEl: new Element(),
    messageInput: Object.assign(new Element(), { value: 'must stay out of ordinary queue' }), sendBtn: new Element(),
    queueOrdinaryMessage: (...args) => { queued.push(args); return true; }, clearTimeout() {}, typingTimeout: 0,
    clearE2eeComposer() {}, bindE2eeComposer() {},
  };
  runInNewContext(code, ctx); runInNewContext(send, ctx);
  if (photo) {
    const preferences = new Map(Object.entries(photo.preferences || {}));
    const { e2ee } = ctx.window.zchatDesktop;
    Object.assign(e2ee, { preparePhoto: photo.prepare, attachmentInput: photo.lookup, pendingAttachments: photo.list });
    Object.assign(ctx, {
      TextEncoder, crypto: { randomUUID: photo.nextId }, accountStorageKey: name => name + ':1',
      e2eeRecoverySendBlock: () => photo.block ?? null,
      imageByteLabel: size => String(size) + ' байт',
      renderE2eeComposer() { if (ctx.activeView()) ctx.renderE2eePhoto(ctx.activeView()); },
      confirm: () => photo.confirm ?? true,
    });
    ctx.localStorage.getItem = name => name === 'token' ? session : preferences.get(name);
    ctx.localStorage.setItem = (name, value) => { writes.push([name, value]); preferences.set(name, value); };
    ctx.bindE2eeComposer = ctx.renderE2eeComposer;
    runInNewContext(photoCode + '\nthis.activeView=()=>e2eeHistoryView;', ctx);
  }
  return { ctx, nodes, byId, calls, queued, writes, get aborted() { return aborted; },
    changeAccount() { session = 'other'; listeners.get('storage')({ key: 'token' }); }, leave() { ctx.leaveE2eeView(); } };
}
const PHOTO_PREF = 'zchat_e2ee_photo_quality_v1:1';
const SIZE_MB = 1024 * 1024;
function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { resolve, promise };
}
function fileComposer(options = {}) {
  const f = fixture(), sent = [], lookedUp = [];
  const reply = (target, context, kind = 'prepared', epoch = target.expectedEpoch) => ({ context, attachment: {
    conversationId: target.conversationId, messageId: target.messageId, kind, ...(kind === 'vacant' ? {} : { cryptoEpoch: epoch }),
  } });
  let next = 0;
  Object.assign(f.ctx, { TextEncoder, confirm: () => true, imageByteLabel: size => size + ' Б',
    e2eeRecoverySendBlock: () => options.block ?? null, crypto: { randomUUID: () => item(++next).messageId },
    renderE2eeComposer() { const view = f.ctx.activeView(); if (view) f.ctx.renderE2eeFile(view); },
  });
  f.ctx.bindE2eeComposer = f.ctx.renderE2eeComposer;
  Object.assign(f.ctx.window.zchatDesktop.e2ee, {
    prepareFile: async (attempt, context, signal) => {
      sent.push({ attempt, context, signal });
      return options.prepare ? options.prepare(attempt, context) : reply(attempt, context);
    },
    attachmentInput: async (target, context) => {
      lookedUp.push(target);
      return reply(target, context, options.retained ? 'prepared' : 'vacant', '0');
    },
  });
  runInNewContext(fileCode + '\nthis.activeView=()=>e2eeHistoryView;', f.ctx);
  return { ...f, sent, lookedUp, async open(state) { f.ctx.openE2eeConversation(metadata(state), 'Bob'); await tick(); },
    async pick(file = new File(['payload'], 'file.txt')) {
      await f.byId('e2eeFilePick').click();
      const selector = f.byId('e2eeFileInput');
      selector.files = [file];
      selector.listeners.get('change')({ currentTarget: selector });
      await tick();
    },
  };
}

test('E2EE file selection requires confirmation, validates size and uses a private attachment draft', async () => {
  const f = fileComposer();
  await f.open('closed');
  assert.equal(f.byId('e2eeFilePick').disabled, true);
  await f.open();
  await f.pick(new File([], 'empty.txt'));
  assert.match(f.byId('e2eeFileNotice').textContent, /50 МБ/);
  await f.pick();
  assert.equal(f.byId('e2eeFileDialog').open, true);
  assert.equal(f.sent.length, 0);
  f.byId('e2eeFileCaption').value = 'private caption';
  await f.byId('e2eeFileSend').click();
  assert.equal(f.sent.length, 1);
  assert.equal(f.sent[0].attempt.caption, 'private caption');
  assert.equal(f.sent[0].attempt.expectedEpoch, '0');
  assert.equal(f.byId('e2eeFileDialog').open, false);
  assert.match(f.byId('e2eeFileNotice').textContent, /ожидает отправки/);
  assert.deepEqual(f.writes, []);
  assert.deepEqual(f.queued, []);
});

test('E2EE file retry retains UUID and caption and cannot prepare a vacant file in a new epoch', async () => {
  const opts = { prepare: async () => { throw 'native_wait'; } };
  const f = fileComposer(opts);
  await f.open(); await f.pick();
  f.byId('e2eeFileCaption').value = 'original';
  await f.byId('e2eeFileSend').click();
  f.byId('e2eeFileCaption').value = 'changed';
  await f.byId('e2eeFileSend').click();
  assert.equal(f.sent.length, 2);
  assert.equal(f.sent[0].attempt, f.sent[1].attempt);
  assert.equal(f.sent[1].attempt.caption, 'original');
  assert.equal(f.lookedUp[0].messageId, f.sent[0].attempt.messageId);
  f.ctx.activeView().recovery = { state: { cryptoEpoch: '1' } };
  await f.byId('e2eeFileSend').click();
  assert.equal(f.sent.length, 2);
  assert.match(f.byId('e2eeFileStatus').textContent, /Сессия шифрования изменилась/);
  opts.retained = true;
  await f.byId('e2eeFileSend').click();
  assert.equal(f.sent.length, 2);
  assert.equal(f.byId('e2eeFileDialog').open, false);
});

test('leaving the E2EE chat aborts pending file input and fences the late result', async () => {
  const gate = deferred();
  const f = fileComposer({ prepare: () => gate.promise });
  await f.open(); await f.pick();
  await f.byId('e2eeFileSend').click();
  f.changeAccount();
  const [{ attempt, context, signal }] = f.sent;
  assert.equal(signal.aborted, true);
  gate.resolve({ context, attachment: { conversationId: CHAT, messageId: attempt.messageId, kind: 'prepared', cryptoEpoch: '0' } });
  await tick();
  assert.equal(f.byId('e2eeFileDialog').open, false);
  assert.equal(f.byId('e2eeFileNotice').textContent, '');
  assert.deepEqual(f.writes, []);
});

function voiceComposer() {
  const f = fileComposer();
  const permission = deferred(), sent = [];
  let tracksStopped = 0, created = 0, recorder;
  class Recorder {
    static isTypeSupported(type) { return type === 'audio/webm;codecs=opus'; }
    state = 'inactive';
    constructor(_stream, options) { this.mimeType = options.mimeType; created++; recorder = this; }
    start(slice) { assert.equal(slice, 1000); this.state = 'recording'; }
    stop() { this.state = 'inactive'; queueMicrotask(() => this.onstop?.()); }
    chunk(data) { this.ondataavailable?.({ data }); }
  }
  Object.assign(f.ctx, { MediaRecorder: Recorder, Blob, File, DOMException, optionalById: () => null,
    navigator: { mediaDevices: { getUserMedia: () => permission.promise } },
    renderE2eeComposer() { const view = f.ctx.activeView(); if (view) { f.ctx.renderE2eeFile(view); f.ctx.renderE2eeVoice(view); } },
  });
  f.ctx.bindE2eeComposer = f.ctx.renderE2eeComposer;
  f.ctx.window.zchatDesktop.e2ee.prepareVoice = async (attempt, context) => {
    sent.push(attempt);
    return { context, attachment: { conversationId: attempt.conversationId, messageId: attempt.messageId, kind: 'prepared', cryptoEpoch: attempt.expectedEpoch } };
  };
  runInNewContext(recorderCode + '\n' + voiceCode, f.ctx);
  return { ...f, sent, get tracksStopped() { return tracksStopped; }, get created() { return created; },
    grant() { permission.resolve({ getTracks: () => [{ stop() { tracksStopped++; } }] }); },
    chunk(data) { recorder.chunk(data); },
  };
}

test('E2EE voice from the microphone requests confirmation and sends only via the native attachment API', async () => {
  const f = voiceComposer();
  await f.open();
  await f.byId('e2eeVoiceRecord').click();
  f.grant(); await tick();
  f.chunk(new Blob(['audio-a'])); f.chunk(new Blob(['audio-b']));
  await f.byId('e2eeVoiceRecord').click();
  assert.equal(f.tracksStopped, 1);
  assert.equal(f.sent.length, 0);
  assert.equal(f.byId('e2eeFileDialog').open, true);
  await f.byId('e2eeFileSend').click();
  assert.equal(f.sent.length, 1);
  assert.equal(f.sent[0].file.type, 'audio/webm');
  assert.equal(await f.sent[0].file.text(), 'audio-aaudio-b');
  assert.deepEqual(f.writes, []);
  assert.deepEqual(f.queued, []);
});

test('stale microphone permission and oversized recording release all tracks without opening a send dialog', async () => {
  const stale = voiceComposer();
  await stale.open(); await stale.byId('e2eeVoiceRecord').click();
  stale.changeAccount(); stale.grant(); await tick();
  assert.equal(stale.created, 0);
  assert.equal(stale.tracksStopped, 1);
  const oversized = voiceComposer();
  await oversized.open(); await oversized.byId('e2eeVoiceRecord').click();
  oversized.grant(); await tick();
  oversized.chunk({ size: 10 * 1024 * 1024 + 1 });
  await tick();
  assert.equal(oversized.tracksStopped, 1);
  assert.equal(oversized.byId('e2eeFileDialog').open, false);
  assert.match(oversized.byId('e2eeFileNotice').textContent, /10 МБ/);
});
const incomingFile = (n = 1, cacheState = 'missing') => ({ ...item(n), cacheState,
  attachment: { kind: 'file', filename: 'file.txt', size: 9, caption: '', mimeType: 'application/octet-stream', photoMetadataStripped: null } });

test('E2EE history downloads on explicit click and displays only a durable scoped result', async () => {
  const file = incomingFile();
  const f = fixture(async () => page([file, incomingFile(2, 'removed'), { ...incomingFile(3), outgoing: true, cacheState: undefined }]));
  const gate = deferred(), calls = [];
  f.ctx.window.zchatDesktop.e2ee.downloadAttachment = (target, context) => { calls.push([target, context]); return gate.promise; };
  assert.equal(f.ctx.openE2eeConversation(metadata(), 'Bob'), true);
  await tick();
  assert.equal(calls.length, 0);
  const [outgoing, removed, pending] = f.byId('e2eeHistory').children;
  assert.equal(outgoing.children.length, 3);
  assert.equal(removed.children[3].children[0].hidden, true);
  const [button, notice] = pending.children[3].children;
  await button.click();
  await button.click();
  assert.equal(calls.length, 1);
  assert.equal(calls[0][0].conversationId, CHAT);
  assert.equal(calls[0][0].messageId, file.messageId);
  assert.equal(calls[0][1], 2);
  assert.equal(notice.textContent, 'Скачиваем…');
  gate.resolve({ context: 2, attachment: calls[0][0] });
  await tick();
  assert.equal(button.hidden, true);
  assert.equal(notice.textContent, 'Сохранено на этом устройстве');
  f.ctx.openE2eeConversation(metadata(), 'Bob');
  await tick();
  assert.equal(f.byId('e2eeHistory').children[2].children[3].children[0].hidden, true);
  assert.equal(calls.length, 1);
});

test('E2EE download failure keeps the same message available for explicit retry', async () => {
  const f = fixture(async () => page([incomingFile()]));
  const requests = [];
  f.ctx.window.zchatDesktop.e2ee.downloadAttachment = async (target, context) => {
    requests.push(target.messageId);
    if (requests.length === 1) throw 'attachment_unavailable';
    return { context, attachment: target };
  };
  f.ctx.openE2eeConversation(metadata(), 'Bob');
  await tick();
  const [button, notice] = f.byId('e2eeHistory').children[0].children[3].children;
  await button.click();
  assert.match(notice.textContent, /недоступен на сервере/);
  assert.equal(button.disabled, false);
  await button.click();
  assert.deepEqual(requests, [item(1).messageId, item(1).messageId]);
  assert.equal(notice.textContent, 'Сохранено на этом устройстве');
});

test('leaving E2EE history or switching accounts cancels download and discards late success', async () => {
  for (const leave of ['leave', 'changeAccount']) {
    const file = incomingFile(), gate = deferred(), f = fixture(async () => page([{ ...file }]));
    let signal;
    f.ctx.window.zchatDesktop.e2ee.downloadAttachment = (_target, _context, bound) => { signal = bound; return gate.promise; };
    f.ctx.openE2eeConversation(metadata(), 'Bob');
    await tick();
    await f.byId('e2eeHistory').children[0].children[3].children[0].click();
    f[leave]();
    assert.equal(signal.aborted, true);
    gate.resolve({ context: 2, attachment: { conversationId: CHAT, messageId: file.messageId } });
    await tick();
    assert.equal(f.byId('e2eeHistory').children.length, 0);
    assert.deepEqual(f.writes, []);
  }
});

test('saved photo opens explicit in-memory preview and revokes it when the session changes', async () => {
  const photo = incomingFile(1, 'saved');
  photo.attachment = { ...photo.attachment, kind: 'photo', filename: 'image.png', mimeType: 'image/png' };
  const f = fixture(async () => page([photo]));
  let requested = 0;
  const revoked = [];
  Object.assign(f.ctx, { atob, Uint8Array, Blob, URL: {
    createObjectURL(blob) { assert.equal(blob.type, 'image/png'); return 'blob:preview'; },
    revokeObjectURL: url => revoked.push(url),
  } });
  const api = f.ctx.window.zchatDesktop.e2ee;
  api.previewAttachment = async (target, context) => {
    requested++;
    assert.equal(target.generationId, null);
    return { target, context, result: { kind: 'preview', png: 'iVBORw0KGgo=' } };
  };
  api.exportAttachment = async (target, context, suggestedName) => {
    assert.equal(suggestedName, 'image.png');
    return { target, context, result: { kind: 'cancelled' } };
  };
  const image = f.byId('e2eeAttachmentPreviewImage');
  Object.assign(image, { naturalWidth: 1920, naturalHeight: 1080, decode: async () => {} });
  let ratio;
  image.style.setProperty = (name, value) => { assert.equal(name, '--e2ee-photo-ratio'); ratio = Number(value); };
  f.ctx.openE2eeConversation(metadata(), 'Bob');
  await tick();
  const [preview, save, notice] = f.byId('e2eeHistory').children[0].children[3].children[2].children;
  assert.equal(requested, 0);
  await save.click();
  assert.equal(notice.textContent, '');
  await preview.click();
  assert.equal(f.byId('e2eeAttachmentPreview').open, true);
  assert.equal(ratio, 1920 / 1080);
  assert.equal(f.byId('e2eeAttachmentPreviewImage').src, 'blob:preview');
  f.changeAccount();
  assert.equal(f.byId('e2eeAttachmentPreview').open, false);
  assert.equal(f.byId('e2eeAttachmentPreviewImage').src, undefined);
  assert.deepEqual(revoked, ['blob:preview']);
  assert.deepEqual(f.writes, []);
});

test('saved E2EE voice plays inline, survives new messages and detaches its binary URL at view exit', async () => {
  const voice = incomingFile(1, 'saved');
  voice.attachment = { ...voice.attachment, kind: 'voice', filename: 'voice.webm', mimeType: 'audio/webm' };
  Object.assign(voice, { cryptoEpoch: '0', createdAtUnixMs: '1' });
  let history = [voice];
  const f = fixture(async () => page(history));
  const revoked = [];
  let calls = 0, played = 0, paused = 0, reset = 0;
  Object.assign(f.ctx, { Blob, URL: { createObjectURL(blob) { assert.equal(blob.type, 'audio/webm'); return 'blob:voice'; }, revokeObjectURL: url => revoked.push(url) } });
  const api = f.ctx.window.zchatDesktop.e2ee;
  api.previewAttachment = api.exportAttachment = () => assert.fail('voice requests its own verified read');
  api.listenAttachment = async (target, context) => { calls++; return { target, context, result: { kind: 'voice', mimeType: 'audio/webm' }, audio: new Uint8Array([1, 2]) }; };
  f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
  const transcript = f.byId('e2eeHistory');
  const row = transcript.children[0];
  const [listen, audio] = row.children[3].children[2].children;
  audio.play = async () => { played++; }; audio.pause = () => paused++; audio.load = () => reset++;
  assert.equal(calls, 0);
  assert.equal(audio.hidden, true);
  await listen.click();
  assert.equal(played, 1);
  assert.equal(audio.hidden, false);
  assert.equal(listen.hidden, true);
  assert.equal(f.byId('e2eeAttachmentPreview').open, false);
  assert.equal(audio.src, 'blob:voice');
  history = [Object.assign(item(2), { cryptoEpoch: '0', createdAtUnixMs: '2' }), voice];
  await runInNewContext('refreshE2eeHistory(e2eeHistoryView, () => true)', f.ctx);
  assert.equal(transcript.children.length, 2);
  assert.equal(transcript.children[0], row);
  assert.equal(audio.src, 'blob:voice');
  f.leave();
  assert.equal(paused, 1); assert.equal(reset, 1);
  assert.equal(audio.src, undefined); assert.deepEqual(revoked, ['blob:voice']);
});
function photoFixture(options = {}) {
  const prepared = [], lookedUp = [], pagesRead = [];
  let next = 0;
  const reflect = (intent, context, kind = 'prepared', extra = {}) => ({
    context, attachment: { kind, conversationId: intent.conversationId, messageId: intent.messageId, ...extra },
  });
  const config = {
    preferences: options.preferences,
    nextId: () => item(++next).messageId,
    prepare: async (message, context) => {
      prepared.push({ message, context });
      return options.prepare ? options.prepare(message, context) : reflect(message, context);
    },
    lookup: async (message, context) => {
      lookedUp.push({ message, context });
      return options.lookup ? options.lookup(message, context) : reflect(message, context, 'vacant');
    },
    list: async (chat, context, cursor) => {
      pagesRead.push({ chat, context, cursor });
      return options.list ? options.list(chat, context, cursor) : { context, page: { items: [], nextCursor: null } };
    },
  };
  const f = fixture(async () => page(), config);
  const get = key => f.byId('e2eePhoto' + key);
  const open = ({ state = 'active', chat = CHAT, context = 2 } = {}) => {
    const reply = metadata(state);
    reply.context = context; reply.conversation.conversationId = chat;
    f.ctx.openE2eeConversation(reply, 'Bob');
  };
  const select = async (size = SIZE_MB, name = 'photo.png') => {
    await get('Pick').click();
    const input = get('File');
    input.files = [{ name, size }];
    input.listeners.get('change')({ currentTarget: input });
    return input.files[0];
  };
  const change = (key, value) => {
    if (typeof value === 'boolean') get(key).checked = value;
    else get(key).value = value;
    get('Dialog').listeners.get('input')({ target: get(key) });
  };
  return { ...f, get, open, select, change, prepared, lookedUp, pagesRead, reflect, config };
}
const cleanChoice = (quality = null, maxDimension = 0) => ({ mode: 'clean', quality, maxDimension });
test('photo picker defaults to cleanup; original requires fresh consent and is never remembered', async () => {
  const f = photoFixture({ preferences: { 'zchat_image_send_v1:1': '{"version":1,"mode":"original"}' } });
  f.open();
  await f.select();
  assert.equal(f.get('Mode').value, 'clean');
  assert.equal(f.prepared.length, 0);
  f.change('Mode', 'original');
  assert.equal(f.get('OriginalField').hidden, false);
  assert.equal(f.get('Send').disabled, true);
  f.change('Original', true);
  f.change('Mode', 'clean');
  f.change('Mode', 'original');
  assert.equal(f.get('Send').disabled, true);
  f.change('Original', true);
  await f.get('Send').click();
  assert.equal(f.prepared[0].message.choice.mode, 'original');
  assert.equal(f.prepared[0].message.choice.metadataWarningAcknowledged, true);
  assert.equal(f.writes.length, 0);
  assert.match(f.get('Notice').textContent, /подготовлено на этом устройстве/);
  assert.equal(f.pagesRead.length, 2);
});
test('saved settings apply to small photos and are shown for large photos before confirmation', async () => {
  const f = photoFixture({ preferences: { [PHOTO_PREF]: JSON.stringify(cleanChoice(75, 2560)) } });
  f.open();
  await f.select(5 * SIZE_MB);
  assert.equal(f.get('Settings').open, false);
  assert.equal(f.get('Quality').value, '75');
  assert.match(f.get('SettingsSummary').textContent, /Метаданные удаляются, качество 75/);
  await f.get('Cancel').click();
  await f.select(5 * SIZE_MB + 1);
  assert.equal(f.get('Settings').open, true);
  assert.equal(f.prepared.length, 0);
  f.change('Quality', '65');
  f.change('Size', '1600');
  f.change('Caption', 'Подпись');
  f.change('Remember', true);
  await f.get('Send').click();
  assert.deepEqual(JSON.parse(f.writes[0][1]), cleanChoice(65, 1600));
  assert.deepEqual(JSON.parse(JSON.stringify(f.prepared[0].message.choice)), cleanChoice(65, 1600));
  assert.equal(f.prepared[0].message.caption, 'Подпись');
});
test('photo with a lost confirmation is reconciled by UUID before reading the source again', async () => {
  const f = photoFixture({
    prepare: async () => { throw 'native_wait'; },
    lookup: (message, context) => f.reflect(message, context, 'outgoing', { storedOnServer: true, sealedUnsent: false }),
  });
  f.open();
  await f.select();
  await f.get('Send').click();
  assert.equal(f.get('Caption').disabled, true);
  const id = f.prepared[0].message.messageId;
  f.open();
  assert.equal(f.get('Pick').textContent, 'Проверить фото');
  await f.get('Pick').click();
  await f.get('Send').click();
  assert.equal(f.lookedUp[0].message.messageId, id);
  assert.equal(f.prepared.length, 1);
  assert.match(f.get('Notice').textContent, /сохранено на сервере/);
});
test('a vacant retry keeps immutable input and stops when the encryption epoch changes', async () => {
  const f = photoFixture({ prepare: async (message, context) => {
    if (f.prepared.length === 1) throw 'native_wait';
    return f.reflect(message, context);
  } });
  f.open();
  const view = f.ctx.activeView();
  view.recovery = { state: { cryptoEpoch: '0' } };
  await f.select();
  f.change('Caption', 'Исходная подпись');
  await f.get('Send').click();
  f.get('Caption').value = 'Изменённая подпись';
  view.recovery.state.cryptoEpoch = '1';
  await f.get('Send').click();
  assert.match(f.get('Status').textContent, /Сессия шифрования изменилась/);
  assert.equal(f.prepared.length, 1);
  view.recovery.state.cryptoEpoch = '0';
  await f.get('Send').click();
  assert.equal(f.prepared.length, 2);
  assert.strictEqual(f.prepared[1].message, f.prepared[0].message);
  assert.equal(f.prepared[1].message.caption, 'Исходная подпись');
});
test('actual codec rejection allows new settings while a recovery block prevents another send', async () => {
  const f = photoFixture({ prepare: async () => { throw 'photo_output_too_large'; } });
  f.open();
  await f.select();
  await f.get('Send').click();
  assert.match(f.get('Status').textContent, /10 МБ/);
  assert.equal(f.get('Mode').disabled, false);
  f.config.block = 'Восстановление';
  f.ctx.renderE2eeComposer();
  await f.get('Send').click();
  assert.equal(f.get('Send').disabled, true);
  assert.equal(f.prepared.length, 1);
});
test('photo picker and delayed preparation are fenced on chat switch and logout', async () => {
  const response = deferred();
  const f = photoFixture({ prepare: () => response.promise });
  f.open({ state: 'closed' });
  assert.equal(f.get('Pick').disabled, true);
  f.open();
  await f.get('Pick').click();
  f.open({ chat: item(20).messageId });
  const input = f.get('File');
  input.files = [{ name: 'stale.png', size: SIZE_MB }];
  input.listeners.get('change')({ currentTarget: input });
  assert.equal(f.get('Dialog').open, false);
  await f.select();
  f.change('Remember', true);
  await f.get('Send').click();
  await f.get('Cancel').click();
  assert.equal(f.get('Dialog').open, true);
  assert.equal(f.prepared.length, 1);
  f.changeAccount();
  response.resolve(f.reflect(f.prepared[0].message, 2));
  await tick();
  assert.equal(f.get('Dialog').open, false);
  assert.equal(f.get('Name').textContent, '');
  assert.equal(f.get('Notice').textContent, '');
  assert.equal(f.writes.length, 0);
});
test('photo preparation refreshes pending rows even if a previous page is still loading', async () => {
  const older = deferred();
  const f = photoFixture({ list: async (_, context) => f.pagesRead.length === 1
    ? older.promise : { context, page: { items: [], nextCursor: null } } });
  f.open();
  await f.select();
  await f.get('Send').click();
  assert.equal(f.pagesRead.length, 1);
  older.resolve({ context: 2, page: { items: [], nextCursor: CURSOR } });
  await tick();
  assert.equal(f.pagesRead.length, 2);
  assert.equal(f.pagesRead[1].cursor, null);
});
test('prepared E2EE attachments reappear on reopen and move into history without showing duplicates', async () => {
  const attachment = { kind: 'photo', filename: '<camera>.png', caption: '<img src=example>', size: 1024 };
  const prepared = { messageId: item(1).messageId, attachment };
  let promoted = false;
  const f = fixture(async () => promoted ? page([{ ...item(1, '', true), attachment }]) : page());
  const calls = [];
  f.ctx.window.zchatDesktop.e2ee.pendingAttachments = async (...args) => {
    calls.push(args);
    // The inventory and history can cross a promotion between reads.
    return { context: 2, page: { items: [prepared], nextCursor: null } };
  };
  for (let i = 0; i < 2; i++) {
    f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
    assert.equal(f.nodes.get('e2eePendingAttachments').hidden, false);
    assert.equal(f.nodes.get('e2eePendingAttachmentList').children[0].textContent, 'Фото · <camera>.png · 1 КБ\n<img src=example>');
    f.leave();
  }
  promoted = true;
  f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
  assert.equal(f.nodes.get('e2eePendingAttachments').hidden, true);
  assert.equal(f.nodes.get('e2eeHistory').children[0].children[1].textContent, 'Фото · <camera>.png · 1 КБ\n<img src=example>');
  assert.deepEqual(calls, Array.from({ length: 3 }, () => [CHAT, 2, null]));
});
test('a late pending attachment page after logout cannot render private filenames', async () => {
  const f = fixture();
  let resolve;
  f.ctx.window.zchatDesktop.e2ee.pendingAttachments = () => new Promise(done => { resolve = done; });
  f.ctx.openE2eeConversation(metadata(), 'Bob');
  f.changeAccount();
  resolve({ context: 2, page: { items: [{ messageId: item(1).messageId, attachment: { kind: 'file', filename: 'private', size: 4 } }], nextCursor: null } });
  await tick();
  assert.equal(f.nodes.get('e2eePendingAttachments').hidden, true);
  assert.equal(f.nodes.get('e2eePendingAttachmentList').children.length, 0);
});
test('opening a UUID view clears numeric selection/old requests; ordinary composer can never queue its text', async () => {
  const f = fixture(); f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
  assert.equal(f.ctx.currentChatID, null); assert.equal(f.ctx.currentChatType, 'e2ee'); assert.equal(f.aborted, true);
  assert.equal(f.nodes.get('chatArea').dataset.chatMode, 'e2ee');
  await f.ctx.sendMessage(); assert.equal(f.queued.length, 0); assert.equal(f.writes.length, 0);
  assert.deepEqual(f.calls, [[CHAT, null]]); assert.match(f.nodes.get('e2eeHistoryNotice').textContent, /пока нет/);
});
test('native history renders as literal text oldest-first, keeps long texts and paginates without ordinary HTTP', async () => {
  const bad = '<img src=x onerror=alert(1)>', long = '\u0001'.repeat(2000);
  const f = fixture(async (_, cursor) => cursor ? page([item(1, long)]) : page([item(3, bad), item(2)], CURSOR));
  f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
  const rows = f.nodes.get('e2eeHistory'); assert.equal(rows.children[1].children[1].textContent, bad);
  await f.nodes.get('e2eeHistoryMore').click();
  assert.equal(rows.children.length, 3); assert.equal(rows.children[0].children[1].textContent, long);
  assert.deepEqual(f.calls, [[CHAT, null], [CHAT, CURSOR]]);
  assert.equal(f.nodes.get('e2eeHistoryMore').hidden, true);
});
test('a recorder starting during creation cannot be hidden by the final view transition', async () => {
  const f = fixture(); f.nodes.get('voiceBtn').classList.contains = name => name === 'recording';
  assert.equal(f.ctx.openE2eeConversation(metadata(), 'Bob'), false);
  assert.equal(f.ctx.currentChatID, 17); assert.equal(f.calls.length, 0); assert.equal(f.nodes.get('chatArea').dataset.chatMode, undefined);
});
test('closed conversation stays read-only; a late recipient ACK outranks expiry without claiming read status', async () => {
  const delivery = { sealedUnsent: false, storedOnServer: true, state: 'recipient_stored', removal: 'expired' };
  const f = fixture(async () => page([item(1, 'old history', true, delivery)]));
  f.ctx.openE2eeConversation(metadata('closed'), 'Bob'); await tick();
  assert.match(f.nodes.get('e2eeConversationState').textContent, /диалог закрыт/);
  const status = f.nodes.get('e2eeHistory').children[0].children[2];
  assert.equal(status.textContent, 'Сохранено получателем'); assert.match(status.title, /Срок хранения/);
  assert.equal(f.ctx.e2eeDeliveryLabel({ sealedUnsent: true }), 'Не отправлено');
});
test('switch/close/account change discards an in-flight plaintext reply and clears all rendered history', async () => {
  for (const leave of ['leave', 'changeAccount']) {
    let finish; const f = fixture(() => new Promise(resolve => { finish = resolve; }));
    f.ctx.messagesEl.append(new Element()); f.ctx.chatNameEl.textContent = 'old ordinary chat';
    f.ctx.openE2eeConversation(metadata(), 'Bob'); f[leave](); finish(page([item(1, 'must not render')])); await tick();
    assert.equal(f.nodes.get('e2eeHistory').children.length, 0); assert.equal(f.nodes.get('e2eeConversationTitle').textContent, '');
    assert.equal(f.nodes.get('e2eeConversationView').hidden, true);
    if (leave === 'changeAccount') {
      assert.equal(f.ctx.messagesEl.children.length, 0); assert.equal(f.ctx.messageInput.value, '');
      assert.equal(f.ctx.chatNameEl.textContent, ''); assert.equal(f.writes.length, 0);
    }
  }
});
test('context mismatch/stale cursor drops history and refresh restarts at latest without mutation', async () => {
  let calls = 0;
  const f = fixture(async (_, cursor) => { calls++; if (cursor) throw 'stale'; return calls === 3 ? page([], null, 3) : page([item(1)], CURSOR); });
  f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick(); await f.nodes.get('e2eeHistoryMore').click();
  assert.equal(f.nodes.get('e2eeHistory').children.length, 0); assert.match(f.nodes.get('e2eeHistoryNotice').textContent, /Состояние истории/);
  await f.nodes.get('e2eeHistoryRefresh').click();
  assert.equal(f.calls[2][1], null); assert.equal(f.nodes.get('e2eeHistory').children.length, 0);
});
test('a newer selected view cannot be overwritten by the older view response', async () => {
  const finishes = [];
  const f = fixture(() => new Promise(resolve => finishes.push(resolve)));
  f.ctx.openE2eeConversation(metadata(), 'Bob');
  f.ctx.openE2eeConversation(metadata(), 'New selection');
  finishes[1](page([item(2, 'current')])); await tick();
  finishes[0](page([item(1, 'stale')])); await tick();
  assert.equal(f.nodes.get('e2eeConversationTitle').textContent, 'New selection');
  assert.equal(f.nodes.get('e2eeHistory').children.length, 1);
  assert.equal(f.nodes.get('e2eeHistory').children[0].children[1].textContent, 'current');
});
test('older history remains browsable while visible plaintext/DOM stay bounded to 128 whole texts', async () => {
  let n = 0;
  const f = fixture(async () => { const items = Array.from({ length: 8 }, () => item(++n)); return page(items, CURSOR); });
  f.ctx.openE2eeConversation(metadata(), 'Bob'); await tick();
  for (let i = 0; i < 19; i++) await f.nodes.get('e2eeHistoryMore').click();
  assert.equal(f.nodes.get('e2eeHistory').children.length, 128); assert.equal(f.nodes.get('e2eeHistoryMore').hidden, false);
  assert.equal(f.calls.length, 20); assert.match(f.nodes.get('e2eeHistoryNotice').textContent, /128/);
  await tick(); assert.equal(f.calls.length, 20, 'only explicit pagination reads more history');
});
