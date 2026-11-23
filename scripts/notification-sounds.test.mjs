import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const sounds = compile('032-notification-sounds.ts');
const calls = compile('051-incoming-call-handling.ts');
const tick = () => new Promise(resolve => setImmediate(resolve));

function fixture(options = {}) {
  let token = 'session', dnd = false, muted = false, timerID = 0;
  const sources = [], events = new Map(), timers = new Map();
  const signal = { send() {}, readyState: 1 };
  const context = {
    token: 'session', authExpiredHandled: false,
    userSettings: { notification_sound: true },
    localStorage: { getItem: () => token },
    window: { location: { origin: 'https://app.example' }, setTimeout: fn => { timers.set(++timerID, fn); return timerID; }, addEventListener: (key, fn) => events.set(key, fn) },
    document: { addEventListener: (key, fn) => events.set(key, fn) },
    accountStorageKey: base => base + ':4', navigator: {}, URL,
    canShowAppNotifications: () => !dnd, isChatMutedByID: () => muted,
    callState: 'idle', incomingCallData: null, callType: null, callChatID: null,
    socket: signal, WebSocket: { OPEN: 1 }, callUserID: '4', chats: [],
    optionalById: () => null, joinLivekitRoom() {}, clearTimeout: id => timers.delete(id),
    cleanupCall() {
      context.stopAppSound('ring');
      context.callState = 'idle'; context.incomingCallData = null;
    },
  };
  const fakeAudio = {
    state: 'running', sampleRate: 8000, destination: {},
    createBuffer(_channels, length, rate) {
      const data = new Float32Array(length);
      return { duration: length / rate, getChannelData: () => data };
    },
    createGain: () => ({ gain: { value: 1 }, connect() {}, disconnect() {} }),
    createBufferSource: () => ({
      start() { sources.push(this); }, stop() { this.stopped = true; }, connect() {}, disconnect() { this.disconnected = true; },
    }),
    decodeAudioData: options.decode ?? (async () => ({ duration: 2 })),
  };
  context.audio = fakeAudio;
  context.window.AudioContext = function () { return fakeAudio; };
  const vm = (source) => runInNewContext(source, context);
  vm(sounds + calls + '\nappAudio = audio; soundPreferences = Promise.resolve();');
  return {
    context, vm, sources, timers, fakeAudio,
    dnd(value) { dnd = value; }, mute(value) { muted = value; },
    logout() { token = 'other'; events.get('storage')({ key: 'token' }); },
    invite() { context.handleCallInvite({ chat_id: 7, sender_id: 5 }); },
    custom(kind, file) { context.customFile = file; vm('customSounds[' + JSON.stringify(kind) + '] = customFile'); },
  };
}

test('default message is audible, one-shot and coalesced while playing', async () => {
  const f = fixture();
  f.context.playNotificationSound(); f.context.playNotificationSound(); await tick();
  assert.equal(f.sources.length, 1);
  assert.equal(f.sources[0].loop, false);
  assert.ok(f.sources[0].buffer.getChannelData(0).some(sample => sample > 0));
  const old = f.sources[0].onended;
  old(); f.context.playNotificationSound(); await tick();
  old(); assert.equal(f.sources[1].stopped, undefined);
  f.logout(); assert.equal(f.sources[1].stopped, true);
});

test('default ringtone loops once per call and stops on accept, reject, remote hangup and expiry', async () => {
  for (const end of ['accept', 'reject', 'hangup', 'timeout']) {
    const f = fixture(); f.invite(); f.invite(); await tick();
    assert.equal(f.sources.length, 1); assert.equal(f.sources[0].loop, true);
    assert.ok(f.sources[0].buffer.duration > 2);
    if (end === 'accept') f.context.acceptIncomingCall();
    if (end === 'reject') f.context.rejectIncomingCall();
    if (end === 'hangup') f.context.handleCallEnd({ chat_id: 7 });
    if (end === 'timeout') f.timers.values().next().value();
    assert.equal(f.sources[0].stopped, true, end);
  }
});

test('disabled sounds, muted calls and DND leave incoming call UI available without ringing', async () => {
  const f = fixture(); f.mute(true); f.invite(); await tick();
  assert.equal(f.context.callState, 'ringing'); assert.equal(f.sources.length, 0);
  f.mute(false); f.dnd(true); f.context.syncNotificationSoundPolicy(); f.context.playNotificationSound(); await tick();
  assert.equal(f.sources.length, 0);
  f.dnd(false); f.context.syncNotificationSoundPolicy(); await tick(); assert.equal(f.sources.length, 1);
  f.context.userSettings.notification_sound = false;
  f.context.syncNotificationSoundPolicy(); assert.equal(f.sources[0].stopped, true);
});

test('custom sound decodes locally; a cancelled decoding operation cannot ring late', async () => {
  let finish;
  const f = fixture({ decode: () => new Promise(resolve => { finish = resolve; }) });
  f.custom('ring', { size: 7, arrayBuffer: async () => new ArrayBuffer(7) });
  f.invite(); await tick(); f.context.rejectIncomingCall();
  finish({ duration: 2 }); await tick(); assert.equal(f.sources.length, 0);
  f.invite(); await tick(); assert.equal(f.sources.length, 1); assert.equal(f.sources[0].buffer.duration, 2);
});

test('blocked autoplay drops message alerts; protected revisions sound once without reading chat history', async () => {
  const f = fixture(); f.fakeAudio.state = 'suspended';
  f.context.playNotificationSound(); await tick(); assert.deepEqual(f.sources, []);
  f.fakeAudio.state = 'running';
  const update = (context, revision) => f.context.observeProtectedMessageSound({ phase: 'active', runtimePhase: 'Running', context, incomingMessageRevision: revision });
  update(1, '14'); await tick(); assert.equal(f.sources.length, 0);
  update(1, '15'); await tick(); assert.equal(f.sources.length, 1); f.sources[0].onended();
  update(1, '15'); update(2, '9'); await tick(); assert.equal(f.sources.length, 1);
  update(2, '10'); await tick(); assert.equal(f.sources.length, 2);
});
