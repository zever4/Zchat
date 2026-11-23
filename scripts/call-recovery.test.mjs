import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

const source = ts.transpile(readFileSync(new URL('../web/src/chat/050-join-livekit-room.ts', import.meta.url), 'utf8'));

// Exercise the real join/event handlers with the SDK event ordering observed
// during a full SFU restart. Network/media recovery itself needs a real SFU.
async function fixture(desktop) {
  let now = 0, next = 0;
  const timers = new Map();
  const setTimeout = (fn, ms) => { const id = ++next; timers.set(id, { fn, at: now + ms }); return id; };
  const clearTimeout = id => timers.delete(id);
  const mic = { stopped: false, stop() { this.stopped = true; } };
  class Room extends EventEmitter {
    state = 'disconnected';
    remoteParticipants = new Map([['peer', {}]]);
    localParticipant = { publishTrack: async () => {} };
    async connect(url, token) { this.destination = url; this.grant = token; this.state = 'connected'; }
    async disconnect() { this.state = 'disconnected'; mic.stop(); }
  }
  const events = Object.fromEntries(['TrackSubscribed', 'TrackUnsubscribed', 'ParticipantConnected',
    'ParticipantDisconnected', 'Disconnected', 'Reconnecting', 'SignalReconnecting', 'Reconnected']
    .map(name => [name, name]));
  const noop = () => {};
  const ctx = {
    Promise, AbortController, console, clearTimeout, window: { setTimeout, zchatDesktop: desktop },
    localStorage: { getItem: () => 'token' }, token: 'token', authExpiredHandled: false,
    callAttempt: 0, callChatID: 42, callState: 'calling', callRoom: null, callJoinRequest: null,
    incomingCallData: null, callPeerLeftTimer: null, callInviteTimer: null, callRecoveryTimer: null,
    chats: [{ id: 42, type: 'private' }], localTracks: {}, status: '', notices: [], reconciled: [],
    LivekitClient: { Room, RoomEvent: events, createLocalAudioTrack: async () => mic,
      Track: { Source: { Microphone: 'microphone' } }, DisconnectReason: { CONNECTION_FAILED: 12 } },
    apiFetch: async () => ({ ok: true, json: async () => ({ url: 'ws://test.invalid', token: 'grant' }) }),
    showCallOverlay: noop, setCallConnectionStatus: message => { ctx.status = message; },
    loadCallParticipantDirectory: async () => {}, loadCallScreenShareDraftFromSettings: noop,
    isRadioMicrophoneMode: () => false, addParticipantToCallUI: participant => ctx.reconciled.push(participant),
    removeParticipantFromCallUI: noop, updateVideoGridLayout: noop,
    renderSystemMessage: message => ctx.notices.push(message), alert: noop,
    cleanupCall: () => {
      ++ctx.callAttempt; clearTimeout(ctx.callRecoveryTimer); ctx.callRecoveryTimer = null;
      ctx.callState = 'idle'; ctx.callRoom?.disconnect(); ctx.callRoom = null;
    },
  };
  ctx.endCall = ctx.cleanupCall;
  runInNewContext(source + '\nthis.join = joinLivekitRoom;', ctx);
  await ctx.join('audio');
  assert.equal(ctx.callState, 'in_call');
  const room = ctx.callRoom;
  const advance = ms => {
    const end = now + ms;
    for (;;) {
      const pending = [...timers].filter(([, t]) => t.at <= end).sort((a, b) => a[1].at - b[1].at)[0];
      if (!pending) break;
      const [id, timer] = pending;
      timers.delete(id); now = timer.at; timer.fn();
    }
    now = end;
  };
  const leave = () => { room.remoteParticipants.clear(); room.emit(events.ParticipantDisconnected, {}); };
  const recover = () => { room.state = 'connected'; room.emit(events.Reconnected); };
  return { ctx, room, events, mic, advance, leave, recover };
}

test('browser calls retain the server-provided URL and grant', async () => {
  const f = await fixture();
  assert.equal(f.room.destination, 'ws://test.invalid');
  assert.equal(f.room.grant, 'grant');
});

test('desktop calls use its selected gateway without changing the session-bound grant', async () => {
  const f = await fixture({ callGatewayURL: 'wss://chat.example:9443/rtc/' });
  assert.equal(f.room.destination, 'wss://chat.example:9443/rtc/');
  assert.equal(f.room.grant, 'grant');
});

test('reconnected room reconciles already subscribed participants even without a new subscription event', async () => {
  const f = await fixture();
  const participant = f.room.remoteParticipants.get('peer');
  f.recover();
  await Promise.resolve();
  assert.ok(f.ctx.reconciled.includes(participant));
});

test('reconciliation runs after SDK listeners install deferred tracks during Reconnected', async () => {
  const f = await fixture();
  f.room.remoteParticipants.clear();
  const deferredParticipant = { identity: 'deferred-peer' };
  // The pinned SDK registers its deferred onTrackAdded listener after ours.
  f.room.once(f.events.Reconnected, () => f.room.remoteParticipants.set('deferred-peer', deferredParticipant));
  f.recover();
  await Promise.resolve();
  assert.ok(f.ctx.reconciled.includes(deferredParticipant));
});

test('ending a call before deferred reconciliation cannot recreate its media', async () => {
  const f = await fixture();
  f.recover();
  f.ctx.endCall();
  await Promise.resolve();
  assert.equal(f.ctx.reconciled.length, 0);
  assert.equal(f.ctx.callState, 'idle');
  assert.equal(f.mic.stopped, true);
});

test('full reconnect cancels the peer-left timer and preserves microphone ownership', async () => {
  const f = await fixture();
  f.leave(); // SDK clears the old peers before emitting Reconnecting.
  f.room.state = 'reconnecting'; f.room.emit(f.events.Reconnecting);
  f.advance(2500);
  assert.equal(f.ctx.callState, 'in_call'); assert.equal(f.mic.stopped, false);
  assert.match(f.ctx.status, /Восстанавливаем/);
  f.recover();
  f.room.remoteParticipants.set('peer', {}); f.room.emit(f.events.ParticipantConnected, {});
  f.advance(12000);
  assert.equal(f.ctx.callState, 'in_call'); assert.equal(f.ctx.status, '');
});

test('signal reconnect does not interpret temporarily missing peers as a hangup', async () => {
  const f = await fixture();
  f.room.state = 'signalReconnecting'; f.room.emit(f.events.SignalReconnecting); f.leave();
  f.advance(3000);
  assert.equal(f.ctx.callState, 'in_call'); assert.equal(f.mic.stopped, false);
});

test('a genuine peer departure still ends a connected private call', async () => {
  const f = await fixture(); f.leave(); f.advance(2001);
  assert.equal(f.ctx.callState, 'idle'); assert.equal(f.mic.stopped, true);
});

test('reconnected room with no returning peer releases capture after a bounded grace period', async () => {
  const f = await fixture();
  f.room.state = 'reconnecting'; f.leave(); f.recover(); f.advance(10001);
  assert.equal(f.ctx.callState, 'idle'); assert.equal(f.mic.stopped, true);
});

test('late events from an old room cannot end a replacement call', async () => {
  const f = await fixture(); f.ctx.callRoom = { state: 'connected' };
  f.leave(); f.room.emit(f.events.Disconnected, 12); f.advance(5000);
  assert.equal(f.ctx.callState, 'in_call'); assert.equal(f.mic.stopped, false);
});

test('a stalled reconnect has a deadline and releases the microphone', async () => {
  const f = await fixture();
  f.room.state = 'signalReconnecting'; f.room.emit(f.events.SignalReconnecting);
  f.advance(59000); assert.equal(f.ctx.callState, 'in_call');
  f.room.emit(f.events.Reconnecting); // Repeated SDK retries must not extend the deadline.
  f.advance(1001);
  assert.equal(f.ctx.callState, 'idle'); assert.equal(f.mic.stopped, true);
  assert.match(f.ctx.notices[0], /Не удалось восстановить/);
});

test('terminal network failure releases capture and explains that a retry is needed', async () => {
  const f = await fixture(); f.room.emit(f.events.Disconnected, 12);
  assert.equal(f.ctx.callState, 'idle'); assert.equal(f.mic.stopped, true);
  assert.match(f.ctx.notices[0], /Связь.*потеряна/);
});
