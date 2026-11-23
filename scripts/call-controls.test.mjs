import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';

// Load the actual control functions without the unrelated overlay DOM setup.
const file = ts.createSourceFile('controls.ts', readFileSync(new URL('../web/src/chat/052-call-overlay-ui.ts', import.meta.url), 'utf8'), ts.ScriptTarget.Latest, true);
const names = new Set(['setCallMicEnabled', 'syncCallMicrophoneState', 'syncLocalParticipantMedia', 'detachCallParticipantTracks', 'syncCallVideoControls', 'attachCallTrack', 'findAttachedTrackElement']);
const functions = file.statements.filter(s => ts.isFunctionDeclaration(s) && names.has(s.name?.text));
assert.equal(functions.length, names.size);
const source = ts.transpile(functions.map(s => s.getText(file)).join('\n'));

function fixture(muted = true) {
  const publication = { isMuted: muted, track: { mediaStreamTrack: { readyState: 'live' } } };
  const participant = {
    getTrackPublication: () => publication,
    setMicrophoneEnabled: async enabled => { publication.isMuted = !enabled; },
  };
  const room = { localParticipant: participant, disconnect: async () => {} };
  const ctx = { callRoom: room, localTracks: { mic: !muted }, callMicHoldActive: true, notices: [], updates: 0,
    LivekitClient: { Track: { Source: { Microphone: 'microphone' } } },
    updateCallControlButtons: () => { ++ctx.updates; }, updateCallParticipantUI: () => {},
    playCallRadioTone: () => {}, renderSystemMessage: message => ctx.notices.push(message),
    endCall: () => { ctx.callRoom = null; publication.track.mediaStreamTrack.readyState = 'ended'; },
  };
  runInNewContext(source + '\nthis.setMic = setCallMicEnabled; this.syncMic = syncCallMicrophoneState;', ctx);
  return { ctx, participant, publication, room };
}

test('SDK mute/ended state replaces the optimistic microphone flag', () => {
  const f = fixture(); f.ctx.localTracks.mic = true; f.ctx.syncMic();
  assert.equal(f.ctx.localTracks.mic, false);
  f.publication.isMuted = false; f.publication.track.mediaStreamTrack.readyState = 'ended'; f.ctx.syncMic();
  assert.equal(f.ctx.localTracks.mic, false);
});

test('failed capture retry keeps the call listening and the microphone control off', async () => {
  const f = fixture();
  f.participant.setMicrophoneEnabled = async () => { throw new Error('No capture device'); };
  await f.ctx.setMic(true);
  assert.equal(f.ctx.callRoom, f.room); assert.equal(f.ctx.localTracks.mic, false);
  assert.equal(f.ctx.callMicHoldActive, false); assert.ok(f.ctx.updates);
  assert.match(f.ctx.notices[0], /Не удалось включить микрофон/);
});

test('successful capture retry restores the microphone control', async () => {
  const f = fixture(); await f.ctx.setMic(true);
  assert.equal(f.ctx.localTracks.mic, true); assert.equal(f.publication.isMuted, false);
});

test('a failed mute closes the call rather than leaving an active microphone behind an off indicator', async () => {
  const f = fixture(false);
  f.participant.setMicrophoneEnabled = async () => { throw new Error('Mute failed'); };
  await f.ctx.setMic(false);
  assert.equal(f.ctx.callRoom, null); assert.equal(f.publication.track.mediaStreamTrack.readyState, 'ended');
  assert.match(f.ctx.notices[0], /Звонок завершён/);
});

test('completion of an old room operation does not overwrite a replacement call', async () => {
  const f = fixture();
  f.participant.setMicrophoneEnabled = async () => { f.ctx.callRoom = {}; f.ctx.localTracks.mic = false; };
  await f.ctx.setMic(true);
  assert.equal(f.ctx.localTracks.mic, false); assert.equal(f.ctx.updates, 0);
});

test('unpublished local screen leaves no frozen preview or fullscreen control', () => {
  let removed = false, exited = false;
  const video = { srcObject: { id: 'ended-screen-stream' }, pause() { this.paused = true; }, remove() { removed = true; } };
  const button = { hidden: false, setAttribute() {} };
  const tile = {
    querySelectorAll: () => removed ? [] : [video],
    querySelector: selector => selector === 'video' ? (removed ? null : video) : button,
  };
  const participant = { trackPublications: new Map(), getTrackPublication: () => undefined };
  const ctx = {
    callRoom: { localParticipant: participant }, localTracks: { camera: false, screen: false },
    LivekitClient: { Track: { Source: { Microphone: 'mic', ScreenShare: 'screen', Camera: 'camera' } } },
    ensureCallParticipantTile: () => tile,
    getCallParticipantProfile: () => ({ displayName: 'Self', avatarURL: '' }),
    ensureCallParticipantPlaceholder() {}, updateCallParticipantUI() {},
    document: { fullscreenElement: tile, exitFullscreen: async () => { exited = true; } },
  };
  runInNewContext(source + '\nsyncLocalParticipantMedia();', ctx);
  assert.equal(removed, true, 'SDK already removed the publication, but its old video still needs clearing');
  assert.equal(video.srcObject, null);
  assert.equal(video.paused, true);
  assert.equal(button.hidden, true);
  assert.equal(exited, true);
});

test('a matching audio element detached by the SDK is reattached without creating another player', () => {
  const element = { dataset: { trackSid: 'remote-audio' }, srcObject: null };
  const tile = { querySelectorAll: () => [element] };
  let registered = false;
  const track = { kind: 'audio', sid: 'remote-audio', attach(target) {
    assert.equal(target, element);
    target.srcObject = { id: 'recovered-stream' };
    return target;
  } };
  const ctx = { getCallParticipantIdentity: () => 'remote', ensureCallParticipantTile: () => tile,
    registerCallParticipantAudioElement: (identity, audio) => { assert.equal(identity, 'remote'); assert.equal(audio, element); registered = true; },
    track,
  };
  runInNewContext(source + '\nattachCallTrack(track, {}, false);', ctx);
  assert.equal(element.srcObject?.id, 'recovered-stream');
  assert.equal(registered, true);
});
