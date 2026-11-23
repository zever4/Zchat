import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { runInNewContext } from 'node:vm';
import ts from 'typescript';
const compile = name => ts.transpile(readFileSync(new URL('../web/src/chat/' + name, import.meta.url), 'utf8'), { target: ts.ScriptTarget.ES2022 });
const source = compile('025-e2ee-view.ts') + compile('025-e2ee-composer.ts');
const CHAT = '11111111-1111-4111-8111-111111111111', OTHER = '22222222-2222-4222-8222-222222222222';
const tick = () => new Promise(resolve => setImmediate(resolve));
class Element {
  children = []; dataset = {}; style = {}; hidden = false; disabled = false; readOnly = false; value = ''; textContent = ''; scrollTop = 0; listeners = new Map();
  classList = { contains: () => false };
  append(...items) { this.children.push(...items); }
  prepend(item) { this.children.unshift(item); }
  replaceChildren(...items) { this.children = [...items]; }
  get lastElementChild() { return this.children.at(-1); }
  removeChild(item) { this.children = this.children.filter(n => n !== item); }
  get scrollHeight() { return this.children.length * 20; }
  addEventListener(name, fn) { this.listeners.set(name, fn); }
  async click() { if (!this.disabled && !this.hidden) this.listeners.get('click')?.(); await tick(); }
}
function accepted(input, context = 2, sealedUnsent = false) {
  return { context, queued: { conversationId: input.conversationId, messageId: input.messageId, cryptoEpoch: '0', createdAtUnixMs: '1700000000000', storedOnServer: false, sealedUnsent } };
}
async function fixture(options = {}) {
  const nodes = new Map(['e2eeConversationView', 'chatArea', 'e2eeHistory', 'e2eeConversationTitle', 'e2eeConversationState', 'e2eeHistoryNotice', 'e2eeHistoryMore', 'e2eeHistoryRefresh', 'e2eeViewClose', 'audioCallBtn', 'voiceBtn', 'e2eeTextInput', 'e2eeTextSend', 'e2eeDraftClear', 'e2eeTextNotice', 'e2eeTextSize', 'e2eePendingAttachments', 'e2eePendingAttachmentList', 'e2eePendingAttachmentNotice', 'e2eePendingAttachmentMore'].map(id => [id, new Element()]));
  const sends = [], histories = [], writes = [], questions = [], listeners = new Map();
  let session = 'session', uuid = 0, nativeContext = 2, allowClear = options.confirm !== false;
  const ctx = { token: 'session', authExpiredHandled: false, currentChatID: 17, currentChatType: 'private', chatViewVersion: 1, TextEncoder,
    crypto: { randomUUID: () => '00000000-0000-4000-8000-' + (++uuid).toString(16).padStart(12, '0') },
    confirm: message => { questions.push(message); return allowClear; },
    localStorage: { getItem: () => session, setItem: (...args) => writes.push(args) },
    window: { zchatDesktop: { e2ee: { compiled: true,
      history: async (...args) => { histories.push(args); return { context: nativeContext, page: { kind: 'history', items: [], nextCursor: null } }; },
      sendText: async (message, expectedContext) => {
        if (expectedContext !== nativeContext) throw 'stale';
        const copy = { ...message }; sends.push(copy); return options.send ? options.send(copy) : accepted(copy, nativeContext);
      },
    } }, addEventListener: (name, fn) => { const group = listeners.get(name) || []; group.push(fn); listeners.set(name, group); } },
    document: { createElement: () => new Element(), getElementById: id => nodes.get(id) }, byId: id => nodes.get(id),
    chatHistoryRequests: new Map(), clearTypingDisplay() {}, closeInfoPanel() {},
    messagesEl: new Element(), chatNameEl: new Element(), chatStatusEl: new Element(), messageInput: new Element(),
  };
  runInNewContext(source + '\nthis.drafts=()=>[...e2eeDrafts.values()];', ctx);
  const open = async (chat = CHAT, context = 2, state = 'active') => {
    nativeContext = context; ctx.openE2eeConversation({ context, conversation: { conversationId: chat, peerUserId: OTHER, state, closeReason: state === 'closed' ? 'device_replaced' : null, closedAtUnixMs: state === 'closed' ? '1' : '0' } }, 'Bob'); await tick();
  };
  await open();
  return { ctx, nodes, sends, histories, writes, questions, open, uuidCount: () => uuid,
    input: nodes.get('e2eeTextInput'), send: () => nodes.get('e2eeTextSend').click(), clear: () => nodes.get('e2eeDraftClear').click(), notice: () => nodes.get('e2eeTextNotice').textContent,
    type(text) { nodes.get('e2eeTextInput').value = text; nodes.get('e2eeTextInput').listeners.get('input')(); },
    allowClear(value) { allowClear = value; },
    advanceNativeContext(value) { nativeContext = value; },
    changeAccount() { session = 'other'; for (const fn of listeners.get('storage') || []) fn({ key: 'token' }); },
    pagehide() { for (const fn of listeners.get('pagehide') || []) fn(); },
  };
}
test('native durable acceptance, not invoke start, clears text; double click is bounded and no browser storage is written', async () => {
  let finish;
  const f = await fixture({ send: input => new Promise(resolve => { finish = () => resolve(accepted(input)); }) });
  f.type('<b>  secret\n</b>'); await f.send();
  assert.equal(f.input.value, '<b>  secret\n</b>'); assert.equal(f.input.readOnly, true); assert.match(f.notice(), /Сохраняем/);
  await f.ctx.sendE2eeText(); assert.equal(f.sends.length, 1); assert.equal(f.uuidCount(), 1);
  assert.deepEqual(Object.keys(f.sends[0]).sort(), ['conversationId', 'messageId', 'text']);
  finish(); await tick(); assert.equal(f.input.value, ''); assert.equal(f.ctx.drafts().length, 0);
  assert.match(f.notice(), /Сохранено на этом устройстве/); assert.equal(f.writes.length, 0); assert.equal(f.histories.length, 2);
});
test('uncertain result keeps exact UUID/body, refuses body edits, and retries only on explicit action', async () => {
  let n = 0;
  const f = await fixture({ send: input => { if (++n === 1) throw 'native_wait'; return accepted(input); } });
  f.type('original'); await f.send(); assert.equal(f.input.readOnly, true); assert.equal(f.input.value, 'original');
  await tick(); assert.equal(f.sends.length, 1); assert.match(f.notice(), /тем же текстом/);
  f.type('changed DOM'); assert.equal(f.input.value, 'original'); await f.send();
  assert.deepEqual(f.sends[1], f.sends[0]); assert.equal(f.uuidCount(), 1); assert.equal(f.input.value, '');
});
test('UTF8 cap and empty text are checked before UUID/invoke; accepted Unicode is sent without trimming', async () => {
  for (const text of [' ', 'я'.repeat(1001), '🦀'.repeat(501)]) {
    const f = await fixture(); f.type(text); await f.ctx.sendE2eeText();
    assert.equal(f.sends.length, 0); assert.equal(f.uuidCount(), 0); assert.equal(f.input.value, text);
  }
  const f = await fixture(); f.type('я'.repeat(1000)); await f.send(); assert.equal(new TextEncoder().encode(f.sends[0].text).length, 2000);
  const g = await fixture(); g.type('  keep spaces\n'); await g.send(); assert.equal(g.sends[0].text, '  keep spaces\n');
});
test('clearing an uncertain draft requires consent and never claims to cancel native delivery', async () => {
  const f = await fixture({ confirm: false, send: () => { throw 'remote'; } }); f.type('unknown'); await f.send();
  await f.clear(); assert.equal(f.input.value, 'unknown'); assert.equal(f.ctx.drafts().length, 1); assert.match(f.questions[0], /не отменяет/);
  f.allowClear(true); await f.clear(); assert.equal(f.input.value, ''); assert.equal(f.ctx.drafts().length, 0);
  f.type('new explicit message'); await f.send(); assert.notEqual(f.sends[0].messageId, f.sends[1].messageId); assert.equal(f.sends.length, 2);
});
test('acceptance for an older selection cannot clear the newly selected chat draft', async () => {
  let finish;
  const f = await fixture({ send: input => new Promise(resolve => { finish = () => resolve(accepted(input)); }) });
  f.type('chat one'); await f.send(); await f.open(OTHER); f.type('chat two'); finish(); await tick();
  assert.equal(f.input.value, 'chat two'); assert.equal(f.ctx.drafts().length, 1); assert.equal(f.ctx.drafts()[0].chat, OTHER);
  assert.equal(f.sends.length, 1); assert.equal(f.histories.length, 2);
});
test('reopening the same chat restores the pending immutable draft and trusted acceptance settles it', async () => {
  let finish;
  const f = await fixture({ send: input => new Promise(resolve => { finish = () => resolve(accepted(input)); }) });
  f.type('pending'); await f.send(); await f.open(OTHER); await f.open(CHAT);
  assert.equal(f.input.value, 'pending'); assert.equal(f.input.readOnly, true);
  finish(); await tick(); assert.equal(f.input.value, ''); assert.equal(f.ctx.drafts().length, 0); assert.equal(f.sends.length, 1);
});
test('account/page retirement clears RAM drafts and late native replies cannot restore their plaintext', async () => {
  for (const retire of ['changeAccount', 'pagehide']) {
    let finish;
    const f = await fixture({ send: input => new Promise(resolve => { finish = () => resolve(accepted(input)); }) });
    f.type('old account secret'); await f.send(); f[retire](); assert.equal(f.ctx.drafts().length, 0); assert.equal(f.input.value, '');
    finish(); await tick(); assert.equal(f.input.value, ''); assert.equal(f.ctx.drafts().length, 0); assert.equal(f.nodes.get('e2eeConversationView').hidden, true);
    assert.equal(f.writes.length, 0);
  }
});
test('a newer native context cannot inherit or be cleared by the old attempt', async () => {
  let finish;
  const f = await fixture({ send: input => new Promise(resolve => { finish = () => resolve(accepted(input, 2)); }) });
  f.type('old context'); await f.send(); await f.open(CHAT, 3); f.type('new context draft');
  finish(); await tick(); assert.equal(f.input.value, 'new context draft'); assert.equal(f.ctx.drafts().length, 1); assert.equal(f.ctx.drafts()[0].context, 3);
});
test('a stale still-open form supplies its old context and is refused before native mutation', async () => {
  const f = await fixture(); f.type('old form'); f.advanceNativeContext(3); await f.send();
  assert.equal(f.sends.length, 0); assert.equal(f.input.value, 'old form'); assert.equal(f.ctx.drafts()[0].context, 2);
  assert.match(f.notice(), /Сессия устройства изменилась/);
  assert.equal(f.input.readOnly, true); assert.equal(f.writes.length, 0);
});
test('a sealed unsent accepted record is not shown as delivered or automatically re-encrypted', async () => {
  const f = await fixture({ send: input => accepted(input, 2, true) }); f.type('sealed original'); await f.send();
  assert.match(f.notice(), /отправка остановлена/); assert.equal(f.input.value, ''); assert.equal(f.sends.length, 1); assert.equal(f.uuidCount(), 1);
});
test('closed or recovery-frozen chats cannot silently trigger a reset or plaintext fallback', async () => {
  const f = await fixture(); await f.open(CHAT, 2, 'closed'); f.type('blocked'); await f.ctx.sendE2eeText(); assert.equal(f.sends.length, 0);
  const g = await fixture({ send: () => { throw 'recovery_required'; } }); g.type('frozen'); await g.send();
  assert.match(g.notice(), /восстановления/); assert.equal(g.input.value, 'frozen'); assert.equal(g.input.readOnly, true); assert.equal(g.writes.length, 0);
});
test('Shift+Enter and IME composition preserve text; ordinary Enter invokes only native text', async () => {
  const f = await fixture(); f.type('text'); let prevented = 0;
  const key = f.input.listeners.get('keydown');
  key({ key: 'Enter', shiftKey: true, preventDefault: () => prevented++ }); key({ key: 'Enter', isComposing: true, preventDefault: () => prevented++ }); await tick(); assert.equal(f.sends.length, 0);
  key({ key: 'Enter', preventDefault: () => prevented++ }); await tick(); assert.equal(f.sends.length, 1); assert.equal(prevented, 1);
});
test('RAM draft admission is bounded to eight chats and never drops existing drafts or invokes a ninth send', async () => {
  const f = await fixture();
  for (let i = 1; i <= 8; i++) { await f.open('00000000-0000-4000-8000-' + i.toString(16).padStart(12, '0')); f.type('draft ' + i); }
  assert.equal(f.ctx.drafts().length, 8);
  await f.open(OTHER); f.type('ninth'); await f.ctx.sendE2eeText();
  assert.equal(f.ctx.drafts().length, 8); assert.equal(f.sends.length, 0); assert.equal(f.uuidCount(), 0); assert.equal(f.input.value, 'ninth'); assert.match(f.notice(), /слишком много/);
});
