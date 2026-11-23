// Creates synthetic users/data. Use through smoke-compose.sh on a fresh stack.
import { createHash, randomUUID } from 'node:crypto';

const [major, minor] = process.versions.node.split('.').map(Number);
if (major < 22 || (major === 22 && minor < 22)) throw Error('Requires Node >=22.22');
const base = new URL(process.env.SMOKE_BASE_URL || 'invalid');
if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.pathname !== '/' || base.search || base.hash) {
  throw Error('SMOKE_BASE_URL must be an application origin without credentials');
}
const peers = [], stamp = randomUUID().replaceAll('-', '').slice(0, 12);
const assert = (value, message) => { if (!value) throw Error(message); };
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
async function check(name, run) { await run(); console.log('PASS: ' + name); }
async function request(method, path, user, data, expected = 200) {
  const url = new URL(path, base), headers = new Headers();
  if (user) headers.set('Authorization', 'Bearer ' + user.token);
  const multipart = data instanceof FormData;
  if (data !== undefined && !multipart) headers.set('Content-Type', 'application/json');
  let response;
  try {
    response = await fetch(url, { method, headers, body: data === undefined ? undefined : multipart ? data : JSON.stringify(data),
      redirect: 'error', credentials: 'omit', signal: AbortSignal.timeout(15000) });
  } catch { throw Error(`Request failed: ${method} ${url.pathname}`); }
  assert(response.status === expected, `${method} ${url.pathname}: ${response.status}, expected ${expected}`);
  return response;
}
async function json(method, path, user, data) { return (await request(method, path, user, data)).json(); }
async function account(label) {
  const user = { username: `smoke_${label}_${stamp}`, email: `smoke_${label}_${stamp}@example.invalid`, password: 'Local-smoke-password-2026' };
  const auth = await json('POST', '/api/register', null, user);
  return { ...user, ...auth, ...await json('GET', '/api/profile', auth) };
}
async function ticket(user) { return json('POST', '/api/ws-ticket', user); }
async function connect(user) {
  const grant = await ticket(user), url = new URL('/ws', base);
  assert(/^[A-Za-z0-9_-]{43}$/.test(grant.ticket), 'Malformed connection ticket');
  url.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
  url.searchParams.set('ticket', grant.ticket);
  const socket = new WebSocket(url), messages = [];
  const peer = { socket, grant, messages };
  peers.push(peer);
  socket.addEventListener('message', event => {
    for (const line of String(event.data).split('\n')) {
      try { messages.push(JSON.parse(line)); } catch { /* Ignore greeting framing only. */ }
    }
  });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(Error('WebSocket open timed out')), 5000);
    socket.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    socket.addEventListener('error', () => { clearTimeout(timer); reject(Error('WebSocket handshake failed')); }, { once: true });
  });
  return peer;
}
function send(peer, chatID, content, type) {
  assert(peer.socket.readyState === WebSocket.OPEN, 'WebSocket not open');
  peer.socket.send(JSON.stringify({ chat_id: Number(chatID), content, type }));
}
async function received(peer, content) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const message = peer.messages.find(m => m.content === content);
    if (message) return message;
    await wait(25);
  }
  throw Error('Message delivery timed out');
}
async function closed(peer) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    if (peer.socket.readyState === WebSocket.CLOSED) return;
    await wait(25);
  }
  throw Error('Revoked WebSocket stayed open');
}
async function receipt(peer, clientID, type = 'message_ack', after = 0) {
  const deadline = Date.now() + 5000;
  while (Date.now() < deadline) {
    const message = peer.messages.slice(after).find(m => m.type === type && m.client_message_id === clientID);
    if (message) return message;
    await wait(25);
  }
  throw Error('Correlated message receipt timed out');
}
try {
  await check('health, page policy, local login and hashed vendor assets', async () => {
    await request('GET', '/health');
    const page = await request('GET', '/chat.html');
    const policy = page.headers.get('content-security-policy') || '';
    assert(policy.includes("script-src 'self'") && policy.includes("script-src-attr 'none'"), 'Missing browser policy');
    const html = await page.text();
    assert(!/\son(?:click|change|load)\s*=/.test(html), 'Inline static event handler remains');
    const login = await (await request('GET', '/login.html')).text();
    assert(login.includes('src="login.js?') && login.includes('href="login.css?'), 'Missing external login assets');
    const manifest = await json('GET', '/vendor/manifest.json');
    assert(manifest.files.length >= 5 && manifest.packages['livekit-client'], 'Missing asset manifest');
    for (const file of manifest.files) {
      const bytes = Buffer.from(await (await request('GET', '/vendor/' + file.path)).arrayBuffer());
      assert(createHash('sha256').update(bytes).digest('hex') === file.sha256, 'Packaged vendor hash differs');
    }
    await request('GET', '/metrics', null, undefined, 404);
  });
  let owner = await account('owner');
  const member = await account('member');
  const outgoing = await json('POST', '/api/friends/request', owner, { to_user_id: Number(member.id) });
  const privateChat = await json('POST', `/api/friends/requests/${outgoing.id}/accept`, member);
  const group = (await json('POST', '/api/groups', owner, { name: 'Smoke ' + stamp, is_public: true })).group;
  const a = await connect(owner), b = await connect(member);
  await check('private WS delivery and persisted history', async () => {
    const content = 'private-' + stamp;
    send(a, privateChat.id, content);
    await received(a, content); await received(b, content);
    assert((await json('GET', `/api/chats/${privateChat.id}/messages`, member)).messages.some(m => m.content === content), 'Message not persisted');
  });
  await check('durable receipts, concurrent retry, ID conflict and edit/delete replay', async () => {
    const twin = await connect(owner), clientID = randomUUID(), content = 'receipt-' + stamp;
    await wait(50);
    assert(a.messages.some(m => m.message_receipts === true), 'Missing receipt capability');
    const frame = JSON.stringify({ chat_id: Number(privateChat.id), content, client_message_id: clientID });
    a.socket.send(frame); twin.socket.send(frame);
    const first = await receipt(a, clientID);
    await receipt(twin, clientID); await received(b, content);
    const history = (await json('GET', `/api/chats/${privateChat.id}/messages`, owner)).messages;
    assert(history.filter(m => m.content === content).length === 1, 'Retry created duplicate rows');
    assert(b.messages.filter(m => m.content === content).length === 1, 'Retry broadcast a duplicate');
    assert(!b.messages.some(m => m.client_message_id === clientID), 'Receipt leaked to peer');
    const start = a.messages.length;
    a.socket.send(JSON.stringify({ chat_id: Number(privateChat.id), content: 'changed', client_message_id: clientID }));
    const denied = await receipt(a, clientID, 'error', start);
    assert(denied.code === 'id_conflict' && denied.retryable === false, 'Collision was retryable/accepted');
    for (const deleted of [false, true]) {
      await json(deleted ? 'DELETE' : 'PUT', `/api/messages/${first.id}`, owner, deleted ? undefined : { content: 'edited-' + stamp });
      const after = a.messages.length;
      a.socket.send(frame);
      const replay = await receipt(a, clientID, 'message_ack', after);
      assert(String(replay.id) === String(first.id), 'Replay changed committed ID');
      const messages = (await json('GET', `/api/chats/${privateChat.id}/messages`, owner)).messages;
      assert(deleted ? !messages.some(m => String(m.id) === String(first.id)) : messages.find(m => String(m.id) === String(first.id))?.content === 'edited-' + stamp, 'Retry resurrected original content');
    }
    twin.socket.close();
  });
  async function forbiddenGroupMessage(content) {
    send(b, group.id, content);
    // The allowed frame on the SAME socket proves processing passed the denied one.
    const marker = 'barrier-' + randomUUID();
    send(b, privateChat.id, marker);
    await received(b, marker); await received(a, marker);
    assert(!(await json('GET', `/api/chats/${group.id}/messages`, owner)).messages.some(m => m.content === content), 'Forbidden group message persisted');
  }
  await check('nonmember denial, group join and server-owned message type', async () => {
    await forbiddenGroupMessage('nonmember-' + stamp);
    await json('POST', `/api/groups/invite/${group.invite_code}/join`, member);
    const content = 'group-' + stamp;
    send(b, group.id, content, 'private');
    const message = await received(a, content); await received(b, content);
    assert(message.type === 'group', 'Client spoofed server chat type');
  });
  await check('group permission change applies to an already open socket', async () => {
    await json('PUT', `/api/groups/${group.id}/permissions`, owner, { admins_only_write: true });
    await forbiddenGroupMessage('restricted-' + stamp);
  });
  await check('ticket replay, legacy JWT query and current logout', async () => {
    await request('GET', '/ws?ticket=' + a.grant.ticket, null, undefined, 401);
    await request('GET', '/ws?token=' + encodeURIComponent(owner.token), null, undefined, 401);
    const pending = await ticket(owner);
    await json('POST', '/api/profile/logout', owner);
    await closed(a);
    await request('GET', '/api/profile', owner, undefined, 401);
    await request('GET', '/ws?ticket=' + pending.ticket, null, undefined, 401);
    await json('GET', '/api/profile', member);
    assert(b.socket.readyState === WebSocket.OPEN, 'Logout closed another user');
    owner = { ...owner, ...await json('POST', '/api/login', null, { email: owner.email, password: owner.password }) };
  });
  const fresh = await connect(owner);
  await check('file upload/download under runtime UID and current membership', async () => {
    const content = 'attachment-' + stamp, form = new FormData();
    form.set('chat_id', String(group.id));
    form.set('file', new Blob([content], { type: 'text/plain' }), 'smoke.txt');
    const file = await json('POST', '/api/files/upload', owner, form);
    assert(file.url.startsWith('/uploads/files/'), 'Unexpected attachment URL');
    await request('GET', file.url, null, undefined, 401);
    const download = await request('GET', file.url, member);
    assert(download.headers.get('x-content-type-options') === 'nosniff' && download.headers.get('content-disposition')?.startsWith('attachment;'), 'Unsafe attachment headers');
    assert(await download.text() === content, 'Attachment content changed');
    await json('DELETE', `/api/groups/${group.id}/members/${member.id}`, owner);
    await request('GET', file.url, member, undefined, 404);
  });
  await check('password rotation revokes socket and pending ticket; independent user survives', async () => {
    const pending = await ticket(owner), nextPassword = 'Local-smoke-password-after-2026';
    await json('PUT', '/api/profile/password', owner, { old_password: owner.password, new_password: nextPassword });
    await closed(fresh);
    await request('GET', '/api/profile', owner, undefined, 401);
    await request('GET', '/ws?ticket=' + pending.ticket, null, undefined, 401);
    await json('GET', '/api/profile', member);
    assert(b.socket.readyState === WebSocket.OPEN, 'Password change closed another user');
    const login = await json('POST', '/api/login', null, { email: owner.email, password: nextPassword });
    await json('GET', '/api/profile', login);
  });
} catch (error) {
  console.error('FAIL: ' + error.message);
  process.exitCode = 1;
} finally {
  for (const peer of peers) {
    try { peer.socket.close(); } catch { /* Cleanup may race a failed handshake. */ }
  }
}
