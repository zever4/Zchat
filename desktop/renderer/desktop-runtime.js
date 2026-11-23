"use strict";

// This adapter is bundled only for Tauri. Browser pages retain native fetch/WS.
(async function () {
  const entry = document.currentScript?.getAttribute("data-entry");
  const content = document.getElementById?.("desktop-content") || document.body;
  content.inert = true;
  const { invoke, Channel } = window.__TAURI__.core;
  const rawFetch = window.fetch.bind(window);
  const config = await invoke("get_config");
  const server = new URL(config.serverUrl);
  // Desktop signaling always enters the selected Go server's authenticated
  // gateway. LIVEKIT_URL is a browser-facing address and may use another alias
  // or proxy port; it must not expand the native transport's destination scope.
  const callGateway = new URL("/rtc/", server);
  callGateway.protocol = server.protocol === "https:" ? "wss:" : "ws:";
  const epoch = config.epoch;
  const appURL = new URL(location.href);
  const apiOrigin = location.protocol === "http:" || location.protocol === "https:"
    ? "http://zchat-api.localhost" : "zchat-api://localhost";
  const sameApp = url => url.protocol === appURL.protocol && url.host === appURL.host;
  const sameServer = url => url.protocol === server.protocol && url.host === server.host;
  const requestPath = url => url.pathname + url.search;
  const routedPath = path => `${apiOrigin}/${epoch}${path}`;
  const isAPI = path => /^\/(api\/|uploads\/)/.test(path) || path === "/health" || path.startsWith("/rtc");

  // Tauri keeps the same origin when the configured server changes. Do not send
  // a session or display account caches from the previous server to the new one.
  if (localStorage.getItem("zchat_desktop_server") !== config.serverUrl) {
    // Unsent ordinary messages have their own server+account namespace. Keep
    // those queues on device while removing all previous credentials/caches.
    for (let index = localStorage.length - 1; index >= 0; index--) {
      const key = localStorage.key(index);
      if (key && !key.startsWith("zchat_outbox_v1:")) localStorage.removeItem(key);
    }
    sessionStorage.clear();
    localStorage.setItem("zchat_desktop_server", config.serverUrl);
  }

  // A session context is native-owned. JWT is authenticated by discovery before
  // any store is opened; the renderer never supplies an account UUID or CA/path.
  const e2eeCompiled = config.e2eeCompiled === true;
  let e2eeContext = config.e2eeContext;
  const e2eeBindingContext = config.e2eeContext;
  const sessionToken = localStorage.getItem("token");
  let e2eeClosed = false;
  let closingE2ee;
  const phases = new Set(["disabled", "idle", "stopping", "starting", "setup_required", "replacement_required", "replacement_pending", "device_unavailable", "active", "failed", "closed"]);
  const uuid4 = value => typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(value);
  const peerId = value => typeof value === "string" && /^[1-9][0-9]{0,18}$/.test(value) && BigInt(value) <= 9223372036854775807n;
  const dialogMetadata = value => value && Object.keys(value).length === 5 &&
    ["conversationId", "peerUserId", "state", "closeReason", "closedAtUnixMs"].every(key => Object.hasOwn(value, key)) &&
    uuid4(value.conversationId) && uuid4(value.peerUserId) &&
    typeof value.closedAtUnixMs === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value.closedAtUnixMs) && BigInt(value.closedAtUnixMs) <= 9223372036854775807n &&
    (value.state === "active" && value.closeReason === null && value.closedAtUnixMs === "0" ||
     value.state === "closed" && value.closeReason === "device_replaced" && BigInt(value.closedAtUnixMs) > 0n);
  const i64String = value => typeof value === "string" && /^(0|[1-9][0-9]{0,18})$/.test(value) && BigInt(value) <= 9223372036854775807n;
  const textQueued = (value, message) => value && Object.keys(value).length === 6 &&
    ["conversationId", "messageId", "cryptoEpoch", "createdAtUnixMs", "storedOnServer", "sealedUnsent"].every(key => Object.hasOwn(value, key)) &&
    value.conversationId === message?.conversationId && value.messageId === message?.messageId &&
    i64String(value.cryptoEpoch) && i64String(value.createdAtUnixMs) &&
    typeof value.storedOnServer === "boolean" && typeof value.sealedUnsent === "boolean" && !(value.storedOnServer && value.sealedUnsent);
  const keysAre = (value, keys) => value && typeof value === "object" && Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
  const messageStates = new Set(["pending", "recipient_stored", "recipient_rejected", "expired", "recipient_replaced"]);
  const deliveryView = v => keysAre(v, ["storedOnServer", "state", "removal", "sealedUnsent"]) &&
    typeof v.storedOnServer === "boolean" && typeof v.sealedUnsent === "boolean" && !(v.storedOnServer && v.sealedUnsent) &&
    (v.state === null || messageStates.has(v.state)) && (v.removal === null || v.removal === "expired" || v.removal === "recipient_replaced");
  const utf8Bounded = (value, max) => typeof value === "string" && value.length <= max && new TextEncoder().encode(value).length <= max;
  const attachmentMetadata = v => keysAre(v, ["kind", "filename", "caption", "mimeType", "size", "photoMetadataStripped"]) &&
    utf8Bounded(v.filename, 255) && !!v.filename && utf8Bounded(v.caption, 2000) &&
    Number.isSafeInteger(v.size) && v.size > 0 &&
    (v.kind === "file" && v.size <= 50 * 1024 * 1024 && v.mimeType === "application/octet-stream" && v.photoMetadataStripped === null ||
     v.kind === "photo" && v.size <= 10 * 1024 * 1024 && ["image/jpeg", "image/png", "image/gif", "image/webp"].includes(v.mimeType) && typeof v.photoMetadataStripped === "boolean" ||
     v.kind === "voice" && v.size <= 10 * 1024 * 1024 && ["audio/ogg", "audio/webm"].includes(v.mimeType) && v.photoMetadataStripped === null);
  const historyItem = (v, query) => keysAre(v, ["messageId", "conversationId", "cryptoEpoch", "createdAtUnixMs", "senderUserId", "text", "outgoing", "delivery",
    ...(v && Object.hasOwn(v, "attachment") ? ["attachment"] : []),
    ...(v && Object.hasOwn(v, "cacheState") ? ["cacheState"] : [])]) &&
    uuid4(v.messageId) && v.conversationId === query.conversationId && i64String(v.cryptoEpoch) && i64String(v.createdAtUnixMs) && uuid4(v.senderUserId) &&
    typeof v.text === "string" && v.text.length > 0 && new TextEncoder().encode(v.text).length <= 2000 && typeof v.outgoing === "boolean" &&
    (v.outgoing ? deliveryView(v.delivery) : v.delivery === null) &&
    (!Object.hasOwn(v, "attachment") || attachmentMetadata(v.attachment)) &&
    (!Object.hasOwn(v, "cacheState") || Object.hasOwn(v, "attachment") && ["saved", "removed", ...(!v.outgoing ? ["missing"] : [])].includes(v.cacheState));
  const archiveScopes = {
    archives: [], archive_dialogs: ["generationId"], segments: ["generationId", "conversationId"],
    archive_history: ["generationId", "conversationId", "cryptoEpoch"],
  };
  const archiveKinds = new Set(Object.keys(archiveScopes));
  const archiveKeys = (value, keys) => !Array.isArray(value) && keysAre(value, keys);
  const archivePosition = (kind, item) => kind === "archives" ? [item.generationId] : kind === "archive_dialogs" ? [item.conversationId] :
    kind === "segments" ? [BigInt(item.cryptoEpoch)] : [BigInt(item.cryptoEpoch), BigInt(item.createdAtUnixMs), item.messageId];
  const comparePosition = (a, b) => {
    for (let i = 0; i < a.length; i++) { if (a[i] < b[i]) return -1; if (a[i] > b[i]) return 1; }
    return 0;
  };
  const archivePage = (page, query) => {
    const scope = archiveScopes[query.kind];
    if (!archiveKeys(page, ["kind", "items", "nextCursor", ...scope]) || !scope.every(k => page[k] === query[k])) return false;
    if (query.kind === "archives") {
      if (!page.items.every(v => archiveKeys(v, ["generationId", "replacedByGenerationId", "historyReady"]) && uuid4(v.generationId) &&
          (v.replacedByGenerationId === null || uuid4(v.replacedByGenerationId) && v.replacedByGenerationId !== v.generationId) && typeof v.historyReady === "boolean") ||
          page.items.filter(v => v.replacedByGenerationId === null).length > 1) return false;
    } else if (query.kind === "archive_dialogs") {
      if (!page.items.every(v => archiveKeys(v, ["conversationId", "peerUserId", "state", "closeReason", "closedAtUnixMs"]) && dialogMetadata(v))) return false;
    } else if (query.kind === "segments") {
      if (!page.items.every(v => archiveKeys(v, ["cryptoEpoch", "sealed", "createdAtUnixMs", "sealedAtUnixMs"]) && i64String(v.cryptoEpoch) && typeof v.sealed === "boolean" &&
          i64String(v.createdAtUnixMs) && BigInt(v.createdAtUnixMs) > 0n && (v.sealed ? i64String(v.sealedAtUnixMs) && BigInt(v.sealedAtUnixMs) >= BigInt(v.createdAtUnixMs) : v.sealedAtUnixMs === null))) return false;
      if (page.items.some((v, i) => !v.sealed && (query.cursor !== null || i !== 0)) ||
          (page.items.length > 0 && query.cursor === null && page.items[0].sealed) ||
          page.items.some((v, i) => i && BigInt(page.items[i - 1].cryptoEpoch) !== BigInt(v.cryptoEpoch) + 1n) ||
          (page.items.length > 0 && (page.nextCursor === null ? page.items.at(-1).cryptoEpoch !== "0" : page.items.at(-1).cryptoEpoch === "0"))) return false;
    } else if (!page.items.every(v => !Array.isArray(v) && historyItem(v, query) && (query.cryptoEpoch === null || v.cryptoEpoch === query.cryptoEpoch)) ||
        new Set(page.items.map(v => v.messageId)).size !== page.items.length) return false;
    const direction = query.kind === "archives" || query.kind === "archive_dialogs" ? 1 : -1;
    return page.items.every((v, i) => !i || comparePosition(archivePosition(query.kind, v), archivePosition(query.kind, page.items[i - 1])) === direction);
  };
  const readPage = (response, query) => {
    const page = response.page;
    if (!keysAre(response, ["context", "page", "replyLease"]) || !page || page.kind !== query?.kind ||
        !Array.isArray(page.items) || page.items.length > 8 || !(page.nextCursor === null || uuid4(page.nextCursor)) ||
        (page.nextCursor !== null && (page.items.length === 0 || page.nextCursor === query.cursor)) || new TextEncoder().encode(JSON.stringify(response)).length > (["history", "archive_history", "pending_attachments"].includes(page.kind) ? 16384 : 4096)) return false;
    if (archiveKinds.has(query.kind)) return !Array.isArray(response) && archivePage(page, query);
    if (query.kind === "pending_attachments") return keysAre(page, ["kind", "conversationId", "items", "nextCursor"]) &&
      page.conversationId === query.conversationId && page.items.every((v, i) =>
        keysAre(v, ["messageId", "cryptoEpoch", "createdAtUnixMs", "attachment"]) &&
        uuid4(v.messageId) && (!i || page.items[i - 1].messageId < v.messageId) &&
        i64String(v.cryptoEpoch) && i64String(v.createdAtUnixMs) && attachmentMetadata(v.attachment));
    if (!keysAre(page, ["kind", "items", "nextCursor"])) return false;
    if (page.kind === "dialogs") return page.items.every(dialogMetadata) && new Set(page.items.map(v => v.conversationId)).size === page.items.length;
    if (page.kind === "statuses") return page.nextCursor === null && page.items.length === query.messageIds.length && page.items.every((v, i) =>
      keysAre(v, ["messageId", "cryptoEpoch", "delivery"]) && v.messageId === query.messageIds[i] && i64String(v.cryptoEpoch) && deliveryView(v.delivery));
    return page.kind === "history" && new Set(page.items.map(v => v?.messageId)).size === page.items.length && page.items.every(v => historyItem(v, query));
  };
  // Only native-issued cursor provenance and public ordering metadata, never
  // plaintext. Native independently verifies its own TTL/LRU and full scope.
  const archiveCursors = new Map();
  const archiveKey = query => JSON.stringify([query.kind, query.generationId ?? null, query.conversationId ?? null, query.cryptoEpoch ?? null]);
  const archiveGate = expectedContext => {
    if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) { archiveCursors.clear(); throw staleE2ee(); }
    if (!Number.isSafeInteger(expectedContext) || expectedContext < 0 || expectedContext !== e2eeContext) throw staleE2ee();
  };
  async function archiveRead(kind, target, expectedContext, cursor) {
    archiveGate(expectedContext);
    const fields = archiveScopes[kind];
    if (!archiveKeys(target, fields)) throw new TypeError("Invalid archive target");
    const query = Object.freeze({ kind, ...Object.fromEntries(fields.map(k => [k, target[k]])), cursor });
    if ((kind !== "archives" && !uuid4(query.generationId)) || (fields.includes("conversationId") && !uuid4(query.conversationId)) ||
        (fields.includes("cryptoEpoch") && query.cryptoEpoch !== null && !i64String(query.cryptoEpoch)) || (cursor !== null && !uuid4(cursor))) throw new TypeError("Invalid archive query");
    const key = archiveKey(query), now = performance.now();
    for (const [token, value] of archiveCursors) if (now - value.used >= 120000) archiveCursors.delete(token);
    const previous = cursor === null ? null : archiveCursors.get(cursor);
    if (cursor !== null && (!previous || previous.context !== expectedContext || previous.key !== key)) throw new DOMException("Archive cursor unavailable for this query; refresh the first page", "InvalidStateError");
    if (previous) { archiveCursors.delete(cursor); archiveCursors.set(cursor, { ...previous, used: now }); }
    archiveGate(expectedContext);
    let response;
    try { response = await e2eeRequest("e2ee_read", { query }); }
    catch (error) { archiveGate(expectedContext); throw error; }
    archiveGate(expectedContext);
    if (previous && kind === "segments" && (!response.page.items.length || BigInt(response.page.items[0].cryptoEpoch) !== previous.position[0] - 1n)) throw staleE2ee();
    if (previous && response.page.items.length && comparePosition(archivePosition(kind, response.page.items[0]), previous.position) !== (kind === "archives" || kind === "archive_dialogs" ? 1 : -1)) throw staleE2ee();
    const next = response.page.nextCursor;
    if (next !== null) {
      const existing = archiveCursors.get(next);
      if (existing && (existing.context !== expectedContext || existing.key !== key)) throw staleE2ee();
      archiveCursors.delete(next);
      while (archiveCursors.size >= 32) archiveCursors.delete(archiveCursors.keys().next().value);
      archiveCursors.set(next, { context: expectedContext, key, position: archivePosition(kind, response.page.items.at(-1)), used: performance.now() });
    }
    return response;
  }
  const recoveryStates = new Set(["pending", "accepted", "declined", "cancelled", "expired", "superseded"]);
  const recoveryReply = (response, operation) => {
    const value = response.recovery;
    if (!keysAre(response, ["context", "recovery", "replyLease"]) || new TextEncoder().encode(JSON.stringify(response)).length > 4096 || value?.conversationId !== operation.conversationId) return false;
    if (operation.action !== "get") return keysAre(value, ["kind", "conversationId", "cryptoEpoch", "operationId"]) && value.kind === "queued" && value.cryptoEpoch === operation.expectedEpoch && uuid4(value.operationId);
    if (!keysAre(value, ["kind", "conversationId", "cryptoEpoch", "receiveOnly", "sendingFrozen", "proposals"]) || value.kind !== "state" ||
        !(value.cryptoEpoch === null || i64String(value.cryptoEpoch)) || typeof value.receiveOnly !== "boolean" || typeof value.sendingFrozen !== "boolean" ||
        !Array.isArray(value.proposals) || value.proposals.length > 2 || (value.cryptoEpoch === null && (value.proposals.length || value.sendingFrozen))) return false;
    return new Set(value.proposals.map(v => v?.proposalId)).size === value.proposals.length &&
      new Set(value.proposals.map(v => v?.ownProposal)).size === value.proposals.length && value.proposals.every(p =>
        keysAre(p, ["proposalId", "state", "ownProposal", "decision", "sendingFrozen"]) && uuid4(p.proposalId) && typeof p.ownProposal === "boolean" &&
        (p.state === null ? p.ownProposal : recoveryStates.has(p.state)) && p.sendingFrozen === value.sendingFrozen &&
        (p.decision === null || p.ownProposal && p.decision === "cancel" || !p.ownProposal && ["accept", "decline"].includes(p.decision)));
  };
  const staleE2ee = () => new DOMException("Desktop session changed", "AbortError");
  const attachmentTarget = v => keysAre(v, ["conversationId", "messageId"]) && uuid4(v.conversationId) && uuid4(v.messageId);
  const downloadReply = (response, target) => keysAre(response, ["context", "attachment", "replyLease"]) &&
    attachmentTarget(response.attachment) && response.attachment.conversationId === target.conversationId && response.attachment.messageId === target.messageId;
  const localFileTarget = v => keysAre(v, ["generationId", "conversationId", "messageId"]) &&
    (v.generationId === null || uuid4(v.generationId)) && uuid4(v.conversationId) && uuid4(v.messageId);
  const localFileReply = (reply, target, action) => {
    if (!keysAre(reply, ["context", "target", "result", "replyLease", ...(action === "listen" ? ["audio"] : [])]) || !localFileTarget(reply.target) ||
        Object.keys(target).some(key => target[key] !== reply.target[key])) return false;
    const result = reply.result;
    if (action === "export") return keysAre(result, ["kind"]) && ["exported", "cancelled"].includes(result.kind);
    if (action === "listen") return keysAre(result, ["kind", "mimeType"]) && result.kind === "voice" && ["audio/ogg", "audio/webm"].includes(result.mimeType);
    return keysAre(result, ["kind", "png"]) && result.kind === "preview" && typeof result.png === "string" &&
      result.png.length <= 1_600_000 && result.png.length % 4 === 0 && /^iVBORw0KGgo[A-Za-z0-9+/]*={0,2}$/.test(result.png);
  };
  const maxVoiceBytes = 10 * 1024 * 1024;
  function decodeVoiceReply(raw) {
    const bytes = raw instanceof Uint8Array ? raw : raw instanceof ArrayBuffer ? new Uint8Array(raw)
      : Array.isArray(raw) && raw.length <= 4 + 512 + maxVoiceBytes ? Uint8Array.from(raw) : null;
    if (!bytes || bytes.length < 5 || bytes.length > 4 + 512 + maxVoiceBytes) throw staleE2ee();
    const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
    const offset = 4 + length;
    if (length < 1 || length > 512 || offset >= bytes.length || bytes.length - offset > maxVoiceBytes) throw staleE2ee();
    let header;
    try { header = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes.subarray(4, offset))); }
    catch { throw staleE2ee(); }
    if (!keysAre(header, ["context", "target", "result", "replyLease"])) throw staleE2ee();
    return { ...header, audio: bytes.subarray(offset) };
  }
  const attachmentReply = (response, target, preparing) => {
    if (!keysAre(response, ["context", "attachment", "replyLease"]) || new TextEncoder().encode(JSON.stringify(response)).length > 4096) return false;
    const v = response.attachment;
    if (!v || v.conversationId !== target.conversationId || v.messageId !== target.messageId) return false;
    const base = ["conversationId", "messageId", "kind"];
    if (v.kind === "prepared") return keysAre(v, [...base, "cryptoEpoch"]) && i64String(v.cryptoEpoch);
    if (preparing) return false;
    if (v.kind === "vacant") return keysAre(v, base);
    return v.kind === "outgoing" && keysAre(v, [...base, "cryptoEpoch", "storedOnServer", "sealedUnsent"]) && i64String(v.cryptoEpoch) &&
      typeof v.storedOnServer === "boolean" && typeof v.sealedUnsent === "boolean" && !(v.storedOnServer && v.sealedUnsent);
  };
  const photoContext = expected => {
    if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken || !Number.isSafeInteger(expected) || expected < 0 || expected !== e2eeContext) throw staleE2ee();
  };
  const photoChoice = choice => {
    if (keysAre(choice, ["mode", "quality", "maxDimension"]) && choice.mode === "clean" &&
        (choice.quality === null || Number.isInteger(choice.quality) && choice.quality >= 1 && choice.quality <= 100) &&
        Number.isInteger(choice.maxDimension) && (choice.maxDimension === 0 || choice.maxDimension >= 64 && choice.maxDimension <= 12000)) {
      return { mode: "clean", quality: choice.quality, maxDimension: choice.maxDimension };
    }
    if (keysAre(choice, ["mode", "metadataWarningAcknowledged"]) && choice.mode === "original" && choice.metadataWarningAcknowledged === true) return { mode: "original", metadataWarningAcknowledged: true };
    throw new TypeError("Invalid private photo choice or missing original metadata consent");
  };
  let attachmentBusy = false;
  async function e2eeRequest(command, args = {}, changing = false, binary = null, signal = null) {
    if (signal?.aborted) throw staleE2ee();
    const context = e2eeContext;
    const requestId = crypto.randomUUID();
    const cancel = () => { void invoke("e2ee_cancel", { epoch, context, requestId }).catch(() => {}); };
    signal?.addEventListener("abort", cancel, { once: true });
    let response;
    try {
      response = await invoke(command, binary ? binary({ epoch, context, requestId }) : { epoch, context, requestId, ...args });
      if (command === "e2ee_local_attachment" && args.operation.action === "listen") response = decodeVoiceReply(response);
      if (!response || signal?.aborted || !Number.isSafeInteger(response.context) || response.context < context || typeof response.replyLease !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(response.replyLease) ||
          (command === "e2ee_conversation" ? !dialogMetadata(response.conversation)
            : command === "e2ee_send_text" ? !textQueued(response.queued, args.message)
            : command === "e2ee_prepare_attachment" ? !attachmentReply(response, args.target, true) ||
              args.expectedEpoch !== null && response.attachment.cryptoEpoch !== args.expectedEpoch
            : command === "e2ee_attachment_input" ? !attachmentReply(response, args.query, false)
            : command === "e2ee_download_attachment" ? !downloadReply(response, args.target)
            : command === "e2ee_local_attachment" ? !localFileReply(response, args.target, args.operation.action)
            : command === "e2ee_read" ? !readPage(response, args.query)
            : command === "e2ee_recovery" ? !recoveryReply(response, args.operation)
            : !phases.has(response.phase)) ||
          e2eeContext !== context || (!changing && response.context !== context)) throw staleE2ee();
      if (command === "e2ee_session" && args.token && localStorage.getItem("token") !== args.token) throw staleE2ee();
      if (e2eeClosed && !(command === "e2ee_session" && args.token === null)) throw staleE2ee();
      if (changing) { if (e2eeContext !== response.context) archiveCursors.clear(); e2eeContext = response.context; }
    } catch (error) {
      if (signal?.aborted) throw staleE2ee();
      throw error;
    } finally {
      signal?.removeEventListener("abort", cancel);
      // Complete replies hold native admission slots until consumed. No unbounded
      // native event stream; an abandoned page is also fenced by native page-load.
      if (Number.isSafeInteger(response?.context)) {
        await invoke("e2ee_ack", { epoch, context: response.context, requestId, replyLease: response.replyLease }).catch(() => {});
      }
    }
    // ACK is asynchronous too: recheck after it, immediately before exposing data.
    if (signal?.aborted || (e2eeClosed && !(command === "e2ee_session" && args.token === null)) || e2eeContext !== response.context || (!changing && (e2eeClosed || localStorage.getItem("token") !== sessionToken)) ||
        (changing && args.token && localStorage.getItem("token") !== args.token)) throw staleE2ee();
    return response;
  }
  async function localAttachment(target, expectedContext, operation, signal) {
    photoContext(expectedContext);
    if (!localFileTarget(target)) throw new TypeError("Invalid local attachment target");
    const { generationId, conversationId, messageId } = target;
    return e2eeRequest("e2ee_local_attachment", { target: { generationId, conversationId, messageId }, operation }, false, null, signal);
  }
  async function prepareAttachment(message, expectedContext, choice, maxBytes, signal = null) {
    const { file, caption, conversationId, messageId } = message;
    if (!uuid4(conversationId) || !uuid4(messageId) || !(file instanceof File) || !file.size || file.size > maxBytes ||
        !file.name || !utf8Bounded(file.name, 255) || !utf8Bounded(caption, 2000)) throw new TypeError("Invalid private attachment input");
    const filename = file.name, expectedBytes = file.size;
    const target = { conversationId, messageId };
    if (attachmentBusy) throw "busy";
    attachmentBusy = true;
    try {
      const existing = await e2eeRequest("e2ee_attachment_input", { query: target }, false, null, signal);
      photoContext(expectedContext);
      if (existing.attachment.kind !== "vacant") throw "conflict";
      const source = await file.arrayBuffer();
      photoContext(expectedContext);
      if (!(source instanceof ArrayBuffer) || source.byteLength !== expectedBytes) throw new TypeError("Invalid private attachment bytes");
      return await e2eeRequest("e2ee_prepare_attachment", { target, expectedEpoch: choice.expectedEpoch ?? null }, false, binding => {
        const metadata = new TextEncoder().encode(JSON.stringify({ ...binding, ...target, filename, caption, choice }));
        if (metadata.length > 16 * 1024) throw new TypeError("Attachment metadata too large");
        const frame = new Uint8Array(4 + metadata.length + source.byteLength);
        new DataView(frame.buffer).setUint32(0, metadata.length, true);
        frame.set(metadata, 4); frame.set(new Uint8Array(source), 4 + metadata.length);
        return frame;
      }, signal);
    } finally { attachmentBusy = false; }
  }
  const e2ee = Object.freeze({
    compiled: e2eeCompiled,
    downloadAttachment: async (target, expectedContext, signal = null) => {
      photoContext(expectedContext);
      if (!attachmentTarget(target)) throw new TypeError("Invalid attachment target");
      const { conversationId, messageId } = target;
      return e2eeRequest("e2ee_download_attachment", { target: { conversationId, messageId } }, false, null, signal);
    },
    previewAttachment: (target, expectedContext, signal = null) => localAttachment(target, expectedContext, { action: "preview" }, signal),
    listenAttachment: (target, expectedContext, signal = null) => localAttachment(target, expectedContext, { action: "listen" }, signal),
    exportAttachment: async (target, expectedContext, suggestedName, signal = null) => {
      if (!utf8Bounded(suggestedName, 255)) throw new TypeError("Invalid suggested name");
      return localAttachment(target, expectedContext, { action: "export", suggestedName }, signal);
    },
    pendingAttachments: async (conversationId, expectedContext, cursor = null) => {
      photoContext(expectedContext);
      if (!uuid4(conversationId) || (cursor !== null && !uuid4(cursor))) throw new TypeError("Invalid pending attachment query");
      return e2eeRequest("e2ee_read", { query: { kind: "pending_attachments", conversationId, cursor } });
    },
    attachmentInput: async (target, expectedContext, signal = null) => {
      photoContext(expectedContext);
      if (!attachmentTarget(target)) return Promise.reject(new TypeError("Invalid attachment target"));
      return e2eeRequest("e2ee_attachment_input", { query: { conversationId: target.conversationId, messageId: target.messageId } }, false, null, signal);
    },
    preparePhoto: async (message, expectedContext) => {
      photoContext(expectedContext);
      if (!keysAre(message, ["conversationId", "messageId", "file", "caption", "choice"])) throw new TypeError("Invalid private photo input");
      return prepareAttachment(message, expectedContext, photoChoice(message.choice), 20 * 1024 * 1024);
    },
    prepareFile: async (message, expectedContext, signal = null) => {
      photoContext(expectedContext);
      if (!keysAre(message, ["conversationId", "messageId", "file", "caption", "expectedEpoch"]) || !i64String(message.expectedEpoch)) throw new TypeError("Invalid private file input");
      return prepareAttachment(message, expectedContext, { mode: "file", expectedEpoch: message.expectedEpoch }, 50 * 1024 * 1024, signal);
    },
    prepareVoice: async (message, expectedContext, signal = null) => {
      photoContext(expectedContext);
      if (!keysAre(message, ["conversationId", "messageId", "file", "caption", "expectedEpoch"]) || !i64String(message.expectedEpoch)) throw new TypeError("Invalid private voice input");
      return prepareAttachment(message, expectedContext, { mode: "voice", expectedEpoch: message.expectedEpoch }, maxVoiceBytes, signal);
    },
    listArchives: (expectedContext, cursor = null) => archiveRead("archives", {}, expectedContext, cursor),
    archiveConversations: (target, expectedContext, cursor = null) => archiveRead("archive_dialogs", target, expectedContext, cursor),
    archiveSegments: (target, expectedContext, cursor = null) => archiveRead("segments", target, expectedContext, cursor),
    archiveHistory: (target, expectedContext, cursor = null) => archiveRead("archive_history", target, expectedContext, cursor),
    status: async () => {
      if (!e2eeCompiled) return { context: e2eeContext, phase: "disabled", error: null };
      if (e2eeClosed || localStorage.getItem("token") !== sessionToken) throw staleE2ee();
      return e2eeRequest("e2ee_status", { context: e2eeBindingContext }, true);
    },
    createConversation: peerUserId => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!peerId(peerUserId)) return Promise.reject(new TypeError("Invalid peer ID"));
      return e2eeRequest("e2ee_conversation", { operation: { action: "create", peerUserId } });
    },
    createConversationAfterClosed: (peerUserId, closedConversationId, confirmed) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!peerId(peerUserId) || !uuid4(closedConversationId) || confirmed !== true) return Promise.reject(new TypeError("Подтвердите создание нового E2EE-диалога взамен закрытого"));
      return e2eeRequest("e2ee_conversation", { operation: { action: "create_after_closed", peerUserId, closedConversationId, confirmed: true } });
    },
    getConversation: conversationId => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!uuid4(conversationId)) return Promise.reject(new TypeError("Invalid conversation ID"));
      return e2eeRequest("e2ee_conversation", { operation: { action: "get", conversationId } });
    },
    sendText: (message, expectedContext) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      // A form/draft belongs to the context that produced its dialog. Checking
      // only the response is too late: a new owner could already have encrypted
      // an old uncertain UUID. Compare synchronously BEFORE capturing IPC context.
      if (!Number.isSafeInteger(expectedContext) || expectedContext < 0 || expectedContext !== e2eeContext) return Promise.reject(staleE2ee());
      if (!message || Object.keys(message).length !== 3 || !uuid4(message.conversationId) || !uuid4(message.messageId) ||
          typeof message.text !== "string" || !message.text || new TextEncoder().encode(message.text).length > 2000) return Promise.reject(new TypeError("Invalid text message"));
      // Snapshot caller input; retries keep THEIR message UUID, while requestId
      // is only the disposable IPC admission/ACK identifier. No ordinary outbox.
      return e2eeRequest("e2ee_send_text", { message: { conversationId: message.conversationId, messageId: message.messageId, text: message.text } });
    },
    recoveryStatus: conversationId => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!uuid4(conversationId)) return Promise.reject(new TypeError("Invalid recovery conversation"));
      return e2eeRequest("e2ee_recovery", { operation: { action: "get", conversationId } });
    },
    proposeRecovery: (intent, expectedContext, confirmed) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken || !Number.isSafeInteger(expectedContext) || expectedContext < 0 || expectedContext !== e2eeContext) return Promise.reject(staleE2ee());
      if (confirmed !== true || !keysAre(intent, ["conversationId", "expectedEpoch", "expectedOwnProposalId"]) || !uuid4(intent.conversationId) || !i64String(intent.expectedEpoch) ||
          !(intent.expectedOwnProposalId === null || uuid4(intent.expectedOwnProposalId))) return Promise.reject(new TypeError("Подтвердите восстановление текущей E2EE-сессии"));
      return e2eeRequest("e2ee_recovery", { operation: { action: "propose", conversationId: intent.conversationId, expectedEpoch: intent.expectedEpoch, expectedOwnProposalId: intent.expectedOwnProposalId, confirmed: true } });
    },
    decideRecovery: (intent, expectedContext, confirmed) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken || !Number.isSafeInteger(expectedContext) || expectedContext < 0 || expectedContext !== e2eeContext) return Promise.reject(staleE2ee());
      if (confirmed !== true || !keysAre(intent, ["conversationId", "expectedEpoch", "proposalId", "decision"]) || !uuid4(intent.conversationId) || !i64String(intent.expectedEpoch) ||
          !uuid4(intent.proposalId) || !["accept", "decline", "cancel"].includes(intent.decision)) return Promise.reject(new TypeError("Подтвердите решение по выбранному запросу восстановления"));
      return e2eeRequest("e2ee_recovery", { operation: { action: "decide", conversationId: intent.conversationId, expectedEpoch: intent.expectedEpoch, proposalId: intent.proposalId, decision: intent.decision, confirmed: true } });
    },
    listConversations: (cursor = null) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (cursor !== null && !uuid4(cursor)) return Promise.reject(new TypeError("Invalid page cursor"));
      return e2eeRequest("e2ee_read", { query: { kind: "dialogs", cursor } });
    },
    history: (conversationId, cursor = null) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!uuid4(conversationId) || (cursor !== null && !uuid4(cursor))) return Promise.reject(new TypeError("Invalid history query"));
      return e2eeRequest("e2ee_read", { query: { kind: "history", conversationId, cursor } });
    },
    messageStatuses: (conversationId, messageIds) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (!uuid4(conversationId) || !Array.isArray(messageIds) || !messageIds.length || messageIds.length > 8 || !messageIds.every(uuid4) || new Set(messageIds).size !== messageIds.length) return Promise.reject(new TypeError("Invalid status query"));
      return e2eeRequest("e2ee_read", { query: { kind: "statuses", conversationId, messageIds: [...messageIds] } });
    },
    setupDevice: () => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      return e2eeRequest("e2ee_setup", {}, true);
    },
    retry: () => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      return e2eeRequest("e2ee_retry", {}, true);
    },
    replaceDevice: (password, confirmed) => {
      if (!e2eeCompiled || e2eeClosed || localStorage.getItem("token") !== sessionToken) return Promise.reject(staleE2ee());
      if (typeof password !== "string" || !password || new TextEncoder().encode(password).length > 72 || confirmed !== true) return Promise.reject(new TypeError("Подтвердите замену и введите текущий пароль"));
      return e2eeRequest("e2ee_replace", { consent: { password, confirmed: true } }, true);
    },
    endSession: () => {
      if (!e2eeCompiled) return Promise.resolve();
      e2eeClosed = true;
      archiveCursors.clear();
      return closingE2ee ||= e2eeRequest("e2ee_session", { token: null, context: e2eeBindingContext }, true).then(() => {});
    },
  });
  window.addEventListener?.("pagehide", () => { void e2ee.endSession().catch(() => {}); });
  window.addEventListener?.("storage", event => {
    if ((event.key === "token" || event.key === null) && localStorage.getItem("token") !== sessionToken) {
      void e2ee.endSession().catch(() => {});
    }
  });

  if (e2eeCompiled && location.pathname === "/chat.html" && sessionToken) {
    try { await e2eeRequest("e2ee_session", { token: sessionToken }, true); }
    catch (error) {
      if (localStorage.getItem("token") !== sessionToken) {
        await e2ee.endSession().catch(() => {});
        location.replace("/login.html");
        return;
      }
      throw error;
    }
  }

  window.fetch = async function (input, options = {}) {
    const url = new URL(input instanceof Request ? input.url : String(input), location.href);
    if (url.username || url.password || url.hash) throw new TypeError("Invalid application URL");
    // Tauri's own custom protocol is local IPC, not an API destination. Blocking
    // it forces postMessage fallback, which turns binary payloads into JSON.
    if ((url.protocol === "http:" && url.host === "ipc.localhost") ||
        (url.protocol === "ipc:" && url.host === "localhost")) return rawFetch(input, options);
    if ((!sameApp(url) && !sameServer(url)) || !isAPI(url.pathname)) {
      if (sameApp(url) || url.protocol === "blob:") return rawFetch(input, options);
      if (url.protocol === "https:" && url.hostname === "api.giphy.com") {
        const headers = new Headers(options.headers || (input instanceof Request ? input.headers : undefined));
        if (headers.has("authorization")) throw new TypeError("Application credentials cannot be sent to GIF search");
        return rawFetch(input, { ...options, headers, credentials: "omit", redirect: "error" });
      }
      throw new TypeError("Desktop requests require the configured server");
    }
    const id = crypto.randomUUID();
    const headers = new Headers(options.headers || (input instanceof Request ? input.headers : undefined));
    headers.set("x-zchat-request-id", id);
    const signal = options.signal || (input instanceof Request ? input.signal : undefined);
    signal?.throwIfAborted();
    const cancel = () => { void invoke("cancel_request", { id }).catch(() => {}); };
    signal?.addEventListener("abort", cancel, { once: true });
    try {
      let request = { ...options, headers, signal, credentials: "omit", redirect: "error" };
      if (input instanceof Request) {
        request = { method: input.method, body: ["GET", "HEAD"].includes(input.method) ? undefined : await input.clone().arrayBuffer(), ...request };
      }
      return await rawFetch(routedPath(requestPath(url)), request);
    } finally { signal?.removeEventListener("abort", cancel); }
  };

  class NativeSocket extends EventTarget {
    static CONNECTING = 0; static OPEN = 1; static CLOSING = 2; static CLOSED = 3;
    CONNECTING = 0; OPEN = 1; CLOSING = 2; CLOSED = 3;
    readyState = 0; bufferedAmount = 0; extensions = ""; protocol = ""; binaryType = "blob";
    onopen = null; onmessage = null; onerror = null; onclose = null;
    #id = crypto.randomUUID();

    constructor(value, protocols = []) {
      super();
      const url = new URL(value);
      if (!["ws:", "wss:"].includes(url.protocol) || url.username || url.password || url.hash ||
          !((url.host === appURL.host && url.pathname === "/ws") || (url.host === server.host && url.protocol === (server.protocol === "https:" ? "wss:" : "ws:")))) {
        throw new DOMException("Invalid desktop socket destination", "SecurityError");
      }
      if (protocols.length) throw new TypeError("WebSocket subprotocols are not supported");
      this.url = url.href;
      const channel = new Channel();
      channel.onmessage = event => {
        if (event.type === "open") {
          if (this.readyState !== 0) { void invoke("socket_close", { id: this.#id }); return; }
          this.readyState = 1; this.#emit(new Event("open"));
        } else if (event.type === "text" || event.type === "binary") {
          try {
            if (this.readyState !== 1) return;
            const bytes = event.type === "binary" ? new Uint8Array(event.data) : null;
            const data = bytes ? (this.binaryType === "arraybuffer" ? bytes.buffer : new Blob([bytes])) : event.data;
            this.#emit(new MessageEvent("message", { data }));
          } finally { void invoke("socket_ack", { id: this.#id, sequence: event.sequence }).catch(() => {}); }
        } else if (event.type === "error") this.#emit(new Event("error"));
        else if (event.type === "close") this.#closed(event.code);
      };
      void invoke("socket_connect", { epoch, id: this.#id, path: requestPath(url), channel }).then(() => {
        if (this.readyState >= 2) return invoke("socket_close", { id: this.#id });
      }).catch(() => { this.#emit(new Event("error")); this.#closed(1006); });
    }

    #emit(event) { this.dispatchEvent(event); const handler = this["on" + event.type]; if (typeof handler === "function") handler.call(this, event); }
    #closed(code) {
      if (this.readyState === 3) return;
      this.readyState = 3; this.bufferedAmount = 0;
      this.#emit(new CloseEvent("close", { code, wasClean: code === 1000 }));
    }
    send(value) {
      if (this.readyState !== 1) throw new DOMException("Socket is not open", "InvalidStateError");
      const binary = typeof value !== "string";
      const bytes = !binary ? new TextEncoder().encode(value) : value instanceof ArrayBuffer ? new Uint8Array(value) : ArrayBuffer.isView(value) ? new Uint8Array(value.buffer, value.byteOffset, value.byteLength) : null;
      if (!bytes || bytes.length > 65536) throw new TypeError("Unsupported or oversized socket payload");
      this.bufferedAmount += bytes.length;
      void invoke("socket_send", { id: this.#id, data: Array.from(bytes), binary }).catch(() => {
        this.#emit(new Event("error")); this.close();
      }).finally(() => { this.bufferedAmount = Math.max(0, this.bufferedAmount - bytes.length); });
    }
    close() {
      if (this.readyState >= 2) return;
      this.readyState = 2;
      void invoke("socket_close", { id: this.#id }).catch(() => this.#closed(1006));
    }
  }
  window.WebSocket = NativeSocket;

  window.zchatDesktop = Object.freeze({
    isDesktop: true,
    e2ee,
    serverUrl: config.serverUrl,
    callGatewayURL: callGateway.href,
    resetMicrophonePermission: () => invoke("reset_microphone_permission"),
    prepareImage: async (file, quality, maxDimension) => {
      if (!(file instanceof File) || file.size > 20 * 1024 * 1024) throw new Error("Для сжатия выберите изображение до 20 МБ");
      if (!Number.isInteger(quality) || quality < 1 || quality > 100 || !Number.isInteger(maxDimension) ||
          (maxDimension !== 0 && (maxDimension < 64 || maxDimension > 12000))) throw new Error("Недопустимые параметры сжатия");
      const result = await invoke("prepare_image", new Uint8Array(await file.arrayBuffer()), {
        headers: { "x-zchat-quality": String(quality), "x-zchat-max-dimension": String(maxDimension) },
      });
      const bytes = new Uint8Array(result);
      if (bytes.byteLength < 5 || bytes.byteLength > 20 * 1024 * 1024 + 4096) throw new Error("Некорректный результат обработки");
      const length = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getUint32(0, true);
      if (length > 4096 || length + 4 >= bytes.byteLength) throw new Error("Некорректный результат обработки");
      const metadata = JSON.parse(new TextDecoder().decode(bytes.subarray(4, length + 4)));
      const formats = { "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp", "image/gif": "gif", "image/bmp": "bmp", "image/tiff": "tiff" };
      if (metadata.version !== 1 || !Object.hasOwn(formats, metadata.mime) || formats[metadata.mime] !== metadata.extension ||
          typeof metadata.changed !== "boolean" || !["compressed", "original_smaller", "animation", "multipage"].includes(metadata.reason) ||
          !Number.isInteger(metadata.width) || !Number.isInteger(metadata.height) || metadata.width < 1 || metadata.height < 1 ||
          metadata.width > 12000 || metadata.height > 12000 || metadata.width * metadata.height > 24000000 ||
          (!metadata.changed && bytes.byteLength - length - 4 !== file.size)) throw new Error("Некорректный результат обработки");
      const name = (file.name.replace(/\.[^.]*$/, "") || "image") + "." + metadata.extension;
      return { ...metadata, file: metadata.changed ? new File([bytes.subarray(length + 4)], name, { type: metadata.mime }) : file };
    },
    isApplicationURL: value => { try { return sameApp(new URL(value, location.href)); } catch { return false; } },
    resourceURL: value => {
      try {
        const url = new URL(value, location.href);
        if (!(sameApp(url) || sameServer(url)) || !/^\/uploads\/avatar_[a-zA-Z0-9_-]+\.(png|jpe?g|gif|webp)$/.test(url.pathname) || url.search || url.hash) return "";
        return routedPath(url.pathname);
      } catch { return ""; }
    },
  });
  document.addEventListener("click", event => {
    const link = event.target instanceof Element ? event.target.closest("a[href]") : null;
    if (!link || link.hasAttribute("download")) return;
    const url = new URL(link.href);
    if (["http:", "https:"].includes(url.protocol) && !sameApp(url)) {
      event.preventDefault();
      if (event.isTrusted) void invoke("open_external", { targetUrl: url.href }).catch(() => {});
    }
  }, true);

  try { await invoke("check_server"); }
  catch { location.replace("/offline.html"); return; }
  const script = document.createElement("script");
  script.src = entry || (location.pathname === "/chat.html" ? "/chat.js" : "/login.js");
  script.onload = () => { content.inert = false; };
  script.onerror = () => {
    content.inert = false;
    content.textContent = "Не удалось загрузить интерфейс. Перезагрузите окно.";
  };
  document.body.appendChild(script);
})().catch(() => {
  const content = document.getElementById?.("desktop-content") || document.body;
  content.inert = false;
  const error = document.createElement("p");
  error.textContent = "Не удалось запустить desktop-клиент. Перезагрузите окно или откройте настройки сервера кнопкой в заголовке.";
  content.replaceChildren(error);
});
