# Development

This guide covers source builds, verification and Windows packaging. For running
the application, start with [README](README.md). Configuration names/defaults live
in [.env.example](.env.example); components are described in [architecture.md](architecture.md).

## Source builds

Use the toolchains selected by `go.mod`, `.node-version` and `rust-toolchain.toml`.
Node 18 is unsupported. Docker builds the browser/server without host compilers.
For host development:

```bash
npm ci
npm run build:chat
go build -o bin/server ./cmd/server
```

Generate frontend assets before serving `web/`. Generated `web/chat.js`, vendor
assets, `dist-desktop/`, binaries and private `.env` files stay out of Git.
Generated Go files in `gen/` are committed.

## Upload storage

The Go backend runs in Linux/Docker/WSL. Configure its budget in `.env`, then
restart the server (`make up` rebuilds/recreates the Compose service):

| Setting | Default | Meaning |
| --- | --- | --- |
| `STORAGE_TOTAL_GB` | 50 | Total retained and reserved upload storage |
| `STORAGE_ACCOUNT_GB` | 10 | The same budget per uploading account |
| `STORAGE_FREE_GB` | 1 | Free disk space preserved before a new upload |
| `STORAGE_TOTAL_FILES` / `STORAGE_ACCOUNT_FILES` | 200000 / 50000 | File/entry caps, including temporary reservations |
| `STORAGE_FREE_INODES` | 1024 | Spare filesystem entries |
| `STORAGE_UPLOADS` / `STORAGE_ACCOUNT_UPLOADS` | 8 / 2 | Concurrent uploads |
| `STORAGE_DOWNLOADS` / `STORAGE_ACCOUNT_DOWNLOADS` | 64 / 16 | Concurrent downloads; anonymous avatars use the global cap |

GB means decimal gigabytes. Values are positive integers. These caps do not
allocate disk space: the backend also checks actual free space and inodes.
Lowering a cap preserves existing files and blocks new uploads above the cap.
Ordinary files, media and avatars share the budget, including controlled temporary
copies and deleted files still being downloaded. New filenames retain the uploader's
account ID for restart accounting; unattributed existing files count globally.
Accounting rounds allocations and includes metadata allowances, so it exceeds
payload sizes. Existing per-file size limits remain unchanged. Unknown database
commit outcomes conservatively retain the file and its charge.

Private blob storage is off by default. `ENABLE_EXPERIMENTAL_E2EE_BLOBS=true` starts
its owned namespace, shared accounting and one cleanup loop. Raw PUT and native
`ReserveBlob`/`ObserveBlob` RPCs also
require `ENABLE_EXPERIMENTAL_E2EE=true`; disabling that API flag keeps maintenance
running. Once a configured root has its identity marker, disabling its maintenance
is refused so existing files cannot disappear from the shared budget. Attachment
Send and authenticated GET/HEAD use this exposed runtime's cancellation/join boundary.
Native account-owned downloads, verified cache, automatic file ACK and accepted-file
cleanup are implemented; attachment IPC/UI remain pending. Feature readiness/capabilities remain false.

Native reserve/observe calls and a scoped HTTP ciphertext transport are implemented.
The transport uses a freshly checked protected upload snapshot, a trusted native
origin/CA, bounded responses and cancellation that releases the socket and file
buffer before close completes. A bounded native step now performs Observe, exact
reservation replay if missing, one PUT and Observe with durable progress merging.
The experimental account owner now retries durable preparations automatically.
It owns one HTTP body and routes blob metadata through its existing serial RPC
lane; slow uploads do not block text/control RPCs. Selection/retry state is bounded
to64files, with per-file backoff. Logout/switch/owner drop cancels and joins the
body before releasing the worker/store. Minimizing preserves transfers.
Cancellation preserves the durable
operation and ciphertext. Observe the exact operation after an unknown HTTP result;
an HTTP success is not a message receipt. Preparation journals require the committed
operation alongside the ciphertext; old isolated journal format1 fails closed
without automatic conversion.

`E2EE_BLOB_ROOT` must be an existing owned Linux directory with mode0700 outside
`web`. Compose prepares `/var/lib/zchat/e2ee-blobs` on a separate persistent volume
for UID10001. Host runs must create their private directory explicitly. Never put
it inside the public tree or move/replace its synced identity marker. Public assets
are served through `os.Root.FS`, which refuses symlinks escaping that public root.

| Private setting | Default | Meaning |
| --- | --- | --- |
| `E2EE_BLOB_TOTAL_GB` / `E2EE_BLOB_ACCOUNT_GB` | 50 / 10 | Persisted PG ciphertext-content reservation caps |
| `E2EE_BLOB_TOTAL_CLAIMS` / `E2EE_BLOB_ACCOUNT_CLAIMS` | 50000 / 25000 | Retained message claim caps |
| `E2EE_BLOB_TOTAL_ATTEMPTS` / `E2EE_BLOB_ACCOUNT_ATTEMPTS` | 100000 / 50000 | Retained attempt caps, including reclaimed history |
| `E2EE_BLOB_MAX_ARTIFACTS` | 20000 | Live/reserved filename ceiling, matching the restart manifest; configurable2..20000 |
| `E2EE_BLOB_STARTUP_VERIFY_GB` / `E2EE_BLOB_STARTUP_SECONDS` | 100 / 60 | Verification bytes and total private startup time; seconds1..600 |
| `SERVER_HEALTH_START_PERIOD` | 90s | Compose startup grace; increase when choosing a longer startup budget |

PG caps are fixed on first initialization. A later mismatch fails closed and never
resets counters; changing them requires a reviewed maintenance operation. They are
independent of `STORAGE_*`: those physical caps can be lowered on restart while
preserving old files. Startup performs Configure/PG binding/reconciliation and
`AttachBudget` before launching cleanup or mounting the route. Private uploads
reserve space for both publication names, so they cannot outgrow the next startup's
bounded manifest. The private writer cap is at most63, always within the common cap.

Roots share account/global quotas; reservations on one filesystem share a free-space
check, while separate filesystems retain independent headroom. Each root registers
its census once per process; shutdown joins producers before closing the roots.
Do not run another writer against these volumes: one backend owns the namespaces.
Shutdown cancels and joins uploads, control RPCs and cleanup before releasing the root.
If a query does not join within the deadline, ownership and shared resources stay held until
work joins or the process exits. Cleanup pass/failure/reclaim observations use
`e2ee_blob_cleanup_*` Prometheus metrics with fixed lane/result labels.

## Native build foundation

Windows is the supported desktop target. Use native Node/Rust, Visual Studio
Build Tools with the C++ workload, Windows SDK and WebView2. See the
[Tauri prerequisites](https://v2.tauri.app/start/prerequisites/). WSL Cargo targets
Linux and needs its GTK/WebKit dependencies; keep Windows/WSL build outputs separate.

Start the backend independently, then in native Windows PowerShell:

```powershell
npm ci
npm run desktop:dev
```

The desktop title-bar gear selects the backend origin. For an isolated test
profile, set `ZCHAT_DESKTOP_DATA_DIR` to a dedicated absolute directory before
launching. Omit it for normal use; never reset an existing profile to bypass bugs.

To build the per-user NSIS installer:

```powershell
npm run licenses:desktop
npm run desktop:dist:win -- --ci -- --locked
```

The `.exe` appears in `target/release/bundle/nsis/`. It includes the client, not
Go/PostgreSQL or the optional Redis/LiveKit services. Code signing and automatic updates are not implemented.
The dependency inventory is described in [licensing.md](licensing.md).

For an experimental Windows installer, install `protoc` and use
`npm run desktop:dist:win:e2ee -- --ci -- --locked`. This command generates and
bundles the E2EE dependency notices. Keep separate output directories for the
standard and E2EE installers when building both with the same app version.

## Experimental desktop sessions

The Windows feature `experimental-e2ee` enables protected conversations, native
key storage and attachment preparation for local development
(`npm run desktop:dev -- --features experimental-e2ee`). Install `protoc` first.
For the loopback demo, set `ENABLE_EXPERIMENTAL_E2EE=true` and
`ENABLE_EXPERIMENTAL_E2EE_BLOBS=true` in the server's local `.env` and run `make up`.
The default installer and its dependency inventory cover ordinary chat.

Native configuration is `e2ee-servers.json`, beside `desktop-config.json` in the
app config directory (or `ZCHAT_DESKTOP_DATA_DIR`). It is read at startup; the
renderer cannot set its gRPC endpoints, CAs or account-store paths. Example:

```json
[{"httpOrigin":"http://127.0.0.1:18080","grpcOrigin":"http://127.0.0.1:50052"}]
```

Use the actual backend origins/`SERVER_GRPC_PORT`. Remote RPC requires HTTPS;
`extraCaPem` optionally supplies an additional PEM CA for E2EE RPC and encrypted-file
HTTPS uploads; loopback HTTP uploads do not use a CA. Missing configuration affects
only experimental E2EE. Login authenticates before opening an existing account DB;
it never creates/replaces keys. In the experimental desktop, Security → Key setup
explicitly creates the first keys; retry replays saved registration/publication after
lost replies. A foreign installation offers a separate replacement action with a
consequence warning, confirmation and the current password. Its target is selected
natively and checked against fresh authenticated state: login/retry cannot replace
keys, and a changed target requires new confirmation. Unknown replacement results
resume the exact saved operation after password re-entry. Old local history is
retained. The experimental text composer, manual recovery controls and local archive
viewer are available. Local Windows acceptance covered two separate profiles
and installation replacement. Local archives stay with the profile that holds
their keys.

`cargo test -p zchat-desktop --locked --all-features` checks the lifecycle and IPC
boundaries. The ignored Windows workers can also run through the Go/TLS/PostgreSQL
harness: set `ZCHAT_RUST_DESKTOP_TEST_BIN` to the absolute WSL path of that Windows test executable and run
`go test -race ./internal/grpc -run '^TestE2EERustDesktop(Session|Replacement|Dialog|Successor|Text)Postgres$' -count=1 -v`
from WSL with the disposable DB variable. This checks actual DPAPI/account switches, explicit initial setup, exact replay of
a lost Register response, foreign-installation refusal and safe retry after session
rejection. The replacement fixture adds wrong-password classification, lost-COMMIT
replay, competing installations, explicit CAS retargeting, server-side chat closure
and retention of a real v2 protected history record across replacement. These tests
do not exercise a WebView or an archive viewer.

The development-only bridge also exposes explicit Create/Get conversation metadata.
It uses the account's existing serial network lane, with one shared budget of eight
foreground requests across local and network queues. Responses contain public IDs
and closure state; scopes, keys and raw protobuf stay native. Create takes the
decimal numeric backend peer ID; response `peerUserId` is the peer account UUID.
Create/Get do not bootstrap Signal. The dialog fixture checks friendship/privacy/key prerequisites,
exact saved Create replay after a lost commit and reopen, stale-page rejection,
reply limits, foreign-account refusal and closure after peer-device replacement.
The bridge also exposes `sendText({ conversationId, messageId, text }, dialogContext)` for explicit
user text (up to 2000 UTF-8 bytes). Keep the same message UUID when retrying; the
IPC request UUID is separate. Success means the native encrypted outbox committed,
not recipient delivery. Pass the dialog reply's context as the required second
argument: `sendText(message, dialogReply.context)`. The SDK checks that context
synchronously before IPC, so a stale draft cannot be encrypted by a newer owner.
An established session can queue offline; the first send
needs network access to bootstrap. Replies contain IDs, native time/epoch and
observed persistence flags, with no plaintext or ciphertext.

Local reads use `listConversations(cursor?)`, `history(conversationId, cursor?)` and
`messageStatuses(conversationId, messageIds)`. Pages contain up to eight entries;
history replies allow 16,384 bytes, other replies 4,096 bytes, with the same eight
unacknowledged-response slots. Cursors are opaque, scoped to the current account
and query, and expire after two minutes or LRU eviction. Refresh from the latest
page after a stale cursor or live inserts; paging is not a frozen snapshot.
History includes user texts from retained v2 epochs in the current installation
generation. The replacement archive viewer is separate. Reading does not bootstrap
Signal or send delivery ACKs. The text fixture checks whole escaped messages,
cursor/account fences and status/history reads against real Go/TLS/PostgreSQL.

Archive reads use the context captured when opening the viewer:
`listArchives(context, cursor?)`,
`archiveConversations({ generationId }, context, cursor?)`,
`archiveSegments({ generationId, conversationId }, context, cursor?)` and
`archiveHistory({ generationId, conversationId, cryptoEpoch: null }, context, cursor?)`.
Pass an explicit decimal epoch string instead of `null` to select one retained session.
These reads also work through the native read-only owner of a revoked installation;
they do not grant sending permission. The SDK checks exact reply scope/schema, UTF-8
and wire limits, and cursor ordering. Its cursor cache holds at most 32 positions,
no plaintext, and expires entries after two minutes idle. A missing/expired SDK cursor gives
`InvalidStateError`; native code may independently return `stale` after its own TTL or
eviction. Explicitly refresh the first page in the same observed context.
A changed context requires reopening the view; do not retarget it automatically.
`historyReady` describes the local index, not a full-history audit. No read performs
setup, key replacement, repair, delivery ACK or network replay.

The Rust core also provides `open_archived_attachment(generation, conversation,
message)` on AccountClient and ArchiveClient for saved incoming files and outgoing
files already committed to Signal history/outbox. It validates local archive
ownership and the complete protected cache/spool, without a
network fallback or archive writes. AccountClient also supports native explicit
removal of incoming cache with a durably saved positive FILEACK response, and outgoing
spools with a saved Send receipt or verified sealed-unsent fence. History, keys and
delivery facts survive; unknown Send keeps its file for retry. The read-only
ArchiveClient cannot remove files. Attachment IPC/UI and forced pending/preparation
cleanup remain separate work.

Private-photo preprocessing is available as a native foundation with both processor
features enabled. It defaults to fresh metadata-free PNG pixels, optionally encodes
JPEG/WebP with selected quality, and requires an explicit retained-metadata choice
for original bytes. Animation/multipage cleanup is not implemented. The typed draft
handoff now runs through `AccountClient::prepare_photo` and the existing owned upload/
Signal pipeline. The E2EE codec slot survives a cancelled waiter; the account's file
slot spans preprocessing and protected preparation. Occupied UUIDs reject new input:
retry by retained preparation/outgoing lookup, without recompressing.
The experimental desktop SDK now exposes `preparePhoto(message, expectedContext)`
and `attachmentInput(target, expectedContext)`. Preparation uses binary IPC with a
20 MB source limit and obtains native admission before copying bytes. An occupied
UUID returns a conflict; resolve uncertain results by lookup, never by recompressing.
An initial catalogue is fetched through the existing serial RPC lane before source
preparation. Signal bootstrap still follows upload/READY. The SDK reports success
when native preparation commits locally. A protected chat reload reads pending
preparations through `pendingAttachments(conversationId, expectedContext, cursor)`.
Once promoted, an attachment appears in history with its filename, kind, size,
caption and the usual message delivery state. Use **Фото** in an active protected
chat to pick a source, add a caption and confirm preparation. The dialog remembers
cleanup quality and size per account when requested. It opens the settings for
photos above 5 MB and asks for consent every time an original is selected.
An unconfirmed attempt can be checked again using the same message UUID.
See the [photo contract](proto/attachments.md#native-private-photo-preprocessing).

Use **Файл** in a protected chat to confirm one file up to 50 MB with an
optional caption. The bytes, including any embedded metadata, are preserved.
The file uses the same protected preparation and upload queue as photos. If
confirmation is lost, the composer checks the original UUID and only submits
vacant input in the same cryptographic epoch.

**Голосовое** records a message with the active chat's microphone permission.
Stopping opens confirmation. Recordings up to 10 MB use the same upload and
retry path as files, with the Opus container identified by Rust. **Прослушать**
opens the saved local copy in the chat or archive. The WebView discards the
player on exit. [MediaRecorder specification](https://www.w3.org/TR/mediastream-recording/)

Use **Скачать** on a received attachment to save it in the protected cache for
the current account. The client verifies the file before marking it saved and
queuing the file acknowledgement. The saved state survives restart. The archive
shows availability using local data. **Предпросмотр** opens a reduced PNG of a
saved photo. **Сохранить копию…** exports the exact saved bytes to a destination
you choose in the Windows save dialog. Existing files stay intact. The dialog
labels the exported copy as unencrypted. [Windows save dialog](https://learn.microsoft.com/en-us/windows/win32/api/shobjidl_core/nn-shobjidl_core-ifilesavedialog)

Open **Local archive** from the encrypted-dialog list or **Security → Open local
archive**, including on a revoked installation. A separate read-only modal selects
a saved installation, dialog and optional encryption session. Lists page eight
entries at a time; history keeps at most 128 visible messages, with explicit older-page
loading and refresh. It preserves the active draft and recovery consent. A changed
account/device context clears the view and requires reopening it.

The experimental chat UI has a separate encrypted-dialog list and a single
visibility-aware local refresh loop. It reads one dialog page, one selected-chat
history page and up to eight outgoing statuses per pass. Conversation UUIDs never
enter the ordinary HTTP/WebSocket chat queues. History keeps a bounded window;
while reading older messages, incoming text is announced without moving the view.
Account retirement discards late replies, and a changed owner context pauses the
old draft until the user reopens a dialog. These reads never create keys, recover
sessions or acknowledge delivery.

Manual recovery uses `recoveryStatus(conversationId)`,
`proposeRecovery({ conversationId, expectedEpoch, expectedOwnProposalId }, context, true)`
and `decideRecovery({ conversationId, expectedEpoch, proposalId, decision }, context, true)`.
The decision is `accept`, `decline` or `cancel`; native code enforces participant roles.
Read the local status first. The expected epoch is a decimal string; an absent own
proposal is an explicit `null`. Mutations require the observed desktop context and
fresh user confirmation. A queued result means a signed intent committed locally,
not successful cancellation or a completed transition. After an ambiguous proposal
result, read its saved status: never silently change the expected proposal reference
and resubmit. An old reference conflicts, including after a terminal proposal or
an epoch change. Reading status never bootstraps an uninitialized conversation.
The experimental selected-chat UI provides these manual recovery actions with a
separate confirmation for each intent. A changed native epoch/proposal invalidates
that confirmation. Native freeze or unavailable status pauses sending while keeping
RAM drafts and local history. Recovery actions require explicit confirmation.
The archive viewer preserves access to older history.

E2EE stays opt-in for the local demo.

`createConversation(peerUserId)` replays its saved creation attempt even when that
dialog has closed. To advance that peer slot explicitly, call
`createConversationAfterClosed(peerUserId, closedConversationId, true)` after user
confirmation. Rust resolves the known closed result to its protected request;
the renderer cannot supply a creation UUID or generation. Only that head or its
direct successor can be selected, so a lost reply/reopen retries the same request
and stale consent cannot advance a later dialog. A missing/foreign journal fails
without repair. Old history, keys and outbox remain intact. This action does not
bootstrap Signal or replace a device; it is separate from same-dialog recovery.
The successor fixture checks current friendship/privacy/keys, lost server COMMIT
replies, reopen, concurrent retries and navigation/account fences.

Experimental desktop profiles offer friend-only encrypted creation and explicit
closed-chat consent. Public UUIDs from the authenticated friends response bind
profile IDs to native dialog metadata. The separate local-history view clears the
ordinary numeric selection and hides its composer, attachments and calls; text is
rendered literally and never enters browser storage. It supports explicit refresh,
older-history paging and a moving window of at most 128 whole texts. A separate
encrypted composer keeps at most eight page-local drafts. Unknown outcomes retain
the same immutable UUID and text for explicit retry; only native durable acceptance
clears the field. Clearing an uncertain attempt requires confirmation and does not
retract native delivery. No plaintext draft is written to browser storage. Local
dialog discovery and live incoming refresh are the next UI slice. Browser profiles
offer an existing ordinary DM instead.

## Verification

During development, run tests for the affected modules and related contracts.
The full-suite commands below are for CI and final reviews.

```bash
npm run typecheck:chat
npm run test:chat
npm run test:desktop
go mod verify
go vet ./...
go test -race -p=1 ./... -count=1 -timeout=600s
cargo fmt --all --check
cargo test -p rust-processor --locked
cargo test -p zchat-desktop --locked
```

Set `ZCHAT_TEST_DATABASE_URL` to a **dedicated disposable PostgreSQL database**
to enable DB suites. They create/drop test schemas; without the variable they skip.
Desktop Rust tests need the target platform's native prerequisites.
Acquisition-expiry fixtures move deadlines only inside disposable test schemas
and restore database immutability guards in the same transaction. They do not
change production clocks, lease durations or application data.

For the experimental E2EE core, install `protoc`, then:

```bash
cargo test -p rust-processor --locked --no-default-features --features e2ee --all-targets
cargo clippy -p rust-processor --locked --all-features --all-targets -- -D warnings
```

The opt-in Go/Rust interoperability test additionally needs the compiled Rust
unit-test executable, not the ordinary processor binary:

```bash
cargo test -p rust-processor --locked --no-default-features --features e2ee --lib --no-run
# Set ZCHAT_RUST_E2EE_TEST_BIN to the absolute test executable printed above.
go test -race ./internal/grpc -run '^TestE2EERust(Native|DeliveryOutage)Postgres$' -count=1 -v
```

For the owned file transport, use the same Rust test executable and disposable DB:

```bash
go test -race ./cmd/server -run '^TestE2EEOwnerFileInteropPostgres$' -count=1 -timeout=180s -v
```

This checks the production storage runtime, TLS gRPC/HTTP, real Signal/local stores,
lost replies, identical file/Send retries, logout/reopen and signed epoch recovery.
Its disposable schema clock advances the staging lease; key acquisition uses a
separate real-clock pool and keeps the native clock-skew guard. This is a lease
expiry fixture, not whole-system clock-jump acceptance. Download, file ACK and GUI are
not covered. To use a Windows Rust test executable from WSL, also set
`ZCHAT_RUST_E2EE_TEST_WINDOWS=1` so the fixture converts its temporary paths.

Keep the disposable DB variable set. Go starts a temporary TLS server and explicitly
runs both ignored Rust workers. It checks JWT account discovery, registration/publication
recovery, password/CAS replacement with preserved archives, durable conversation
creation across lost replies/restarts/peer replacement, real Signal crypto and
idempotent ciphertext Send, receipt/status journals after reopen, bounded recipient
inbox RPC and atomic protected receive/quarantine before worker-bound ACK replay.
It also checks durable Create/Get closure convergence and accepted-message delivery
after the sender replaces their installation.
These interop workers currently use an isolated, test-only prototype transport.
The production experimental service uses protocol v2. Its separate native transport
is checked by `TestE2EERustEpochTransportPostgres` with the same test DB/binary variables:

```sh
go test -race ./internal/grpc -run '^TestE2EERustEpochTransportPostgres$' -count=1 -v
```

This test uses the production registry/v2 graph, real recovery signatures and injected
lost/corrupted replies. It checks protected initial-epoch commit/reopen, durable
acquisition and real Signal bootstrap. An initial-epoch message is encrypted into
an atomic protected outbox, loses its server reply, then reopens and resends the
same persisted ciphertext before committing the receipt. Local crypto tests cover
v2 callbacks, EC/PQ variants, outgoing process exits and bounded index recovery.
The recipient commits the first PreKey receive together with its automatic Signal
confirmation. The reply survives reopen and exact retry after a discarded server
receipt; the next sender text uses Signal without a confirmation loop. After server
sealing, the original receive is replayed, a fresh accepted Signal text is decrypted,
and the original ACK is retried after a lost server response and local reopen. The sender also commits Pending/Stored status observations, restores them
after reopen and excludes terminal messages from its pending index. A malformed
ciphertext probe checks that processing failure creates no ACK. Explicit native
quarantine then retains that ciphertext and commits Rejected proof; the ACK survives
a discarded remote success, local reopen and exact retry. Recovery proposals and
explicit native acceptance commit signatures and send freeze before RPC; both
recover exact requests after lost replies and local reopen. An authenticated bounded
outgoing audit blocks activation until unknown old sends resolve by exact receipt
or signed sealed fence, without waiting for recipient ACK. Both workers then commit
and reopen the new epoch, exchange real PreKey/confirmation/Signal messages and
retain old receive/ACK/status access. All recipient ACKs use protected proofs.
Unit tests cover pre/post-activation process exits, storage rollback, partial replay,
bounded audit resume and a second transition with the opposite proposer. Desktop
consent UI and desktop owner wiring are still pending.

`TestE2EERustAccountOwnerPostgres` uses the same test DB/native-binary variables
to exercise the owned account runtime. Two account stores run delivery and key
maintenance together, replay lost Send/ACK replies, accept recovery through explicit
native commands, and reopen without replacing identity or losing old-epoch history.
User outboxes are prepared by the fixture while the owner is stopped; text-send
IPC/history paging and Tauri login/logout integration are still pending. Runtime
tests also cover queue rejection, canceled requests, foreground reads during a
stalled RPC, fatal authentication and awaited store release.

`TestE2EERustEpochTransportPostgres` also drives the production v2 runner through another recovery:
saved control replay after lost replies and worker reopen, receipt/fence settlement,
automatic approved-proposer bootstrap, mixed old/new inbox, confirmations and statuses.
It closes actual TLS connections after Send and ACK commits, keeps the server and DB
alive and checks reconnect through the original clients. The continuous loop also
runs with unavailable optional hints and cancels cleanly. Every round has a64-RPC
budget, journal pages contain at most32items, and local/server recovery discovery
handles at most2chats each. Unit tests cover competing offers, read-only replay,
poison messages, pagination, timeout retry and cancellation. These are bounded-core
checks, not a highload, GUI, WAN or PostgreSQL restart acceptance.
The initial-contract and runner phases execute sequentially at the same runtime root
to keep large debug test frames off nested signature-verification calls.
Bounded native polling/recovery/backoff/cancellation are tested with real Signal
and injected failures; the opt-in Go/Rust test drives the actual TLS adapter.
Automatic Signal confirmation is tested for transaction rollback, process exit,
exact replay and no reply loops, including actual TLS delivery. The outage test
force-closes connections after Send/ACK commits, rebuilds the TLS server and service
graph twice on the same endpoint, and checks exact recovery through original native
channels. It also checks pending cancellation, the native10s deadline, a2s runner
round timeout and post-commit Send cancellation using server-context barriers.
The native adapter distinguishes its proven local transport timer (`LocalDeadline`)
from remote `Cancelled`/`DeadlineExceeded` statuses. A remote code or diagnostic text
cannot claim a local timer or user cancellation. The owner stops work by dropping
the in-flight future; an expired RPC can still have committed on the server, so
delivery and key maintenance retain their durable retry journals. The transport-only
`TestE2EERustDeadlinesTLS` checks both registry and v2 clients against real TLS;
it needs `ZCHAT_RUST_E2EE_TEST_BIN` but no PostgreSQL connection.
This scenario exercises transport outages. WAN, host failure and capacity
are separate checks.
From WSL with a Windows test executable, also set `ZCHAT_RUST_E2EE_TEST_WINDOWS=1`
for CA-path conversion and ensure the executable has execute permission.
The separate `TestE2EERustWakeupsPostgres` also needs `ZCHAT_TEST_REDIS_ADDR` and
`ZCHAT_TEST_REDIS_CONTAINER`: an explicitly disposable Docker Redis labelled
`zchat.task=e2ee-wakeups`. This test stops/restarts that container and checks two
TLS endpoints, cross-hub hints, polling recovery and native stream reconnect.

After protobuf edits, use `make gen` and `bash scripts/gen-proto.sh --check`.
Install the exact generator versions used in [CI](.github/workflows/ci.yml).
For a complete packaged HTTP/WS check:

```bash
docker build --target final -t zchat:check .
bash scripts/smoke-compose.sh zchat:check
```

The packaged check repeats HTTP/WS/session/attachment flows with Redis absent,
running and stopped, using one uninterrupted Go server. Redis is excluded from
default startup and readiness; `make redis-up` enables its Compose profile.
The isolated load lab explicitly retains Redis for historical resource comparisons.
The preserved Pub/Sub wrapper has a separate opt-in `TestPubSubRedis` integration
test: set `ZCHAT_TEST_REDIS_ADDR` to a disposable Redis address and run
`go test -race ./internal/redis ./internal/e2ee/wakeup -count=1`.

For the experimental native E2EE core, local wake-up hints work without Redis.
With Redis running, set `E2EE_REDIS_ADDR=redis:6379` for Compose or a host:port
address for a host-side Go process. It is used only when
`ENABLE_EXPERIMENTAL_E2EE=true`; empty means no Redis connection. Enabling a profile
alone does not configure a connection. Redis failure leaves persistence, readiness
and periodic polling available. This does not enable desktop E2EE UI.
The optional `WatchInbox` RPC carries only bounded hints; `StreamMessages` remains
unimplemented. Native callers use `inbox_wakeups()` with `run_with_wakeups()`.
To repeat the packaged check with the core enabled and Redis configured (including
the initially absent Redis), run
`SMOKE_EXPERIMENTAL_E2EE=true bash scripts/smoke-compose.sh zchat:check`.

Server-copy cleanup runs independently of this API flag: one worker alternates expiry
and replaced-recipient sweeps, up to 32 candidates per pass with a 2s context budget.
It preserves receipts/deduplication and late ACKs; it does not delete local private keys.
Prometheus exposes `e2ee_cleanup_passes_total{lane,result}`,
`e2ee_cleanup_removed_total{lane}` and `e2ee_cleanup_busy_total{lane}`.
With the disposable DB configured, run its transaction/concurrency tests using
`go test -race ./internal/repository ./internal/service -run '^TestE2EECleanup' -count=1`.
The packaged smoke check also requires successful passes in both lanes.

## CI and acceptance

[CI](.github/workflows/ci.yml) checks Go race/PostgreSQL, module/vulnerability
checks, generated protocols, frontend tests/build, npm advisories and the packaged
runtime. [Native](.github/workflows/native.yml) checks the Rust core on Linux and
builds/tests Windows desktop (default and experimental session feature), compression
and default-installer license notices.

A green workflow applies to its exact commit. Native CI runs a real Go/Rust
epoch recovery scenario on PostgreSQL. Real GUI, physical media, WAN/TURN
and capacity acceptance remain separate checks.
Rust dependency findings are non-blocking in development CI: the audit step emits
warning annotations and a job summary. Scanner/database failures or incomplete reports
still fail the job. The policy is checked by `python3 -B scripts/check-rust-audit.test.py`;
plain `cargo audit` remains strict for distribution checks. No advisory ignores are used.
Go's `govulncheck` remains blocking for vulnerabilities reachable by the application;
findings outside detected call paths do not fail that check. A failed Go check skips
later steps and the dependent runtime-image job. Native runs independently.

The 2026-11-08 Rust check patched the protobuf build dependency `pulldown-cmark` to0.13.6
(RUSTSEC-2026-0433): zero vulnerabilities, with the existing Linux `glib` unsoundness
and two unmaintained macro warnings. The recorded Go scan (2026-10-29, Go1.26.10) found no
reachable vulnerabilities and four imported/module findings outside detected call paths.
The 2026-11-13 CI found GO-2026-6859 in Go1.26.10's `net/http`; the minimum Go
version and pinned Docker builder were updated to the fixed Go1.26.11.
Run both scanners again before distribution; results apply to the checked graph and
advisory-database snapshot, not every possible runtime path.

## Calls and deployment

Calls need separate LiveKit credentials and `LIVEKIT_URL` in private configuration:

```bash
docker compose -f compose.yaml -f compose.calls.yaml up -d --build
```

Keep signaling behind Go's `/rtc/` admission gateway; do not publish LiveKit's
internal signaling/management port directly. ICE media ports need a host address
reachable by the client; `LIVEKIT_NODE_IP` and `LIVEKIT_MEDIA_BIND_IP` configure the
local profile. Different-network operation and TURN need separate acceptance.
See [monitoring.md](monitoring.md) for observability and an isolated test lab.

Before public hosting: HTTPS and proxy trust, storage quotas/retention/orphan
cleanup, backup/restore and resource budgets. Before multiple Go instances:
distributed revocation, ticket routing and delivery. Review source/history for
secrets and distribution notices before making the repository public.
