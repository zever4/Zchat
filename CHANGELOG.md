# Changelog

## 1.1.0 — Windows Preview (2026-11-23)

- Keep the Windows desktop composer visible and history scrollable when opening
  chats with many messages, including photos and after restarting.
- Add a Windows microphone permission reset in chat settings through WebView2,
  retaining login, cookies and preferences and asking for consent again.
- Provision backend, PostgreSQL, call and load dashboards; add an isolated k6 lab.
- Retain warmed PostgreSQL connections with a bounded 25/25 open/idle pool.
- Confirm ordinary message persistence after commit, deduplicate retries and keep
  a per-server/account pending queue across reconnects and application restarts.
- Add experimental encrypted Windows private chats with text, photos, files, voice
  and local archives. Photo preparation can compress and remove EXIF/GPS before
  encryption. Verify delivery and recovery across Go, Rust and PostgreSQL.
- Add audience controls for new direct chats and custom sounds for notifications.
- Apply current group permissions when editing messages. Preserve the author
  mask on messages sent in anonymous mode and omit typing when policy is unavailable.

## 1.0.0 — Windows Preview (2026-10-18)

First Windows x64 desktop installer, built with Tauri and the shared chat interface.
The Go backend is started separately; this release does not include a hosted service.

- Private/group chat, attachments, voice messages and session controls.
- Bundled frontend, SDK and fonts, with native HTTP/WebSocket transport scoped to
  the selected server and a restrictive content security policy.
- Windows title bar, server settings, persistent login, reconnect/offline recovery
  and WebView2 downloads.
- Optional photo compression with quality/size controls, preview and remembered
  choices; ordinary messages do not depend on the experimental E2EE implementation.
- Per-user Russian/English NSIS setup with project and dependency license notices.
- Go/PostgreSQL race/integration checks and separate Windows/Rust CI.

This preview excludes E2EE, automatic updates, code signing and Linux/macOS GUI
acceptance. See [demo.md](demo.md) for current source status and
[development.md](development.md) for builds and packaging.
