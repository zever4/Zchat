# Demo walkthrough

Run `make init` and `make up`, then open [localhost:18080](http://localhost:18080).
Use two independent browser profiles and synthetic demo accounts/data.

## Suggested walkthrough

1. Register Alice and Bob. Send and accept a friend request, then open a private chat.
2. Exchange messages, replies, edits/deletions, and scroll through history.
3. Open a second tab for one account. Try reconnecting and retrying a pending message;
   show that the same message is saved once and missed live updates load from history.
4. Send a photo, file and voice message. On desktop, compare original/compressed
   photos, quality/size preview and the remembered choice for photos up to 5 MB.
   Larger photos ask again. Video is a file attachment without compression/player.
5. Create a group, join by invitation and demonstrate member write permissions.
6. Sign in twice independently. Logout preserves the other login; logout-all and
   a password change revoke both, including already-open sockets.

Stop with `make stop`; data volumes remain. This is a manual review script,
not a claim that every interface interaction is automated.

## Current feature status

| Area | Current boundary |
| --- | --- |
| Browser chat | Private/group chats, permissions, history, attachments and sessions checked locally and through API regressions |
| Message reliability | Persisted ordinary-message outbox, same-UUID retries and ACK after database commit; ACK is not recipient delivery/read confirmation |
| Windows desktop | Shared UI, native HTTP/WS, persistent login, reconnect, downloads, photo compression and NSIS preview checked locally |
| Calls and voice | Maintainer confirmed browser/desktop voice, calls and screen sharing; recovery/revocation tests also exist |
| Experimental Windows E2EE | Two local desktop clients verified text, photos, files, voice, offline retry, local history after restart and recovery with peer consent. Enabled through a separate build feature and server flags. |
| Other platforms/networks | Linux/macOS GUI, physical-device edge cases and WAN/TURN acceptance pending |

For calls, enable the [LiveKit profile](development.md#calls-and-deployment).
Use a physical microphone and headphones: Stereo Mix captures playback and can
create a feedback loop even when the other client's microphone is muted.
For a second physical device, localhost-only ports require additional network
configuration. A successful local call does not verify operation across networks.

The [CI workflows](development.md#ci-and-acceptance) cover code/build/API checks.
They do not replace manual GUI/media checks. See [monitoring.md](monitoring.md)
for a separate metrics/load demo and [architecture.md](architecture.md) for design.

## Experimental E2EE

Use two Windows clients with separate profile directories and start the local
server with both experimental flags. Configure the clients with the same
loopback server. [Build and configuration](development.md#experimental-desktop-sessions)

1. Register Alice and Bob, accept a friend request and set up keys in **Security** on both clients.
2. Open a protected conversation from the friend's profile and send a text message.
3. Choose **Фото**, then open **Сжатие и метаданные**. Select **Очистить метаданные и сжать**, choose quality and send. On the other client, download and open the photo.
4. Stop the server briefly and send another text. Start the server and check that the queued message arrives once. Reopen the client to see retained local history.
