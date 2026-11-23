# Shared E2EE contract fixtures

## Sender file status public metadata

`file-status-v1/` holds synthetic `GetFileStatusesRequest`/`FileStatuses` protobufs
and textprotos. The sender context queries the same public attachment3 identity as
the FILEACK vector, with epoch2^53+17. ACK, a passed retention deadline and recipient
replacement coexist. Go/prost independently re-encode and validate the same bytes.
There are no keys, credentials, file paths or actual accounts. This is shape/wire
coverage, not an authenticated RPC, file-availability or deletion proof. Format1
must be bumped for new critical semantics; prost discards unknown protobuf fields.
Reproduce with `protoc -I proto --encode=zchat.e2ee.v2.GetFileStatusesRequest` or
`--encode=zchat.e2ee.v2.FileStatuses` against `proto/e2ee_epoch.proto`, using the
corresponding textproto as stdin and comparing to the matching `.bin`.

## File ACK public metadata (static vector, no cache proof)

`file-ack-v1/` holds synthetic public `AcknowledgeFilesRequest`/`FileAcknowledgements`
protobufs and readable textprotos. They bind the attachment3 vector below, including
epoch2^53+17, exact envelope digest and BlobRef; API context remains2. Independent
Go/prost tests re-encode the same bytes and validate exact ordered receipts. There
are no private keys, credentials, filenames, working accounts or durable-cache
proofs. These fixtures do not prove an RPC, native disk COMMIT or physical GC.
Go rejects unknown nested fields; prost discards them, so future critical semantics
require explicit versioning rather than adding an unknown field.

Reproduce from the repository root into temporary files:

```sh
file_ack_candidate=$(mktemp)
protoc -I proto --encode=zchat.e2ee.v2.AcknowledgeFilesRequest proto/e2ee_epoch.proto < proto/fixtures/file-ack-v1/request.textproto > "$file_ack_candidate"
cmp proto/fixtures/file-ack-v1/request.bin "$file_ack_candidate"
protoc -I proto --encode=zchat.e2ee.v2.FileAcknowledgements proto/e2ee_epoch.proto < proto/fixtures/file-ack-v1/response.textproto > "$file_ack_candidate"
cmp proto/fixtures/file-ack-v1/response.bin "$file_ack_candidate"
rm "$file_ack_candidate"
```

## Attachment envelope3 (pure contract, storage disabled)

`attachment-envelope-v3/` contains a public synthetic `Envelope` for the
[attachment contract](../attachments.md): readable `envelope.textproto`, protobuf
`envelope.bin`, canonical `envelope.frame` and `design-vector.json`. Go and Rust
independently validate and reproduce the same frame/hash. The historical JSON
filename does not imply enabled server storage: Send3 requires the explicitly
owned experimental blob runtime; default capabilities/readiness remain false.

The vector contains a fake37-byte blob and three fake Signal bytes, with no usable
AEAD/Signal encryption, file key, credential or user data. Never send these bytes.
The private attachment fixture below is separate, not the decryption of this vector.
The expected canonical frame is354bytes, SHA256
`c8cd0ccf0cb367bcaf43789aaa96decad7741052b90c93c340c703f5f633493f`.
JSON u64 values are decimal strings, including epoch2^53+17. Existing v2 vectors
remain byte-identical. Text/confirmation use outer2 without blob; attachment uses
outer3 with one format1 public ciphertext ref. API/acquisition/recovery controls stay2.

Reproduce protobuf bytes into a temporary file and verify the JSON frame/hash:

```sh
candidate_blob=$(mktemp)
protoc -I proto --encode=zchat.e2ee.v2.Envelope proto/e2ee_epoch.proto < proto/fixtures/attachment-envelope-v3/envelope.textproto > "$candidate_blob"
cmp proto/fixtures/attachment-envelope-v3/envelope.bin "$candidate_blob"
rm "$candidate_blob"
python3 - <<'PYVERIFY'
import hashlib, json
from pathlib import Path
root = Path('proto/fixtures/attachment-envelope-v3')
vector = json.loads((root / 'design-vector.json').read_text())
frame = (root / 'envelope.frame').read_bytes()
assert frame == bytes.fromhex(vector['expected_frame_hex'])
assert len(frame) == 354
assert hashlib.sha256(frame).hexdigest() == vector['expected_sha256_hex']
PYVERIFY
```

## Private attachment payload3 (not enabled)

`epoch-data-v2/attachment.bin` and `attachment.textproto` contain **public synthetic**
private-payload format3 for one file attachment. Its secret-descriptor slot is a
DELIBERATELY PUBLIC DUMMY: version1, key32bytes of0x11, hash32bytes of0x22, size42.
There is no real blob, usable encryption, credential, identity key or user data.
Never use these dummy bytes for an actual file. Both Go and Rust consume this same
453byte vector; it is not a Signal ciphertext or the decryption of either public
envelope vector. Private descriptor bytes remain unchanged.

Text/SessionConfirmation retain outer protocol2/private format2 and unchanged
canonical bytes. Attachment private format3/nested1 now requires outer3 with a
matching public blob reference; outer2 attachments are rejected. A format2 reader must reject3, not silently
ignore unknown critical content. Further critical semantics require version bumps.
Go rejects unknown nested fields; prost discards them as described below.

One attachment per message: private blob UUID (no URL), kind/photo-file-voice,
73byte native descriptor (including file key), bounded filename/MIME/caption and a
required photo-only metadata-policy presence bit. Files<=50*1024*1024bytes, photos/
voice<=10*1024*1024; filename<=255UTF8bytes, caption<=2000. MIME allowlist by kind,
basename/control/bidi checks do not authorize OS file access or prove media safety.
The photo-policy bit is the sender's claim, not proof of EXIF/GPS removal.

Key/filename/MIME/caption travel ONLY in authenticated Signal plaintext and protected
native history. Go's matching plaintext validator is for contract tests, not the
server messaging path. Never log the private proto; native Debug redacts it and
Drop zeroizes its secret/metadata buffers. Presentation summaries contain no key.
Message ACK acknowledges the saved descriptor; durable blob storage needs a separate
file ACK. Upload/cache/GC/quotas/IPC/metadata sanitizer are not enabled by this vector.

To reproduce only the new vector:

```sh
candidate_attachment=$(mktemp)
protoc -I proto --encode=zchat.e2ee.v2.EncryptedPayload proto/e2ee_epoch.proto < proto/fixtures/epoch-data-v2/attachment.textproto > "$candidate_attachment"
cmp proto/fixtures/epoch-data-v2/attachment.bin "$candidate_attachment"
rm "$candidate_attachment"
```

Wire SHA256: `aee64700dde12e9990b60d6e8c55a30417de3e16f68a112ce5f22ab83143991e`.
Protobuf wire serialization need not be identical across libraries: compare decoded
fields, and use only the specified canonical Envelope frame for receipt hashes.

## Epoch-bound data format v2 (not enabled)

`epoch-data-v2/` contains deterministic public vectors for the separate
`zchat.e2ee.v2` data contract in `e2ee_epoch.proto`: initial/nonzero-epoch Envelopes,
payload, SessionConfirmation, acquisition request/public response and SendReceipt.
The three ciphertext bytes are deliberately opaque and **not a real Signal message**;
the payload is synthetic test plaintext, not the decryption of those bytes. Public
prekey material is reused from the reviewed public-key fixtures below. No credentials,
private keys, user database or working accounts are involved.

V2 has **no legacy v1 reader or automatic migration**. Epoch0 is the first valid
session of a new conversation, not a compatibility format. `crypto_epoch` uses
protobuf optional uint64 with mandatory presence, including zero, bounded by
`INT64_MAX`. Missing epoch cannot silently become zero. The envelope's protocol,
text/control payload format, acquisition response and request context require version2.
The new attachment plaintext format3 is documented separately above.
Shared v1-namespaced DeviceRef/Device/prekey types are registry metadata, not
support for v1 ciphertext. The existing prototype transport is still separate;
there are no v2 RPCs or new messaging capabilities enabled by these vectors.

Envelope digest = SHA256 of this frame, independent of protobuf wire field order:

| Field | Encoding |
| --- | --- |
| Domain | ASCII `zchat.e2ee.envelope.v2` followed by one NUL |
| Protocol, crypto suite | Two u32 big-endian integers |
| Message UUID, conversation UUID | Canonical lowercase UUIDv4, 36 ASCII bytes each |
| Crypto epoch | u64 big-endian, mandatory even for zero |
| Sender, then recipient | Each: account UUID (36), device ID (u32 big-endian, MVP1), generation UUID (36) |
| Ciphertext type, length | Two u32 big-endian integers |
| Ciphertext | Exact bytes |

`initial.frame` binds epoch0; `envelope.frame` binds `2^53+17`. Full frames and hashes
are checked by independent Go/Rust implementations. The v2 digest derives the
exact Send binding consumed by the signed recovery-fence contract. UUID/digest/
epoch/participants and accepted transition must all match; a v1 digest cannot
substitute. This remains structural/crypto verification, not server COMMIT or ACK.

After authenticated Signal decrypt, native must compare payload epoch AND all
routing fields with its envelope. SessionConfirmation repeats the epoch and cannot
reply to itself. An authoritative/retained epoch catalog is still needed: matching
two untrusted fields alone does not authorize a scope. Acquisitions bind request UUID,
conversation, peer and epoch; native additionally checks pinned identity/registration
and actual libsignal EC/PQ signatures. EC is optional; one-time or last-resort PQ is
required. Lease freshness is checked by the native owner, not these static vectors.
Receipts bind epoch/message/digest and fixed server30d TTL without extending an exact
retry relative to the client's clock. Neither pure helper changes private state.

Go rejects unknown nested fields; prost discards them. Critical future semantics
require version bumps. Raw Envelope decoders check the48KiB byte bound before decode.
Runtime implementation and acceptance are described in [architecture](../../architecture.md).
These static vectors alone prove contracts, not runtime or desktop readiness.

To reproduce into a **new** directory (existing directories are refused):

```sh
cargo run -p rust-processor --locked --offline --no-default-features --features e2ee \
  --example generate_epoch_fixture -- /tmp/epoch-data-v2-candidate
diff -ru --exclude=attachment.bin --exclude=attachment.textproto \
  proto/fixtures/epoch-data-v2 /tmp/epoch-data-v2-candidate
```

## Epoch-recovery control format v1 (messaging protocol 2, not enabled)

`epoch-recovery-v1/` contains **synthetic public** libsignal identity keys,
signed Propose/Accept/Decline/Cancel control frames, three `RecoveryOutcome`
protobufs, and a typed `EpochSealedFence`. There are no private keys, credentials,
working accounts or real message ciphertext. The fence's envelope digest is
32 bytes of `0x42`, not a digest of a real protocol2 Envelope. Epoch/revision
values exceed `2^53` to exercise full-width integer handling in Go and Rust.

These contract vectors do not exercise recovery RPC/database transitions.
The public-key registry and v2 messaging transport are separate APIs; readiness remains
false until acceptance. Merely adding fixtures does not enable messaging or recovery.
The protobuf namespace remains `zchat.e2ee.v1`; the explicit control protocol is 2,
and its canonical frame format is 1. Unknown format/protocol/suite/action values
must fail closed. Go rejects unknown fields at every control nesting level;
prost discards them, so critical future semantics require an explicit version bump.

Identity signatures cover the **entire canonical frame**, not its SHA256 digest:

| Field | Encoding |
| --- | --- |
| Domain | ASCII `zchat.e2ee.epoch-recovery.v1` followed by one NUL |
| Protocol, format, suite, action, derived signer role | Five u32 big-endian integers |
| Conversation, operation request, proposal UUID | Three canonical lowercase UUIDv4 strings, 36 ASCII bytes each |
| Expected epoch, expected revision | Two u64 big-endian integers, each less than `INT64_MAX` to permit increment |
| Initiator, then peer | Account UUID (36), device ID (u32 big-endian, MVP 1), generation UUID (36), fingerprint length (u32 big-endian, 32), fingerprint bytes |
| Proposal digest | Length (u32 big-endian), then bytes: empty for Propose, 32 bytes for decisions |

Fingerprint = SHA256 of the pinned 33-byte serialized libsignal identity public key.
Actions: Propose=1, Accept=2, Decline=3, Cancel=4. Derived signer role is 1 for
Propose/Cancel (initiator), 2 for Accept/Decline (peer). Propose's request UUID equals
its proposal UUID. A decision has its own persisted request UUID and binds the
original proposal UUID and SHA256(frame). Signatures are generated with libsignal's
identity curve API, not standard Ed25519. A signature/shape check alone does not
prove manual consent, authenticated server COMMIT, expiry or a durable local change.
The native owner must persist the exact frame/signature before RPC and reuse
them on retry. An accepted outcome must contain both signatures, match the exact
saved proposal, and increment epoch/revision by exactly one.

The typed fence additionally binds the exact message UUID/digest, participants,
chat and old epoch to that accepted transition. It is not an ACK/read receipt or
permission to delete ciphertext. Generic transport errors cannot substitute for it.
Rust checks both retained identity pins and signatures; Go currently checks only
structure, digests and bindings. Neither helper activates an epoch or writes an outbox.

`*.frame` are golden signing bytes, `frames.sha256` their SHA256 digests. Go and Rust
compare complete frames/hashes, reorder protobuf fields, reject replay/mutation,
and retain the original v1 envelope bytes/digest. Rust also verifies the actual
public signatures and rejects changed signatures, wrong signers and signing a digest
instead of the frame. V2 routing rejects legacy v1 even at epoch0; the separate
prototype v1 validator continues rejecting protocol2 until that transport is retired.

To create a **new candidate directory**, never overwrite reviewed vectors:

```sh
cargo run -p rust-processor --locked --offline --no-default-features --features e2ee \
  --example generate_recovery_fixture -- /tmp/epoch-recovery-candidate
```

Generation is random; private keys remain in memory and are never written. New
public keys/signatures/hashes will differ. Normal tests consume the reviewed files
and never regenerate them. Server CAS/replay/Send races and native durable intents
will be tested in later SESSION1 steps, not inferred from these fixtures.

## Versioned public-key publication

`e2ee-v1-publication.bin` is a 3537-byte `zchat.e2ee.v1.UploadPreKeysRequest`.
It maps the already reviewed public material in `prekey-upload-v1.bin` to v1,
with generation `33333333-3333-4333-8333-333333333333`, batch
`77777777-7777-4777-8777-777777777777` and expected publication revision 7.
It contains no credentials, private keys or working account. Go tests compare
the complete mapping with the legacy public fixture; Rust tests consume the
same file and verify the receipt frame and byte-limited batch planning.

Wire SHA256: `e260d054fd4fbbbcced2c92171eecc8a8ba0f50a9575ee985683528a756bf4c2`.
Publication digest: `1832c4d75117af6ad403fde33200a57ec5cb47c2c2667d59edb373080f1cb6e1`.

The publication digest is SHA256 of these fields in order:

| Field | Encoding |
| --- | --- |
| Domain prefix | ASCII `zchat.e2ee.publication.v1` followed by one NUL |
| Protocol version | u32 big-endian |
| Generation, batch UUID | Two canonical lowercase UUIDv4 strings, 36 ASCII bytes each |
| Expected revision | u64 big-endian; must be less than signed BIGINT maximum |
| Signed-key presence | u32 big-endian, 0 or 1 |
| Signed key, if present | ID (u32), public bytes (33), signature (64), nonnegative Unix ms (u64), integers big-endian |
| EC key count | u32 big-endian |
| Each EC key, in request order | ID (u32 big-endian), public bytes (33) |
| Kyber key count | u32 big-endian |
| Each Kyber key, in request order | ID (u32 big-endian), public bytes (1569), signature (64), last-resort flag (u32 big-endian, 0 or 1) |

Lengths/tags/counts/UUIDs are validated before hashing. Go rejects unknown fields
at every publication nesting level; prost discards unknown fields, so new
protocol-critical semantics require versioning. Repeated-key order is part of
an exact batch, while protobuf wire field order is not. The hash is not a MAC,
signature or proof of storage by itself. Verify batch ID, digest and committed
revision in the authenticated response. Exact retries return the original
receipt even after later rotations; they do not reactivate old public keys.
An upload receipt never authorizes deleting the corresponding private keys.

## Versioned message envelope

`e2ee-v1-envelope.bin` is a 247-byte `zchat.e2ee.v1.Envelope` from
`proto/e2ee.proto`. Its readable source is `e2ee-v1-envelope.textproto`.
It uses fixed synthetic account/conversation/message/generation UUIDs and three
opaque bytes as ciphertext. **These bytes are not a real Signal ciphertext.**
The fixture has no credentials, private keys or working accounts.

Wire-file SHA256: `0292509a87f63fd842f62a9d24cfb2a2bdae6eb42e1b7f8b34b15d8551dce946`.

To reproduce this deterministic file after reviewing its readable source:

```sh
protoc -I proto --encode=zchat.e2ee.v1.Envelope proto/e2ee.proto \
  < proto/fixtures/e2ee-v1-envelope.textproto > /tmp/e2ee-v1-envelope.bin
cmp proto/fixtures/e2ee-v1-envelope.bin /tmp/e2ee-v1-envelope.bin
```

### Envelope receipt digest

Go and Rust compute SHA256 of this explicit byte frame, in this order:

| Field | Encoding |
| --- | --- |
| Domain prefix | ASCII `zchat.e2ee.envelope.v1` followed by one NUL byte |
| Protocol version, crypto suite | Two unsigned 32-bit big-endian integers |
| Message UUID, conversation UUID | Two canonical lowercase UUIDv4 strings, 36 ASCII bytes each |
| Sender, then recipient | Each: account UUID (36 bytes), device ID (u32 big-endian), generation UUID (36 bytes) |
| Ciphertext type, ciphertext length | Two unsigned 32-bit big-endian integers |
| Ciphertext | Exact bytes |

Expected digest: `9c2252f36ab1d07773dc5a6ca84504ef8af4a6e66e277022391ef84e49e85190`.
The wire-file hash and the receipt digest have different inputs and purposes.
UUID and size validation precedes hashing. V1 rejects unknown envelope/device
fields on Go ingress. Both languages reject unsupported versions and enum values.

Do not substitute a hash of serialized protobuf, even deterministic protobuf:
[protobuf serialization is not canonical](https://protobuf.dev/programming-guides/serialization-not-canonical/).
Tests in both languages move the version field to another valid wire position
and require the same digest; changing routing or ciphertext changes the digest.
This hash supports exact-message receipt comparison; it is not a MAC, signature,
authentication, or proof of durable storage by itself.

Tests also check decrypted-payload context binding and structural request limits.
They do not exercise an authenticated v1 RPC or encrypt this opaque fixture.
Prost discards unknown fields; protocol-critical extensions require a new version,
rather than relying on a receiver retaining an unknown field.

## Public prekey material (legacy wire fixture)

`prekey-upload-v1.bin` is a 3533-byte `messenger.PreKeyBundleUpload`, generated
with the pinned libsignal v0.105.1 (`1738ead8a12ffe539f6309958123dadb16dc3b95`)
and the shared `proto/messenger.proto`.

It contains **only synthetic public keys and public signatures**. Private keys
were generated in memory and were never serialized or written to disk. The
account UUID is a fixed test identifier. This is not a working account or a
production key bundle. `v1` versions this fixture, not a negotiated E2EE protocol.

| Field | Value |
| --- | --- |
| User UUID | `11111111-1111-4111-8111-111111111111` |
| Device / registration ID | `1` / `16383` |
| Signed key ID | `4294967293` |
| EC one-time key ID | `2147483648` |
| Kyber one-time / last-resort ID | `4294967294` / `4294967295` |
| Signed key timestamp, Unix milliseconds | `1700000000123` |
| EC/identity public bytes / signature bytes | `33` / `64` |
| Kyber1024 public bytes, including type tag | `1569` |

SHA256: `7bdf44ee818e80d309da32edb984d61bee80d9470a826d9155a3ea9a1a979580`.

Rust tests decode/re-encode the fixture using prost, parse its keys using
libsignal, verify the signed EC and both Kyber signatures, and reject changed
public bytes/signatures. Go tests decode/re-encode it and roundtrip its public
data and full-width key IDs through the real PostgreSQL repositories. Go does
not implement Signal signature verification. This fixture does not prove an
authenticated gRPC handshake or encrypted message exchange.

To generate a **new** candidate file without overwriting the reviewed fixture:

```sh
cargo run -p rust-processor --locked --no-default-features --features e2ee \
  --example generate_prekey_fixture -- /tmp/prekey-upload-candidate.bin
```

Generation is random: new public bytes/signatures and their hash will differ.
The generator refuses an existing output path. Replacing the golden file is an
explicit review step; update this description/hash and run both Go and Rust
tests. Normal tests never regenerate it.
