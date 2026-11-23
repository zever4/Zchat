# Licensing

## Project license

Original Zchat code is offered under **GNU AGPL version 3 only** (`AGPL-3.0-only`).
Copyright © 2026 Zchat contributors. Existing author/copyright notices remain intact.
The complete, unmodified license is in [LICENSE](LICENSE).

A public repository without a license generally does not grant permission to reuse,
modify or distribute its code. An explicit license makes those permissions and
conditions clear for readers, contributors and downstream users.

AGPL permits use, modification, redistribution and commercial use. Distribution of
covered works requires the corresponding source and license notices; users of a
modified network version must also be offered its corresponding source under the
license's terms. It does not require publishing passwords, user messages or databases.

## Why AGPL here

The pinned [libsignal v0.105.1](https://github.com/signalapp/libsignal/blob/v0.105.1/Cargo.toml)
uses `AGPL-3.0-only` and is a Rust dependency of the native/E2EE work. Choosing the
same license for original Zchat code keeps one clear project policy and preserves
sharing of covered modifications. It is a choice for the whole project, not a claim
that a separate Go service automatically inherits AGPL merely by using an API.

MIT could license independently owned code, but would not remove libsignal's terms
from a combined native distribution. Dependencies retain their own licenses.
Before publishing this license, all contributors whose previously unlicensed code
is included should agree to the grant; authorship is not transferred by this file.

## Third-party notices

| Material | Location | License/source |
| --- | --- | --- |
| Google API annotations/HTTP definitions, Copyright 2025 Google LLC | `proto/google/api/` | Apache-2.0; headers retained, [license text](licenses/Apache-2.0.txt), [upstream](https://github.com/googleapis/googleapis/tree/master/google/api) |
| LiveKit browser SDK | Generated `web/vendor/` | Apache-2.0; its license is copied by the asset build |
| Plus Jakarta Sans font | Generated `web/vendor/fonts/` | OFL-1.1; its license is copied by the asset build |
| libsignal | Cargo Git dependency pinned in `Cargo.lock` | AGPL-3.0-only; upstream copyright/license retained |

Go/npm/Cargo dependencies and independently distributed container services also
retain their upstream terms. This table records directly vendored/browser/crypto
materials; the Windows distribution has the additional inventory below.

## Windows desktop distribution

The Windows installer includes the project `LICENSE.txt`, browser/font licenses,
and [native third-party notices](licenses/native/THIRD-PARTY-NOTICES.txt).
The [machine-readable inventory](licenses/native/inventory.json) records exact
versions, declared license expressions and source download links. It conservatively
includes build-time crates and production npm packages as well as runtime code.
The default installer does **not include** libsignal/E2EE: it enables only
`rust-processor`'s compression feature. The experimental E2EE installer
generates its own inventory and notices with `npm run licenses:desktop:e2ee`.
Run `npm run desktop:dist:win:e2ee -- --ci -- --locked` to generate and bundle
those notices with the experimental Windows binary. Its Git dependency records
link to the exact commits used by the build. Build each variant in a separate
target directory or move the resulting installers to distinct filenames.

Regenerate with `npm run licenses:desktop` on Windows after `npm ci` and dependency
changes. The script resolves the Windows desktop dependency graph from the locked
workspace. Crates omitting a workspace license use the upstream copies with pinned
provenance in `licenses/upstream/`. CI checks inventory drift. Dependencies remain
unmodified under their respective terms, including MPL-2.0, IJG and BSD notices.
This software is based in part on the work of the Independent JPEG Group.

The statically linked Microsoft WebView2 SDK loader has its own
[BSD license and notices](licenses/native/Microsoft-WebView2Loader.txt), separate
from the MIT Rust bindings. Its x64 library was matched byte for byte to the
official NuGet SDK version recorded in that file. Recheck the SDK version and
notices when updating `webview2-com-sys`; the Cargo/npm inventory does not describe
the loader separately. The WebView2 Runtime itself is installed and serviced by
Microsoft, rather than included in this installer.

Each release links the matching project source tag, including Cargo/npm lockfiles,
build instructions and the dependency source links in the inventory. The release
assets also include the source archive and the version configuration used for
packaging. Distributing a different build or hosted modification requires reviewing
its corresponding source and notices separately.
