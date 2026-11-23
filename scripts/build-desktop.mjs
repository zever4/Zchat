import { cp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.join(root, 'dist-desktop');
await rm(output, { recursive: true, force: true });
await mkdir(output, { recursive: true });
// Deliberate allowlist: never package uploads, secrets, source trees or audit notes.
for (const name of ['login.html', 'login.css', 'login.js', 'chat.html', 'chat.css', 'chat.js', 'vendor']) {
  await cp(path.join(root, 'web', name), path.join(output, name), { recursive: true });
}
for (const name of ['settings.html', 'settings.js', 'offline.html', 'offline.js', 'tauri-api.js', 'shared.css', 'desktop-runtime.js', 'window-chrome.js', 'window-chrome.css']) {
  await cp(path.join(root, 'desktop/renderer', name), path.join(output, name), { recursive: true });
}
// Native window decoration belongs to the packaged shell, never browser pages.
const chrome = await readFile(path.join(root, 'desktop/renderer/window-chrome.html'), 'utf8');
for (const page of ['login', 'chat', 'offline']) {
  const file = path.join(output, `${page}.html`);
  const source = await readFile(file, 'utf8');
  if (!source.includes('<body>') || !source.includes('</body>')) throw Error(`Missing ${page} body for desktop shell`);
  await writeFile(file, source
    .replace('</head>', '<link rel="stylesheet" href="window-chrome.css?v=2">\n</head>')
    .replace('<body>', `<body class="desktop-shell desktop-${page}">\n${chrome}\n<div id="desktop-content">`)
    .replace('</body>', '</div>\n<script src="window-chrome.js?v=1"></script>\n</body>'));
}
for (const page of ['login', 'chat']) {
  const file = path.join(output, `${page}.html`);
  const source = await readFile(file, 'utf8');
  const pattern = new RegExp(`<script src="(${page}\\.js[^\"]*)"><\\/script>`);
  if (!pattern.test(source)) throw Error(`Missing shared ${page} entry script`);
  await writeFile(file, source.replace(pattern, (_, entry) => `<script src="desktop-runtime.js?v=18" data-entry="${entry}"></script>`));
}
console.log('Bundled shared chat/login, local vendor assets and desktop settings (no uploads).');
