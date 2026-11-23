import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const output = path.join(root, 'web/vendor');
const project = JSON.parse(await readFile(path.join(root, 'package.json'), 'utf8'));
const packages = {};

async function dependency(name) {
  const directory = path.join(root, 'node_modules', name);
  const metadata = JSON.parse(await readFile(path.join(directory, 'package.json'), 'utf8'));
  // A range or a different local install must not silently replace the pinned SDK.
  if (project.dependencies?.[name] !== metadata.version) {
    throw new Error(`Install the exact locked dependency with npm ci: ${name}`);
  }
  packages[name] = { version: metadata.version, license: metadata.license };
  return directory;
}

const sdk = await dependency('livekit-client');
const font = await dependency('@fontsource-variable/plus-jakarta-sans');
const sdkBytes = await readFile(path.join(sdk, 'dist/livekit-client.umd.js'));
const fontCSS = (await readFile(path.join(font, 'index.css'), 'utf8'))
  .replaceAll('Plus Jakarta Sans Variable', 'Plus Jakarta Sans');
const fontFiles = [...fontCSS.matchAll(/url\(\.\/files\/([a-z0-9-]+\.woff2)\)/g)]
  .map(match => match[1]);
if (!fontFiles.length) throw new Error('The pinned font package contains no supported WOFF2 sources');

// This directory is generated build output, never a source-assets directory.
await rm(output, { recursive: true, force: true });
await mkdir(path.join(output, 'fonts/files'), { recursive: true });
await mkdir(path.join(output, 'licenses'), { recursive: true });
const files = [];
async function emit(relative, bytes) {
  await writeFile(path.join(output, relative), bytes);
  files.push({ path: relative, bytes: Buffer.byteLength(bytes), sha256: createHash('sha256').update(bytes).digest('hex') });
}
await emit('livekit-client.umd.js', sdkBytes);
await emit('livekit-client.umd.js.map', await readFile(path.join(sdk, 'dist/livekit-client.umd.js.map')));
await emit('fonts/plus-jakarta-sans.css', fontCSS);
for (const name of new Set(fontFiles)) {
  await emit(`fonts/files/${name}`, await readFile(path.join(font, 'files', name)));
}
await emit('licenses/livekit-client.txt', await readFile(path.join(sdk, 'LICENSE')));
await emit('licenses/plus-jakarta-sans.txt', await readFile(path.join(font, 'LICENSE')));
await writeFile(path.join(output, 'manifest.json'), JSON.stringify({ packages, files }, null, 2) + '\n');
console.log(`Built local browser assets: LiveKit ${packages['livekit-client'].version}, font ${packages['@fontsource-variable/plus-jakarta-sans'].version}`);
