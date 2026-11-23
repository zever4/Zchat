// Run on Windows after npm ci. Include build-time dependencies conservatively;
// the inventory is a superset of the code embedded in the installer.
import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, readdirSync } from 'node:fs';
import { dirname, join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const experimental = process.argv.includes('--experimental-e2ee');
const features = experimental ? ['--features', 'zchat-desktop/experimental-e2ee'] : [];
const supplementalDirectory = join(root, 'licenses/upstream/e2ee');
const e2eeSupplements = experimental
  ? JSON.parse(readFileSync(join(supplementalDirectory, 'sources.json'), 'utf8')) : {};
const metadataFile = process.argv.indexOf('--metadata');
const metadata = metadataFile >= 0
  ? JSON.parse(readFileSync(process.argv[metadataFile + 1], 'utf8'))
  : (() => {
    const result = spawnSync('cargo', ['metadata', '--locked', '--format-version', '1', '--filter-platform', 'x86_64-pc-windows-msvc', ...features], { cwd: root, encoding: 'utf8', maxBuffer: 16 * 1024 * 1024 });
    if (result.error || result.status !== 0) throw result.error || Error(result.stderr);
    return JSON.parse(result.stdout);
  })();
const packages = new Map(metadata.packages.map(p => [p.id, p]));
const nodes = new Map(metadata.resolve.nodes.map(n => [n.id, n]));
const entry = metadata.packages.find(p => p.name === 'zchat-desktop');
if (!entry) throw Error('Missing desktop package');
if (nodes.get(entry.id).features.includes('experimental-e2ee') !== experimental) {
  throw Error('Cargo metadata does not match the requested desktop variant');
}
const selected = new Set();
function visit(id) {
  if (selected.has(id)) return;
  selected.add(id);
  for (const dep of nodes.get(id).deps) {
    if (dep.dep_kinds.some(k => k.kind !== 'dev')) visit(dep.pkg);
  }
}
visit(entry.id);
function licenseFiles(directory, prefix = '', recursive = true) {
  const result = [];
  for (const e of readdirSync(directory, { withFileTypes: true })) {
    if (e.isSymbolicLink() || ['.git', 'target', 'node_modules'].includes(e.name)) continue;
    const name = join(prefix, e.name);
    if (e.isDirectory() && recursive) result.push(...licenseFiles(join(directory, e.name), name));
    else if (/^(licen[cs]e|copying|notice|copyright|patents)([._-]|$)/i.test(e.name)) result.push(name);
  }
  return result.sort();
}
const inventory = [], texts = [], missing = [];
function collect(name, version, license, source, directory, files) {
  if (!license || !files.length) { missing.push({ name, version, license, source }); return; }
  inventory.push({ name, version, license, source });
  texts.push(`\n${'='.repeat(80)}\n${name} ${version}\nLicense: ${license}\nSource: ${source}\n`);
  for (const file of files) texts.push(`\n--- ${file.replaceAll('\\', '/')} ---\n${readFileSync(join(directory, file), 'utf8')}\n`);
}
for (const p of [...selected].map(id => packages.get(id)).sort((a, b) => a.name.localeCompare(b.name) || a.version.localeCompare(b.version))) {
  if (p.source === null) continue; // Original project code has its own LICENSE.
  let directory = dirname(p.manifest_path);
  let source = `https://crates.io/crates/${p.name}/${p.version}`;
  let files = licenseFiles(directory);
  if (p.source.startsWith('git+')) {
    const [repository, revision] = p.source.slice(4).split('#');
    if (!revision) throw Error(`Missing pinned source revision for ${p.name}`);
    source = `${repository.split('?')[0].replace(/\.git$/, '')}/tree/${revision}`;
    // Git crates can inherit their license from the repository root.
    if (!files.length && !p.license_file) {
      const checkout = spawnSync('git', ['rev-parse', '--show-toplevel'], { cwd: directory, encoding: 'utf8' });
      if (checkout.error || checkout.status !== 0) throw checkout.error || Error(checkout.stderr);
      directory = checkout.stdout.trim();
      files = licenseFiles(directory, '', false);
    }
  }
  if (!files.length) {
    const e2eeSupplement = e2eeSupplements[`${p.name}-${p.version}`];
    if (e2eeSupplement) {
      collect(p.name, p.version, p.license, source, supplementalDirectory, [e2eeSupplement.file]);
      texts.push(`Supplemental license provenance: ${e2eeSupplement.source}\n`);
      continue;
    }
    // A few crates omit a workspace license from the published crate. The
    // pinned upstream copy and its provenance are maintained in this folder.
    const supplemental = join(root, 'licenses/upstream', `${p.name}-${p.version}.txt`);
    try {
      readFileSync(supplemental);
      collect(p.name, p.version, p.license, source, dirname(supplemental), [`${p.name}-${p.version}.txt`]);
      continue;
    } catch (error) { if (error.code !== 'ENOENT') throw error; }
  }
  if (p.license_file && !files.includes(p.license_file)) files.push(p.license_file);
  collect(p.name, p.version, p.license, source, directory, files);
  // Published crates can also contain an incomplete license file (mozjpeg-sys
  // omits its vendored IJG README). Include the pinned supplement in that case.
  try {
    texts.push(`\n--- Supplemental upstream notices ---\n${readFileSync(join(root, 'licenses/upstream', `${p.name}-${p.version}.txt`), 'utf8')}\n`);
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
}
// Include all production npm dependencies conservatively, including browser SDK
// transitive packages, rather than assuming its UMD bundle has no dependencies.
const lock = JSON.parse(readFileSync(join(root, 'package-lock.json'), 'utf8'));
for (const [location, p] of Object.entries(lock.packages).sort(([a], [b]) => a.localeCompare(b))) {
  if (!location || p.dev || p.optional) continue;
  const name = location.replace(/^node_modules\//, '');
  const directory = join(root, location);
  const installed = JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8'));
  if (installed.version !== p.version) throw Error(`Run npm ci: ${name}`);
  const files = licenseFiles(directory);
  if (files.length) {
    collect(`npm:${name}`, p.version, installed.license || p.license, `https://www.npmjs.com/package/${name}/v/${p.version}`, directory, files);
  } else {
    const supplemental = `${name.replaceAll('/', '_')}-${p.version}.txt`;
    collect(`npm:${name}`, p.version, installed.license || p.license, `https://www.npmjs.com/package/${name}/v/${p.version}`, join(root, 'licenses/upstream'), [supplemental]);
  }
}
const output = join(root, experimental ? 'licenses/native-e2ee' : 'licenses/native');
const variant = experimental ? 'Experimental E2EE is included.' : 'Experimental E2EE is excluded.';
if (missing.length) throw Error(`Missing license texts: ${JSON.stringify(missing)}`);
mkdirSync(output, { recursive: true });
writeFileSync(join(output, 'inventory.json'), JSON.stringify({ target: 'x86_64-pc-windows-msvc', scope: 'Conservative desktop dependency inventory, including build-time crates and production npm packages. ' + variant, packages: inventory }, null, 2) + '\n');
writeFileSync(join(output, 'THIRD-PARTY-NOTICES.txt'), ('Zchat Desktop: third-party notices\n\nOriginal Zchat code: AGPL-3.0-only; see the accompanying LICENSE.txt.\nThis file conservatively includes build-time and production dependencies.\nSome dependency notices describe code not embedded in this Windows binary.\nThis software is based in part on the work of the Independent JPEG Group.\nCorresponding project source: https://github.com/zever4/Zchat (release tag).\n' + texts.join('')).replaceAll('\r\n', '\n').replace(/[ \t]+$/gm, '').trimEnd() + '\n');
console.log(`Collected ${inventory.length} dependency notices in ${relative(root, output)}`);
