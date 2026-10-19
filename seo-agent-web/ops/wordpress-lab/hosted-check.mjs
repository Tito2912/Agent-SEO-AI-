// Native HTTP smoke test. It does NOT claim a public TLS or noyaru.com acceptance test.
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { startContainer } from './container-test.mjs';

const probe = createServer();
probe.listen(0, '127.0.0.1');
await once(probe, 'listening');
const port = probe.address().port;
await new Promise(resolve => probe.close(resolve));
const key = randomBytes(32).toString('base64'); // same format as Render generateValue
const container = process.env.NOYARU_DEMO_CONTAINER_IMAGE
  ? await startContainer(process.env.NOYARU_DEMO_CONTAINER_IMAGE, key) : null;
const child = container ? null : spawn(process.execPath, [fileURLToPath(new URL('./hosted.mjs', import.meta.url))], {
  env: { ...process.env, PORT: String(port), NOYARU_WP_DEMO_URL: 'https://wordpress.noyaru.test',
    NOYARU_WP_DEMO_KEY: key }, stdio: ['ignore', 'ignore', 'pipe'],
});
// Consume output without printing errors that might contain sensitive third-party diagnostics.
child?.stderr.resume();
const exited = child ? new Promise(resolve => child.once('exit', resolve)) : null;
let exitCode;
exited?.then(code => { exitCode = code ?? -1; });
let base = container?.base || `http://127.0.0.1:${port}`;
const checks = [];
let sawInitialization = false;
async function waitReady() {
  let ready = false;
  for (let attempt = 0; attempt < 180; attempt++) {
    if (exitCode !== undefined) throw new Error('Hosted fixture startup failed');
    try {
      const response = await fetch(base + '/_noyaru-demo/health', { signal: AbortSignal.timeout(10000) });
      ready = response.status === 200;
      if (container && response.status === 503 && !sawInitialization) {
        assert.equal((await response.json()).ready, false);
        const pending = await fetch(base + '/_noyaru-demo/credentials', { headers: { authorization: 'Bearer ' + key } });
        assert.equal(pending.status, 503);
        assert.equal((await pending.json()).accounts, undefined);
        sawInitialization = true;
      }
    } catch (error) {
      if (error instanceof assert.AssertionError) throw error;
      // The container may not have opened its port yet.
    }
    if (ready) break;
    await setTimeout(1000);
  }
  assert.ok(ready, 'Hosted fixture startup timed out');
}
try {
  await waitReady();
  if (container) assert.ok(sawInitialization, 'HTTP must be open with health 503 before WordPress becomes ready');
  assert.equal((await fetch(base + '/_noyaru-demo/credentials')).status, 401);
  const response = await fetch(base + '/_noyaru-demo/credentials', { headers: { authorization: 'Bearer ' + key } });
  assert.equal(response.status, 200);
  const manifest = await response.json();
  assert.equal(manifest.site_url, 'https://wordpress.noyaru.test');
  assert.equal(manifest.url, manifest.site_url + '/atelier-demo/');
  assert.ok(manifest.boot_id && manifest.accounts.editor.password);
  checks.push('Manifest protected; canonical HTTPS URL and disposable credentials');
  const api = async (account, path, method = 'GET', payload) => {
    const headers = { authorization: 'Basic ' + Buffer.from(account.username + ':' + account.password).toString('base64') };
    if (payload) headers['content-type'] = 'application/json';
    return fetch(base + '/wp-json/' + path, { method, headers, signal: AbortSignal.timeout(12000),
      body: payload ? JSON.stringify(payload) : undefined, redirect: 'manual' });
  };
  const editor = manifest.accounts.editor;
  assert.equal((await fetch(base + '/wp-json/wp/v2/users/me?context=edit')).status, 401);
  assert.equal((await api({ username: 'admin', password: 'password' }, 'wp/v2/users/me?context=edit')).status, 401);
  const me = await api(editor, 'wp/v2/users/me?context=edit');
  assert.equal(me.status, 200);
  assert.equal((await me.json()).capabilities.wpseo_edit_advanced_metadata, true);
  for (const name of ['get-post-seo-data', 'update-post-seo-data']) {
    const schema = await api(editor, 'wp-abilities/v1/abilities/yoast-seo/' + name);
    assert.equal(schema.status, 200);
    assert.equal((await schema.json()).name, 'yoast-seo/' + name);
  }
  checks.push('Native WordPress authentication and both Yoast abilities through gateway');
  const read = async () => {
    const res = await api(editor, 'wp-abilities/v1/abilities/yoast-seo/get-post-seo-data/run?' +
      new URLSearchParams({ 'input[permalink]': manifest.url }));
    if (res.status !== 200) {
      const error = await res.json();
      throw new Error(`Yoast read failed: ${res.status} ${error.code}: ${error.message}`);
    }
    const pages = await res.json();
    assert.equal(pages.length, 1);
    return pages[0];
  };
  const original = await read();
  const payload = { post_id: original.post_id, permalink: manifest.url, seo_title: 'Réparation de vélos à Lyon — Atelier Noyaru Démo',
    meta_description: 'Découvrez notre atelier fictif de réparation de vélos à Lyon, créé uniquement pour tester les corrections SEO de Noyaru.' };
  const writePath = 'wp-abilities/v1/abilities/yoast-seo/update-post-seo-data/run';
  const refused = await api(manifest.accounts.subscriber, writePath, 'POST', { input: payload });
  assert.ok([401, 403].includes(refused.status));
  assert.deepEqual(await read(), original);
  checks.push('Insufficient permissions refused without a write');
  const pageUrl = base + new URL(manifest.url).pathname;
  const home = await (await fetch(base + '/')).text();
  assert.ok(home.includes('/atelier-demo/'), 'The public crawler must be able to discover the fixture page');
  const before = await (await fetch(pageUrl)).text();
  assert.match(before, /<title>Démo<\/title>/);
  assert.doesNotMatch(before, /<meta name="description"/);
  const applied = await api(editor, writePath, 'POST', { input: payload });
  assert.equal(applied.status, 200);
  const page = await read();
  assert.equal(page.seo_title, payload.seo_title);
  assert.equal(page.meta_description, payload.meta_description);
  const after = await (await fetch(pageUrl)).text();
  assert.ok(after.includes('<title>' + payload.seo_title + '</title>'));
  assert.ok(after.includes(payload.meta_description));
  const headings = html => [...html.matchAll(/<h1\b[^>]*>([\s\S]*?)<\/h1>/gi)].map(match => match[1]);
  assert.deepEqual(headings(after), headings(before));
  checks.push('Both metadata values written by native Yoast and present in public HTML');
  const restored = await api(editor, writePath, 'POST', { input: { post_id: original.post_id, permalink: manifest.url,
    seo_title: original.seo_title, meta_description: original.meta_description } });
  assert.equal(restored.status, 200);
  assert.deepEqual(await read(), original);
  const restoredHtml = await (await fetch(pageUrl)).text();
  assert.match(restoredHtml, /<title>Démo<\/title>/);
  assert.doesNotMatch(restoredHtml, /<meta name="description"/);
  assert.deepEqual(headings(restoredHtml), headings(before));
  checks.push('Raw original metadata restored, including a missing description');
  assert.equal((await fetch(base + '/wp-login.php')).status, 404);
  assert.equal((await fetch(base + '/noyaru-demo.json')).status, 404);
  if (container) {
    await container.metrics();
    base = await container.restart();
    await waitReady();
    const next = await (await fetch(base + '/_noyaru-demo/credentials', { headers: { authorization: 'Bearer ' + key } })).json();
    assert.notEqual(next.boot_id, manifest.boot_id);
    assert.notEqual(next.accounts.editor.password, editor.password);
    assert.equal((await api(editor, 'wp/v2/users/me?context=edit')).status, 401);
    await container.metrics();
    checks.push('Container restart creates new credentials; previous credentials are refused');
  }
  console.log(JSON.stringify({ transport: 'loopback HTTP; public DNS/TLS and Noyaru online remain untested',
    wordpress: manifest.wp, yoast: manifest.yoast, checks }, null, 2));
} finally {
  if (container) await container.stop();
  else {
    child.kill('SIGTERM');
    const timer = globalThis.setTimeout(() => child.kill('SIGKILL'), 10000);
    await exited;
    clearTimeout(timer);
  }
}
