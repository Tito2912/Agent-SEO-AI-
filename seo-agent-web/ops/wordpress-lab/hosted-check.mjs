// Native HTTP smoke test. It does NOT claim a public TLS or noyaru.com acceptance test.
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const probe = createServer();
probe.listen(0, '127.0.0.1');
await once(probe, 'listening');
const port = probe.address().port;
await new Promise(resolve => probe.close(resolve));
const key = randomBytes(32).toString('hex');
const child = spawn(process.execPath, [fileURLToPath(new URL('./hosted.mjs', import.meta.url))], {
  env: { ...process.env, PORT: String(port), NOYARU_WP_DEMO_URL: 'https://wordpress.noyaru.test',
    NOYARU_WP_DEMO_KEY: key }, stdio: ['ignore', 'ignore', 'pipe'],
});
// Consume output without printing errors that might contain sensitive third-party diagnostics.
child.stderr.resume();
const exited = new Promise(resolve => child.once('exit', resolve));
let exitCode;
exited.then(code => { exitCode = code ?? -1; });
const base = `http://127.0.0.1:${port}`;
const checks = [];
try {
  let ready = false;
  for (let attempt = 0; attempt < 180; attempt++) {
    if (exitCode !== undefined) throw new Error('Hosted fixture startup failed');
    try { ready = (await fetch(base + '/_noyaru-demo/health')).status === 200; } catch { /* booting */ }
    if (ready) break;
    await setTimeout(1000);
  }
  assert.ok(ready, 'Hosted fixture startup timed out');
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
    return fetch(base + '/wp-json/' + path, { method, headers,
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
  checks.push('Both metadata values written by native Yoast and present in public HTML');
  const restored = await api(editor, writePath, 'POST', { input: { post_id: original.post_id, permalink: manifest.url,
    seo_title: original.seo_title, meta_description: original.meta_description } });
  assert.equal(restored.status, 200);
  assert.deepEqual(await read(), original);
  const restoredHtml = await (await fetch(pageUrl)).text();
  assert.match(restoredHtml, /<title>Démo<\/title>/);
  assert.doesNotMatch(restoredHtml, /<meta name="description"/);
  checks.push('Raw original metadata restored, including a missing description');
  assert.equal((await fetch(base + '/wp-login.php')).status, 404);
  assert.equal((await fetch(base + '/noyaru-demo.json')).status, 404);
  console.log(JSON.stringify({ transport: 'loopback HTTP; public DNS/TLS and Noyaru online remain untested',
    wordpress: manifest.wp, yoast: manifest.yoast, checks }, null, 2));
} finally {
  child.kill('SIGTERM');
  const timer = globalThis.setTimeout(() => child.kill('SIGKILL'), 10000);
  await exited;
  clearTimeout(timer);
}
