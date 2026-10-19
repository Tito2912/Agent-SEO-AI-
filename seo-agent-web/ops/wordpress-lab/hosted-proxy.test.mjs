import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { configuration, gateway } from './hosted-proxy.mjs';

const origin = 'https://wordpress.noyaru.test';
const key = 'a'.repeat(48);
// Render generateValue produces 32 random bytes encoded as standard base64.
// This synthetic value deliberately contains +, / and the trailing padding.
const renderKey = Buffer.alloc(32, 251).toString('base64');
test('configuration requires a dedicated HTTPS origin, secret and bounded lifetime', () => {
  const env = { RENDER_EXTERNAL_URL: origin, NOYARU_WP_DEMO_KEY: key };
  assert.deepEqual(configuration(env), { origin, key, minutes: 120, port: 10000 });
  for (const value of ['http://demo.test', 'https://demo.test/path', 'https://a:b@demo.test', 'https://demo.test:444', 'https://demo.test/?key=a']) {
    assert.throws(() => configuration({ ...env, NOYARU_WP_DEMO_URL: value }));
  }
  for (const value of ['', 'weak', 'x'.repeat(257), 'a b'.repeat(20),
    key + '\n', '"' + key + '"', 'a'.repeat(32) + '=a', 'a'.repeat(32) + '===']) {
    assert.throws(() => configuration({ ...env, NOYARU_WP_DEMO_KEY: value }));
  }
  for (const value of ['0', '1441', 'NaN', '1.5']) {
    assert.throws(() => configuration({ ...env, NOYARU_WP_DEMO_MINUTES: value }));
  }
  assert.throws(() => gateway({ origin, key, upstream: 'https://customer.test', manifest: {} }));
});

test('Render base64 key starts the fixture and authenticates the exact bearer value', async t => {
  assert.match(renderKey, /\+/);
  assert.match(renderKey, /\//);
  assert.ok(renderKey.endsWith('='));
  const config = configuration({ RENDER_EXTERNAL_URL: origin, NOYARU_WP_DEMO_KEY: renderKey });
  assert.equal(config.key, renderKey);
  const server = gateway({ ...config, upstream: 'http://127.0.0.1:1',
    manifest: { fixture: 'protected' }, expiresAt: Date.now() + 60000 });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(() => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); }));
  const url = `http://127.0.0.1:${server.address().port}/_noyaru-demo/credentials`;
  for (const token of ['', 'A' + renderKey.slice(1), renderKey.slice(0, -1), renderKey + '=', '"' + renderKey + '"']) {
    const response = await fetch(url, { headers: { authorization: 'Bearer ' + token } });
    assert.equal(response.status, 401);
    assert.ok(!(await response.text()).includes('protected'));
  }
  const response = await fetch(url, { headers: { authorization: 'Bearer ' + renderKey } });
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('cache-control'), 'no-store');
  assert.equal((await response.json()).fixture, 'protected');
});

test('gateway protects secrets, restricts routes and expires without a health restart', async t => {
  const calls = [];
  const upstream = createServer((req, res) => {
    calls.push({ url: req.url, headers: req.headers, method: req.method });
    res.writeHead(200, { 'Content-Type': 'application/json', 'Set-Cookie': 'admin=secret',
      'Access-Control-Allow-Origin': '*', 'Cache-Control': 'public' });
    req.resume();
    res.end('{"ok":true}');
  });
  upstream.listen(0, '127.0.0.1');
  await once(upstream, 'listening');
  let time = 1000;
  const server = gateway({ origin, key, upstream: `http://127.0.0.1:${upstream.address().port}`,
    manifest: { accounts: { editor: { password: 'fixture-secret' } } }, expiresAt: 2000, now: () => time });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => {
    await Promise.all([server, upstream].map(s => new Promise(resolve => {
      s.close(resolve); s.closeAllConnections();
    })));
  });
  const base = `http://127.0.0.1:${server.address().port}`;
  const fetchPath = (path, options) => fetch(base + path, { redirect: 'manual', ...options });
  for (const authorization of ['', 'Bearer wrong', 'Bearer ' + 'b'.repeat(48), 'Basic ' + key]) {
    const res = await fetchPath('/_noyaru-demo/credentials', { headers: { authorization } });
    assert.equal(res.status, 401);
    assert.ok(!(await res.text()).includes('fixture-secret'));
  }
  let res = await fetchPath('/_noyaru-demo/credentials', { headers: { authorization: 'Bearer ' + key } });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('cache-control'), 'no-store');
  assert.equal(res.headers.get('access-control-allow-origin'), null);
  assert.equal((await res.json()).accounts.editor.password, 'fixture-secret');
  for (const path of ['/wp-login.php', '/wp-admin/', '/xmlrpc.php', '/noyaru-demo.json',
    '/_noyaru-demo/credentials?key=' + key, '/?rest_route=/wp/v2/users',
    '/wp-json/wp/v2/users', '/wp-content/plugins/arbitrary.php', '/%77p-login.php']) {
    assert.equal((await fetchPath(path)).status, 404, path);
  }
  const write = '/wp-json/wp-abilities/v1/abilities/yoast-seo/update-post-seo-data/run';
  assert.equal((await fetchPath(write)).status, 404);
  assert.equal((await fetchPath('/', { method: 'POST' })).status, 404);
  assert.equal(calls.length, 0);
  res = await fetchPath('/', { headers: { authorization: 'Basic example', cookie: 'admin=true',
    'X-Forwarded-Host': 'attacker.test', 'X-Forwarded-Proto': 'http' } });
  assert.equal(res.status, 200);
  assert.equal(res.headers.get('set-cookie'), null);
  assert.equal(res.headers.get('cache-control'), 'no-store');
  assert.equal(calls[0].headers.host, 'wordpress.noyaru.test');
  assert.equal(calls[0].headers['x-forwarded-proto'], 'https');
  assert.equal(calls[0].headers.authorization, undefined);
  assert.equal(calls[0].headers.cookie, undefined);
  res = await fetchPath(write, { method: 'POST', body: '{}', headers: {
    authorization: 'Basic fixture', 'content-type': 'application/json' } });
  assert.equal(res.status, 200);
  assert.equal(calls[1].headers.authorization, 'Basic fixture');
  assert.equal((await fetchPath(write, { method: 'POST', body: 'x'.repeat(17000) })).status, 413);
  assert.equal(calls.length, 2);
  time = 2000;
  assert.equal((await fetchPath('/')).status, 410);
  assert.equal((await fetchPath(write, { method: 'POST', body: '{}' })).status, 410);
  assert.equal((await fetchPath('/_noyaru-demo/credentials', { headers: { authorization: 'Bearer ' + key } })).status, 410);
  res = await fetchPath('/_noyaru-demo/health');
  assert.equal(res.status, 200);
  assert.deepEqual(await res.json(), { ready: true, expired: true });
  assert.equal(calls.length, 2);
});
