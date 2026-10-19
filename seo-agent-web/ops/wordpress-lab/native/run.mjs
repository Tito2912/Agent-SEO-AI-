// Native PHP and SQLite keep the disposable service below the Free memory budget.
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { setTimeout } from 'node:timers/promises';
import { configuration, gateway } from '../hosted-proxy.mjs';

process.umask(0o077);
const config = configuration(process.env);
const state = await mkdtemp(join(tmpdir(), 'noyaru-wp-native-'));
// PHP never receives the operator's bearer key.
const phpEnv = { PATH: process.env.PATH, NOYARU_WP_DEMO_URL: config.origin,
  NOYARU_WP_DEMO_STATE: state, NOYARU_WP_DEMO_MANIFEST: join(state, 'demo.json') };
let phase = 'installing_wordpress';
let forward;
let php;
let stopping;
const server = createServer((req, res) => {
  if (forward) return forward(req, res);
  res.writeHead(503, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Retry-After': '5' });
  res.end(JSON.stringify({ ready: false, phase }));
});
server.requestTimeout = 35000;
async function stop() {
  if (stopping) return stopping;
  stopping = (async () => {
    await new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
    if (php && php.exitCode === null) { php.kill('SIGTERM'); await once(php, 'exit'); }
    await rm(state, { recursive: true, force: true });
  })();
  return stopping;
}
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, async () => { await stop(); process.exit(0); });
}
async function runPhp(script) {
  php = spawn('php', ['-d', 'memory_limit=128M', '-d', 'zend.exception_ignore_args=1', script],
    { env: phpEnv, stdio: 'ignore' });
  const timer = globalThis.setTimeout(() => php?.kill('SIGTERM'), 120000);
  try {
    const [code] = await once(php, 'exit');
    if (code !== 0) throw new Error('Native WordPress initialization failed');
  } finally { clearTimeout(timer); }
}
try {
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen({ port: config.port, host: '0.0.0.0' }, resolve);
  });
  console.log('HTTP port open; health remains 503 while installing native WordPress.');
  await runPhp('/opt/noyaru/native/install.php');
  phase = 'preparing_yoast';
  console.log('WordPress installed; preparing the disposable Yoast fixture.');
  await runPhp('/opt/noyaru/hosted-seed.php');
  const manifest = { ...JSON.parse(await readFile(phpEnv.NOYARU_WP_DEMO_MANIFEST, 'utf8')), boot_id: randomUUID() };
  if (manifest.site_url !== config.origin || manifest.url !== config.origin + '/atelier-demo/') {
    throw new Error('Unexpected fixture origin');
  }
  phase = 'starting_php';
  php = spawn('php', ['-d', 'memory_limit=128M', '-d', 'zend.exception_ignore_args=1',
    '-S', '127.0.0.1:9001', '-t', '/wordpress', '/opt/noyaru/native/router.php'], { env: phpEnv, stdio: 'ignore' });
  php.once('error', async () => { console.error('Native PHP worker failed.'); await stop(); process.exit(1); });
  php.once('exit', async () => {
    if (!stopping) { console.error('Native PHP worker stopped.'); await stop(); process.exit(1); }
  });
  let ready = false;
  for (let attempt = 0; attempt < 120; attempt++) {
    try {
      const response = await fetch('http://127.0.0.1:9001/atelier-demo/', { redirect: 'manual', signal: AbortSignal.timeout(10000) });
      ready = response.status === 200 && (await response.text()).includes('<title>Démo</title>');
    } catch { /* PHP is still starting */ }
    if (ready) break;
    await setTimeout(500);
  }
  if (!ready) throw new Error('Native WordPress readiness failed');
  forward = gateway({ ...config, upstream: 'http://127.0.0.1:9001', manifest,
    expiresAt: Date.now() + config.minutes * 60000 }).listeners('request')[0];
  console.log('Disposable native WordPress ready; health is 200. No credentials were logged.');
} catch {
  console.error(`WordPress demonstration failed during ${phase}. No credentials were logged.`);
  await stop();
  process.exitCode = 1;
}
