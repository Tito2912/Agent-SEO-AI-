import { runCLI } from '@wp-playground/cli';
import { Server } from 'node:http';
import { spawn } from 'node:child_process';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = dirname(fileURLToPath(import.meta.url));
const work = await mkdtemp(join(tmpdir(), 'noyaru-wordpress-lab-'));
// Playground 3.1.58 has no bind-address argument. Restrict its HTTP server here.
// This changes only this test process; the production connector is untouched.
const listen = Server.prototype.listen;
Server.prototype.listen = function (port, ...rest) {
  return typeof port === 'number'
    ? listen.call(this, { port, host: '127.0.0.1' }, ...rest)
    : listen.call(this, port, ...rest);
};
let cli;
let child;
let cleaning;
async function cleanup() {
  if (cleaning) return cleaning;
  cleaning = (async () => {
    child?.kill();
    if (cli) await cli[Symbol.asyncDispose]();
    Server.prototype.listen = listen;
    await rm(work, { recursive: true, force: true });
  })();
  return cleaning;
}
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, async () => { await cleanup(); process.exit(1); });
}
try {
  cli = await runCLI({
    command: 'server', port: 0, workers: 1, wp: '6.9', php: '8.3',
    'site-url': 'https://wordpress.noyaru.test', verbosity: 'quiet',
    blueprint: {
      // Yoast exposes its abilities in production mode. This is still a local fixture.
      constants: { WP_ENVIRONMENT_TYPE: 'production', WP_DEBUG: false },
      siteOptions: { blogname: 'Noyaru — démonstration locale', blog_public: '1' },
      features: { networking: false },
      steps: [
        { step: 'installPlugin', pluginData: { resource: 'url', url: 'https://downloads.wordpress.org/plugin/wordpress-seo.28.7.zip' }, options: { activate: true } },
        { step: 'runPHP', code: await readFile(join(root, 'seed.php'), 'utf8') },
      ],
    },
  });
  if (cli.server.address()?.address !== '127.0.0.1') throw new Error('WordPress must stay on loopback');
  // The CLI deliberately redirects its first request once after boot, even without login.
  await fetch(cli.serverUrl + '/', { redirect: 'manual' });
  const data = JSON.parse(await cli.playground.readFileAsText('/noyaru-lab.json'));
  await writeFile(join(work, 'credentials.json'), JSON.stringify({ ...data, server_url: cli.serverUrl }), { mode: 0o600 });
  const python = process.env.NOYARU_LAB_PYTHON || 'python';
  child = spawn(python, [join(root, 'check.py')], {
    stdio: 'inherit', env: { ...process.env, NOYARU_LAB_ROOT: work },
  });
  const code = await new Promise((resolve, reject) => { child.once('error', reject); child.once('exit', resolve); });
  child = null;
  process.exitCode = code === 0 ? 0 : 1;
} finally {
  await cleanup();
}
