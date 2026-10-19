import { runCLI } from '@wp-playground/cli';
import { Server } from 'node:http';
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { configuration, gateway } from './hosted-proxy.mjs';

const config = configuration(process.env);
const listen = Server.prototype.listen;
let cli;
let server;
let stopping;
async function stop() {
  if (stopping) return stopping;
  stopping = (async () => {
    if (server) await new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
    if (cli) await cli[Symbol.asyncDispose]();
    Server.prototype.listen = listen;
  })();
  return stopping;
}
for (const signal of ['SIGINT', 'SIGTERM']) {
  process.once(signal, async () => { await stop(); process.exit(0); });
}
try {
  // Only the native WordPress server gets this restriction; the public gateway binds below.
  Server.prototype.listen = function (port, ...rest) {
    return typeof port === 'number'
      ? listen.call(this, { port, host: '127.0.0.1' }, ...rest)
      : listen.call(this, port, ...rest);
  };
  cli = await runCLI({
    command: 'server', port: 0, workers: 1, wp: '6.9', php: '8.3', login: false,
    internalCookieStore: false, 'site-url': config.origin, verbosity: 'quiet',
    blueprint: {
      constants: { WP_ENVIRONMENT_TYPE: 'production', WP_DEBUG: false,
        DISABLE_WP_CRON: true, DISALLOW_FILE_MODS: true },
      siteOptions: { blogname: 'Noyaru — démonstration temporaire', blog_public: '1' },
      features: { networking: false },
      steps: [
        { step: 'installPlugin', pluginData: { resource: 'url', url: 'https://downloads.wordpress.org/plugin/wordpress-seo.28.7.zip' }, options: { activate: true } },
        { step: 'writeFile', path: '/wordpress/wp-content/mu-plugins/noyaru-demo.php', data: '<?php add_filter("pre_wp_mail", "__return_false");' },
        { step: 'runPHP', code: await readFile(new URL('./hosted-seed.php', import.meta.url), 'utf8') },
      ],
    },
  });
  Server.prototype.listen = listen;
  if (cli.server.address()?.address !== '127.0.0.1') throw new Error('Invalid fixture binding');
  await fetch(cli.serverUrl + '/', { redirect: 'manual' }); // consume Playground's initial redirect
  const manifest = { ...JSON.parse(await cli.playground.readFileAsText('/noyaru-demo.json')), boot_id: randomUUID() };
  server = gateway({ ...config, upstream: cli.serverUrl, manifest,
    expiresAt: Date.now() + config.minutes * 60000 });
  server.requestTimeout = 35000;
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen({ port: config.port, host: '0.0.0.0' }, resolve);
  });
  console.log('Disposable WordPress ready. Credentials are available only through the protected endpoint.');
} catch {
  console.error('WordPress demonstration startup failed. No credentials were logged.');
  await stop();
  process.exitCode = 1;
}
