// Gateway for a disposable fixture, never for a customer WordPress installation.
import { createHash, timingSafeEqual } from 'node:crypto';
import { createServer, request } from 'node:http';

// Accept Render's standard base64 secrets as well as existing base64url keys.
// Use the same bounds and alphabet at startup and on the protected endpoint.
const validDemoKey = value => typeof value === 'string' && value.length >= 32 && value.length <= 256 &&
  /^[A-Za-z0-9_+/-]+={0,2}$/.test(value);

export function configuration(env) {
  const origin = new URL(env.NOYARU_WP_DEMO_URL || env.RENDER_EXTERNAL_URL || '');
  if (origin.protocol !== 'https:' || origin.username || origin.password || origin.port ||
      origin.pathname !== '/' || origin.search || origin.hash || !origin.hostname.includes('.')) {
    throw new Error('A public HTTPS origin without a path is required');
  }
  const key = env.NOYARU_WP_DEMO_KEY || '';
  if (!validDemoKey(key)) throw new Error('NOYARU_WP_DEMO_KEY must be a random base64 or base64url secret (32–256 characters)');
  const minutes = Number(env.NOYARU_WP_DEMO_MINUTES || '120');
  const port = Number(env.PORT || '10000');
  if (!Number.isInteger(minutes) || minutes < 10 || minutes > 1440 ||
      !Number.isInteger(port) || port < 1 || port > 65535) throw new Error('Invalid demo duration or port');
  return { origin: origin.origin, key, minutes, port };
}

const digest = value => createHash('sha256').update(value).digest();
const readAbility = '/wp-json/wp-abilities/v1/abilities/yoast-seo/get-post-seo-data';
const writeAbility = '/wp-json/wp-abilities/v1/abilities/yoast-seo/update-post-seo-data';
const posts = '/wp-json/wp/v2/posts';
const media = '/wp-json/wp/v2/media';
const postSlug = /^noyaru-demo-[a-z0-9]+(?:-[a-z0-9]+)*$/;
const webpName = '[A-Za-z0-9][A-Za-z0-9._-]{0,139}\\.webp';
const webpDisposition = new RegExp('^attachment; filename="(' + webpName + ')"$');
const webpUpload = new RegExp('^/wp-content/uploads/(?:[0-9]{4}/[0-9]{2}/)?(' + webpName + ')$');
const contentLimit = 2 * 1024 * 1024;

function contentRoute(path, method) {
  if (!['GET', 'POST'].includes(method)) return false;
  return /^\/wp-json\/wp\/v2\/posts(?:\/[1-9][0-9]{0,18})?$/.test(path) ||
    (method === 'POST' && path === media);
}

function allowed(path, method) {
  if (method === 'POST') return path === writeAbility + '/run';
  if (!['GET', 'HEAD'].includes(method)) return false;
  if (path.startsWith('/wp-content/uploads/')) {
    const match = webpUpload.exec(path);
    return Boolean(match && !match[1].includes('..'));
  }
  if (postSlug.test(path.replace(/^\/|\/$/g, ''))) return true;
  return ['/', '/atelier-demo/', '/robots.txt', '/wp-json/wp/v2/users/me',
    readAbility, readAbility + '/run', writeAbility].includes(path) ||
    /^\/(?:wp-content|wp-includes)\/[A-Za-z0-9_./-]+\.(?:css|js|png|jpg|jpeg|gif|webp|svg|ico|woff2?|ttf)$/.test(path);
}

function validPostWrite(path, body) {
  let article;
  try { article = JSON.parse(body); } catch { return false; }
  if (!article || typeof article !== 'object' || Array.isArray(article)) return false;
  if (path !== posts) return article.status === 'publish' && Object.keys(article).length === 1;
  return article.status === 'draft' && typeof article.slug === 'string' &&
    article.slug.length <= 200 && postSlug.test(article.slug) &&
    Object.keys(article).every(field => ['status', 'slug', 'title', 'content', 'excerpt', 'author'].includes(field));
}

export function gateway({ origin, key, upstream, manifest, expiresAt, now = Date.now }) {
  const target = new URL(upstream);
  if (target.protocol !== 'http:' || target.hostname !== '127.0.0.1' || !target.port ||
      target.pathname !== '/' || target.username || target.password || target.search || target.hash) {
    throw new Error('The fixture upstream must stay on loopback');
  }
  const contentCredentials = ['editor', 'subscriber'].map(role => manifest.accounts?.[role])
    .filter(account => account?.username && account?.password)
    .map(account => digest('Basic ' + Buffer.from(account.username + ':' + account.password).toString('base64')));
  return createServer(async (req, res) => {
    const reply = (status, value) => {
      res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store',
        'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff' });
      res.end(JSON.stringify(value));
    };
    // Health stays green after expiry: restarting must be an explicit operator action.
    if (req.url === '/_noyaru-demo/health' && req.method === 'GET') {
      return reply(200, { ready: true, expired: now() >= expiresAt });
    }
    if (now() >= expiresAt) return reply(410, { error: 'demonstration_expired' });
    if (req.url === '/_noyaru-demo/credentials' && req.method === 'GET') {
      const candidate = /^Bearer (.+)$/.exec(req.headers.authorization || '')?.[1] || '';
      const token = validDemoKey(candidate) ? candidate : '';
      if (!timingSafeEqual(digest(token), digest(key))) return reply(401, { error: 'unauthorized' });
      return reply(200, { ...manifest, expires_at: new Date(expiresAt).toISOString() });
    }
    let url;
    try { url = new URL(req.url, origin); } catch { return reply(400, { error: 'invalid_request' }); }
    const content = contentRoute(url.pathname, req.method);
    if (!req.url.startsWith('/') || req.url.startsWith('//') || url.origin !== origin ||
        /[%\\]/.test(req.url.split('?')[0]) || url.searchParams.has('rest_route') || url.searchParams.has('_method') ||
        (!content && !allowed(url.pathname, req.method))) return reply(404, { error: 'not_found' });
    if (content) {
      const provided = digest(req.headers.authorization || '');
      if (!contentCredentials.some(expected => timingSafeEqual(provided, expected))) {
        return reply(401, { error: 'unauthorized' });
      }
      if ([...url.searchParams].some(([name, value]) => req.method !== 'GET' ||
          !((name === 'context' && value === 'edit') || (name === 'per_page' && value === '1')))) {
        return reply(404, { error: 'not_found' });
      }
    }
    const upload = url.pathname === media;
    if (upload) {
      const disposition = webpDisposition.exec(req.headers['content-disposition'] || '');
      if (req.headers['content-type'] !== 'image/webp' || !disposition || disposition[1].includes('..')) {
        return reply(415, { error: 'unsupported_media' });
      }
    }
    const limit = content ? contentLimit : 16384;
    const chunks = [];
    let length = 0;
    if (Number(req.headers['content-length']) > limit) return reply(413, { error: 'request_too_large' });
    try {
      for await (const chunk of req) {
        length += chunk.length;
        if (length > limit) return reply(413, { error: 'request_too_large' });
        chunks.push(chunk);
      }
    } catch { return; }
    if (now() >= expiresAt) return reply(410, { error: 'demonstration_expired' });
    const headers = { host: new URL(origin).host, 'x-forwarded-proto': 'https' };
    // Only the connector's API receives credentials; browser cookies are never forwarded.
    if (url.pathname.startsWith('/wp-json/') && req.headers.authorization) {
      headers.authorization = req.headers.authorization;
    }
    if (req.headers['content-type']) headers['content-type'] = req.headers['content-type'];
    const body = Buffer.concat(chunks);
    if (upload) {
      if (body.length < 12 || body.toString('ascii', 0, 4) !== 'RIFF' || body.toString('ascii', 8, 12) !== 'WEBP') {
        return reply(415, { error: 'unsupported_media' });
      }
      headers['content-disposition'] = req.headers['content-disposition'];
    } else if (content && req.method === 'POST' &&
        (!/^application\/json(?:\s*;|$)/i.test(req.headers['content-type'] || '') || !validPostWrite(url.pathname, body))) {
      return reply(400, { error: 'invalid_post' });
    }
    headers['content-length'] = String(body.length);
    const proxy = request({ hostname: '127.0.0.1', port: target.port,
      method: req.method, path: url.pathname + url.search, headers, timeout: 30000 }, response => {
      const outgoing = { 'Cache-Control': 'no-store', 'Referrer-Policy': 'no-referrer', 'X-Content-Type-Options': 'nosniff' };
      for (const name of ['content-type', 'content-length', 'location', 'link']) {
        if (response.headers[name]) outgoing[name] = response.headers[name];
      }
      res.writeHead(response.statusCode, outgoing);
      response.on('error', () => res.destroy());
      response.pipe(res);
    });
    proxy.on('timeout', () => proxy.destroy());
    proxy.on('error', () => {
      if (!res.headersSent) reply(502, { error: 'fixture_unavailable' });
      else res.destroy();
    });
    res.once('close', () => proxy.destroy());
    proxy.end(body);
  });
}
