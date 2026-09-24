import { randomBytes, scrypt as scryptCallback, timingSafeEqual, createHash } from 'node:crypto';
import { promisify } from 'node:util';
const scrypt = promisify(scryptCallback);
const COOKIE = 'cps_session';
const TTL = 12 * 60 * 60 * 1000;
const digest = (value) => createHash('sha256').update(value).digest('hex');

export async function createAdminAuth(config, saveConfig) {
  async function hash(password) {
    const salt = randomBytes(16).toString('hex');
    const key = await scrypt(password, salt, 64);
    return `${salt}:${key.toString('hex')}`;
  }
  async function verify(password) {
    if (typeof password !== 'string' || !password || password.length > 256) return false;
    const [salt, encoded] = config.adminPasswordHash.split(':');
    const key = await scrypt(password, salt, 64);
    const expected = Buffer.from(encoded, 'hex');
    return expected.length === key.length && timingSafeEqual(key, expected);
  }
  if (!config.adminPasswordHash) {
    const initial = process.env.ADMIN_PASSWORD || config.proxyKey;
    if (!initial) throw new Error('Set ADMIN_PASSWORD or proxyKey to initialize console login');
    config.adminPasswordHash = await hash(initial);
    saveConfig();
  }
  const sessions = new Map();
  const attempts = new Map();
  let busy = 0;
  function token(req) {
    return String(req.headers.cookie || '').split(';').map(x => x.trim()).find(x => x.startsWith(`${COOKIE}=`))?.slice(COOKIE.length + 1) || '';
  }
  function prune() {
    const now = Date.now();
    for (const [key, expires] of sessions) if (expires <= now) sessions.delete(key);
    for (const [key, state] of attempts) if (state.reset <= now) attempts.delete(key);
  }
  function authorized(req) {
    prune();
    return (sessions.get(digest(token(req))) || 0) > Date.now();
  }
  function sameOrigin(req) {
    // Admin browser mutations must carry the exact configured Origin. No Host-header fallback in production.
    const expected = config.publicBaseUrl || `http://${req.headers.host}`;
    try { return req.headers.origin === new URL(expected).origin; } catch { return false; }
  }
  function cookie(res, value, maxAge) {
    const secure = /^https:\/\//i.test(config.publicBaseUrl || '');
    res.setHeader('Set-Cookie', `${COOKIE}=${value}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure ? '; Secure' : ''}`);
  }
  function send(res, status, body) {
    res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
    res.end(JSON.stringify(body));
  }
  async function body(req) {
    if (!String(req.headers['content-type'] || '').toLowerCase().startsWith('application/json')) throw new Error('请使用 JSON 请求');
    const parts = []; let size = 0;
    for await (const part of req) {
      size += part.length;
      if (size > 4096) throw new Error('请求内容过大');
      parts.push(part);
    }
    const data = JSON.parse(Buffer.concat(parts).toString());
    if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('请求格式错误');
    return data;
  }
  async function handle(req, res, pathname) {
    if (!['/api/auth/login', '/api/auth/logout', '/api/auth/password'].includes(pathname)) return false;
    if (req.method !== 'POST') { send(res, 405, { error: { message: '仅支持 POST' } }); return true; }
    if (!sameOrigin(req)) { send(res, 403, { error: { message: '请求来源无效，请从本站登录' } }); return true; }
    if (pathname !== '/api/auth/login' && !authorized(req)) { send(res, 401, { error: { message: '请重新登录' } }); return true; }
    if (pathname === '/api/auth/logout') {
      sessions.delete(digest(token(req))); cookie(res, '', 0); send(res, 200, { ok: true }); return true;
    }
    prune();
    const peer = req.socket.remoteAddress;
    const ip = ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(peer) ? (req.headers['x-real-ip'] || peer) : peer;
    const state = attempts.get(ip) || { count: 0, reset: Date.now() + 15 * 60 * 1000 };
    if (state.count >= 10 || busy >= 2) { res.setHeader('Retry-After', '60'); send(res, 429, { error: { message: '尝试过于频繁，请稍后重试' } }); return true; }
    if (attempts.size >= 10000 && !attempts.has(ip)) { send(res, 429, { error: { message: '请稍后重试' } }); return true; }
    state.count++; attempts.set(ip, state); busy++;
    try {
      const data = await body(req);
      if (pathname === '/api/auth/login') {
        const loginHash = config.adminPasswordHash;
        if (!await verify(data.password) || loginHash !== config.adminPasswordHash) { send(res, 401, { error: { message: '登录密码错误' } }); return true; }
        attempts.delete(ip);
        const value = randomBytes(32).toString('base64url');
        if (sessions.size >= 1000) sessions.delete(sessions.keys().next().value);
        sessions.set(digest(value), Date.now() + TTL);
        cookie(res, value, TTL / 1000); send(res, 200, { ok: true });
      } else {
        if (!await verify(data.currentPassword)) { send(res, 400, { error: { message: '当前密码错误' } }); return true; }
        const next = data.newPassword;
        if (typeof next !== 'string' || next.length < 8 || next.length > 256 || !next.trim()) {
          send(res, 400, { error: { message: '新密码需为 8–256 个字符' } }); return true;
        }
        if (next === data.currentPassword) { send(res, 400, { error: { message: '新密码不能与当前密码相同' } }); return true; }
        // Re-check session after expensive verification to fence concurrent password changes.
        const nextHash = await hash(next);
        if (!authorized(req)) { send(res, 401, { error: { message: '会话已失效，请重新登录' } }); return true; }
        const oldHash = config.adminPasswordHash;
        config.adminPasswordHash = nextHash;
        try { saveConfig(); } catch (error) { config.adminPasswordHash = oldHash; throw error; }
        sessions.clear(); attempts.delete(ip); cookie(res, '', 0);
        send(res, 200, { ok: true, loginRequired: true });
      }
    } catch (error) {
      send(res, error instanceof SyntaxError ? 400 : 400, { error: { message: error instanceof SyntaxError ? '请求格式错误' : '请求失败，请检查输入后重试' } });
    } finally { busy--; }
    return true;
  }
  return { authorized, sameOrigin, handle };
}
