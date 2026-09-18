// Cline Pass 上游观察/切换代理
// 零依赖，Node >= 18。
//
// 网关行为（实测结论，README 有证据）：
// - 订阅模型（cline-pass/*）与非 free 目录模型：请求体里的 provider.* 会被 Cline 网关丢弃，
//   由其规划器在系统凭证上游中自行挑选，响应元数据可回读实际上游。
// - 目录模型 :free 变体：provider.only 真正透传到 OpenRouter，可精确钉住。
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { Readable, Transform } from 'node:stream';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || __dirname;
const CONFIG_PATH = path.join(DATA_DIR, 'config.json');
const META_PATH = path.join(DATA_DIR, 'metadata.json');
const PUBLIC_DIR = path.join(__dirname, 'public');

const DEFAULT_CONFIG = {
  port: 3123,
  apiKey: '',
  proxyKey: '',
  publicBaseUrl: '',
  exposeCatalog: false,    // true 时 /v1/models 合并完整目录模型（默认仅订阅模型）
  upstreamBase: 'https://api.cline.bot/api/v1',
  accounts: [],            // { name, key, enabled } —— Cline Pass 账号池
  accountMode: 'single',   // single=手动指定 | roundrobin=轮询 | weighted=按额度权重加权随机
  activeAccount: 0,        // single 模式下使用的账号下标
  // ---- 并发闸门与加权选号 ----
  // 目标：并发超限时排队等待，绝不本地回 429；加权只按「额度剩余」算，不绑定会话、不做粘性。
  accountConcurrency: 5,   // 未单独设置 concurrency 的账号使用的默认并发
  poolConcurrency: 0,      // 账号池总并发上限；0 = 不限制
  queueTimeoutMs: 120000,  // 并发满时的排队等待上限；0 = 不排队（立即回 503）
  // 上游真 429 时的退避重试；totalTimeoutMs 限住整条请求花在重试上的总时长，避免一条请求拖到分钟级
  retry429: { retries: 3, baseDelayMs: 1500, maxDelayMs: 20000, totalTimeoutMs: 120000 },
  quotaWeights: { five_hour: 1, weekly: 1, monthly: 1 },           // 三窗口剩余做加权平均时各窗口的权重
  quotaStopPercent: 100,   // 任一窗口「已用」达到该值时该账号退出加权（100 = 真正用满才让路）
  quotaRefreshMs: 900000,  // 选号用额度快照的后台刷新间隔
  knownModels: [
    'cline-pass/glm-5.3-flash',
    'cline-pass/kimi-k3',
    'cline-pass/deepseek-v4-flash',
    'cline-pass/deepseek-v4.1-flash',
    'cline-pass/qwen3.8-max',
    'cline-pass/minimax-m3',
    'cline-pass/glm-5.3',
    'cline-pass/glm-5.2',
    'cline-pass/deepseek-v4-pro',
    'cline-pass/mimo-v2.5-pro',
    'cline-pass/mimo-v2.5',
    'cline-pass/kimi-k2.6',
    'cline-pass/qwen3.7-plus',
    'cline-pass/kimi-k2.7-code',
    'cline-pass/qwen3.7-max',
  ],
  // modelId -> { upstreams: string[]（有序优先列表，空=自动）, exclude: string[]（排除列表，优先级高于勾选）,
  //              pinMode: 'strict'|'preferred', sort: 'cost'|'ttft'|'tps'|null }
  // 请求按 upstreams 顺序逐个钉住尝试：第一个异常（非 200 / 网络失败 / 超时）自动顺切下一个，
  // 全部失败才把最后一个错误透传给客户端；exclude 中的上游永不被使用（自动模式下注入排除偏好）。
  // upstream 为旧版单上游兼容镜像（取列表第一个），maxRetries 已退役（旧值仅作回滚兼容保留在文件里）。
  perModel: {},
};

function loadJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return fallback;
  }
}
const config = { ...DEFAULT_CONFIG, ...loadJson(CONFIG_PATH, {}) };
const META = loadJson(META_PATH, { models: {}, history: [], catalog: null, orModelsFetchedAt: 0, orModelList: null });
const saveConfig = () => fs.writeFileSync(CONFIG_PATH, JSON.stringify(config, null, 2));
// metadata.json 每次请求都会变（历史/统计），但每个请求都全量重写一遍既慢又会放大磁盘写入。
// 这里合并成「最多 1 秒落盘一次」，退出前 flush，最多丢 1 秒的诊断数据。
let metaTimer = null;
let metaDirty = false;
function saveMetaNow() {
  metaDirty = false;
  try { fs.writeFileSync(META_PATH, JSON.stringify(META, null, 2)); } catch (e) { console.error('[元数据写入失败]', e.message); }
}
function flushMeta() {
  if (metaTimer) { clearTimeout(metaTimer); metaTimer = null; }
  if (metaDirty) saveMetaNow();
}
function saveMeta() {
  metaDirty = true;
  if (metaTimer) return;
  metaTimer = setTimeout(() => { metaTimer = null; if (metaDirty) saveMetaNow(); }, 1000);
  metaTimer.unref?.();
}
// 旧版单 apiKey 迁移为账号池
if ((!Array.isArray(config.accounts) || config.accounts.length === 0) && config.apiKey) {
  config.accounts = [{ name: '默认账号', key: config.apiKey, enabled: true }];
  config.accountMode = 'single';
  config.activeAccount = 0;
  saveConfig();
}
const ACCOUNT_MODES = new Set(['single', 'roundrobin', 'weighted']);
config.accountMode = ACCOUNT_MODES.has(config.accountMode) ? config.accountMode : 'single';

// perModel 配置升级：旧版单 upstream 迁移为有序多上游列表（upstream 保留为回滚兼容镜像）
(function migratePerModel() {
  let dirty = false;
  for (const c of Object.values(config.perModel || {})) {
    if (!c || typeof c !== 'object') continue;
    if (c.upstreams === undefined) { c.upstreams = c.upstream ? [c.upstream] : []; dirty = true; }
    if (c.exclude === undefined) { c.exclude = []; dirty = true; }
    if (!Array.isArray(c.upstreams)) { c.upstreams = []; dirty = true; }
    if (!Array.isArray(c.exclude)) { c.exclude = []; dirty = true; }
  }
  if (dirty) saveConfig();
})();

// 环境变量覆盖（便于 Docker 部署）。注意：此后若通过控制台保存设置，当前生效值会写回 config.json
if (process.env.CLINE_PASS_KEY) {
  const k = process.env.CLINE_PASS_KEY.trim();
  if (k && !(config.accounts || []).some((a) => a.key === k)) {
    config.accounts = [{ name: 'env-account', key: k, enabled: true }, ...(config.accounts || [])];
  }
}
if (process.env.PROXY_KEY && process.env.PROXY_KEY.trim()) config.proxyKey = process.env.PROXY_KEY.trim();
if (process.env.PUBLIC_BASE_URL) config.publicBaseUrl = process.env.PUBLIC_BASE_URL.trim();
if (process.env.PORT) config.port = Number(process.env.PORT) || config.port;

function isConfigured() {
  return !!config.apiKey || enabledAccounts().length > 0;
}
if (!isConfigured()) {
  console.warn('[提示] 尚未配置上游 API Key：打开控制台「账号管理」添加账号并保存即可；服务已启动。');
}

// ---------- 账号选择与并发闸门 ----------
// 三种模式：
//   single     只用 activeAccount 指定的账号（该账号并发满则排队等它）
//   roundrobin 在启用账号间顺序轮询（并发已满的账号自动跳过，不空转）
//   weighted   按额度权重加权随机：剩余额度越多，被抽中的概率越高（本需求的主模式）
// 关键约定：
// 1) 权重只在「取号」这一刻计算，不绑定会话、不做粘性；每条请求独立按权重抽一次。
// 2) 并发超限时排队等待，绝不向客户端回 429（429 只可能来自上游，见 retry429 退避重试）。
// 3) 名额在整条请求生命周期内持有（含流式透传），请求结束/断开/出错都必须归还。
let RR_COUNTER = 0;

// 注意 Number(null)===0、Number('')===0：缺省必须靠「是不是空值」判断，不能靠 Number() 的结果，
// 否则 weight:null 会被静默写成 0（该账号被排出加权）、concurrency:null 会变成 1。
// 显式传 0 仍然是合法的 0（例如 queueTimeoutMs=0 = 不排队）。
const numOrNull = (v) => (v === null || v === undefined || v === '' ? null : (Number.isFinite(Number(v)) ? Number(v) : null));
const clampInt = (v, lo, hi, dflt) => {
  const n = numOrNull(v);
  if (n === null) return dflt;
  return Math.min(hi, Math.max(lo, Math.floor(n)));
};
const clampNum = (v, lo, hi, dflt) => {
  const n = numOrNull(v);
  if (n === null) return dflt;
  return Math.min(hi, Math.max(lo, n));
};

function enabledAccounts() {
  return (config.accounts || []).filter((a) => a && a.key && a.enabled !== false);
}

// 启用中的账号列表（补显示名）。下标按 config.accounts 原始位置生成，
// 与 quotaSnapshot / META.stats 的命名规则保持一致（三处都靠「账号名」对齐）。
function accountList() {
  const all = config.accounts || [];
  const list = [];
  all.forEach((a, i) => {
    if (a && a.key && a.enabled !== false) list.push({ ...a, name: String(a.name || `账号${i + 1}`) });
  });
  if (!list.length && config.apiKey) return [{ name: '默认', key: config.apiKey, enabled: true }];
  return list;
}

function accountConcurrencyOf(a) {
  const own = Number(a?.concurrency);
  if (Number.isFinite(own) && own > 0) return clampInt(own, 1, 1000, 5);
  return clampInt(config.accountConcurrency, 1, 1000, 5);
}
function poolLimit() {
  const n = Number(config.poolConcurrency);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : Infinity;
}
function queueTimeout() {
  return clampInt(config.queueTimeoutMs, 0, 3600e3, 120000);
}
// 429 重试计划。totalTimeoutMs 是「整条请求用于 429 重试的总预算」：
// 没有它时，retries=3 + attemptTimeoutMs=120s 最坏能把一条请求拖到 8 分钟，客户端早就超时了。
function retry429Plan() {
  const c = config.retry429 || {};
  const retries = clampInt(c.retries, 0, 10, 0);
  const base = clampInt(c.baseDelayMs, 0, 60000, 0);
  const max = clampInt(c.maxDelayMs, base, 300000, base);
  return { retries, baseDelayMs: base, maxDelayMs: max, totalTimeoutMs: clampInt(c.totalTimeoutMs, 1000, 900e3, 120000) };
}

// 额度剩余（0~100）按 quotaWeights 做加权平均；读不到额度返回 null。
// 权重口径（用户指定）：三个窗口剩余的平均值 —— 哪个窗口先撞上限都算进去，不只看单窗口。
const UNKNOWN_HEADROOM = 50;   // 额度未知时的中性余量：不清零，否则额度接口抖动会把账号饿死
function accountHeadroom(q) {
  if (!q || !q.ok) return null;
  let num = 0, den = 0;
  for (const w of q.windows || []) {
    const p = Number(w?.percent);
    if (!Number.isFinite(p)) continue;
    const gw = Number(config.quotaWeights?.[w.type]);
    const k = Number.isFinite(gw) && gw >= 0 ? gw : 1;
    num += Math.max(0, 100 - p) * k;
    den += k;
  }
  return den > 0 ? num / den : null;
}
// 任一窗口「已用」达到 quotaStopPercent 就让该账号退出加权（默认 100 = 真正用满才让路）
function quotaBlocked(q) {
  const stop = Number(config.quotaStopPercent);
  if (!Number.isFinite(stop) || stop <= 0) return false;
  return (q?.windows || []).some((w) => Number.isFinite(Number(w?.percent)) && Number(w.percent) >= stop);
}
// 有效权重 = 额度剩余均值 × 手动 weight（默认 1）；weight=0 表示该账号不参与加权
function accountWeight(a, q) {
  const manual = clampNum(a?.weight, 0, 100, 1);
  const head = accountHeadroom(q);
  const base = head === null ? UNKNOWN_HEADROOM : head;
  return { weight: base * manual, headroom: head, manual, blocked: quotaBlocked(q) };
}
function quotaByName(name) {
  return (quotaCache.accounts || []).find((a) => a.name === name) || null;
}

// 并发闸门状态：per-account 计数 + 池总量 + FIFO 等待队列
const pool = { total: 0, active: new Map(), queue: [] };
const activeOf = (name) => pool.active.get(name) || 0;

// 按模式圈定候选账号（这里只按模式过滤，不再按并发过滤；并发在 allocateSlot 里判断）
function eligibleAccounts() {
  const list = accountList();
  if (!list.length) return [];
  if (config.accountMode !== 'single') return list;
  const all = config.accounts || [];
  const idx = clampInt(config.activeAccount, 0, Math.max(0, all.length - 1), 0);
  const want = all[idx];
  if (want && want.key && want.enabled !== false) {
    const name = String(want.name || `账号${idx + 1}`);
    const hit = list.find((a) => a.name === name);
    if (hit) return [hit];
  }
  return list;
}

// 加权随机抽一个账号；cands 为空（例如所有账号并发都满了）返回 null，由调用方决定排队
function weightedPick(cands) {
  if (!cands || !cands.length) return null;
  const scored = cands.map((a) => {
    const w = accountWeight(a, quotaByName(a.name));
    return { a, w: w.blocked ? 0 : w.weight };
  });
  // 全部被额度阈值挡下（或手动权重全是 0）时退化为等概率，避免账号池直接不可用
  const usable = scored.filter((s) => s.w > 0);
  const list = usable.length ? usable : scored.map((s) => ({ a: s.a, w: 1 }));
  const total = list.reduce((s, x) => s + x.w, 0);
  let r = Math.random() * total;
  for (const x of list) { r -= x.w; if (r <= 0) return x.a; }
  return list[list.length - 1].a;
}

// 真正分配一个名额：池总量未满 + 目标账号未满
function allocateSlot() {
  if (pool.total >= poolLimit()) return null;
  const cands = eligibleAccounts();
  if (!cands.length) return null;
  const hasRoom = (a) => activeOf(a.name) < accountConcurrencyOf(a);
  let pick = null;
  if (config.accountMode === 'weighted') {
    pick = weightedPick(cands.filter(hasRoom));
  } else if (config.accountMode === 'roundrobin') {
    // 从上次的位置往后找第一个还有空闲名额的账号：满了就跳过，不空转
    const n = cands.length;
    const start = n ? RR_COUNTER % n : 0;
    for (let i = 0; i < n; i++) {
      const a = cands[(start + i) % n];
      if (hasRoom(a)) { pick = a; RR_COUNTER = (start + i + 1) % 1000000000; break; }
    }
  } else {
    const a = cands[0];
    if (a && hasRoom(a)) pick = a;
  }
  if (!pick) return null;
  pool.total += 1;
  pool.active.set(pick.name, activeOf(pick.name) + 1);
  return { name: pick.name, key: pick.key, concurrency: accountConcurrencyOf(pick), released: false };
}

function releaseSlot(lease) {
  if (!lease || lease.released) return;
  lease.released = true;
  pool.total = Math.max(0, pool.total - 1);
  pool.active.set(lease.name, Math.max(0, activeOf(lease.name) - 1));
  drainQueue();
}

// 名额释放后按 FIFO 把名额发给等待者（已取消/超时/对端已断开的等待者原地丢弃）
function drainQueue() {
  for (;;) {
    while (pool.queue.length && pool.queue[0].done) pool.queue.shift();
    if (!pool.queue.length) return;
    const lease = allocateSlot();
    if (!lease) return;
    const w = pool.queue.shift();
    // w.done：已超时或已被 close 事件取消；w.gone()：对端其实已经走了，但服务端还没收到 close
    // （实测 undici 客户端 abort 后服务端 close 事件可能滞后甚至不到达）。这种情况不能再发名额，
    // 否则会替一个已经离开的客户端白占并发、白烧上游额度。
    if (w.done || w.gone()) {
      // 名额直接还回去（不能调 releaseSlot，否则会递归重入 drainQueue）
      pool.total = Math.max(0, pool.total - 1);
      pool.active.set(lease.name, Math.max(0, activeOf(lease.name) - 1));
      continue;
    }
    w.resolve(lease);
  }
}

// 取号：有空闲立即返回；否则进 FIFO 队列等待；排队超时/客户端断开则 reject（绝不回 429）
function acquireSlot(req, res, timeoutMs) {
  quotaPeek();   // 过期就丢一个后台刷新（不阻塞）：single/roundrobin 下也要能续期
  if (!eligibleAccounts().length) {
    return Promise.reject(Object.assign(new Error('尚未配置可用的上游账号：请在控制台「账号管理」添加账号并启用'), { code: 'NO_ACCOUNT' }));
  }
  const got = allocateSlot();
  if (got) return Promise.resolve(got);
  const waitMs = clampInt(timeoutMs, 0, 3600e3, 120000);
  if (waitMs <= 0) {
    return Promise.reject(Object.assign(new Error('账号并发已满且未开启排队（queueTimeoutMs=0）'), { code: 'BUSY' }));
  }
  return new Promise((resolve, reject) => {
    // 对端是否已经走了。注意：不能看 req.destroyed —— 请求体读完后它本来就是 true/false 都不可靠；
    // 真正可靠的是 socket 是否已销毁（实测客户端 abort 后 socket.destroyed 会变 true）。
    // 另外 req 的 'close' 事件在「请求体读完」时就已经发过了（Node ≥16 语义），
    // 所以我们这里注册的 close 监听多数情况下不会再触发，只能当兜底，主力判定是 gone()。
    const w = {
      done: false,
      resolve: null,
      reject: null,
      timer: null,
      gone: () => !!(req?.socket?.destroyed || res?.destroyed),
    };
    const onClose = () => w.reject(Object.assign(new Error('客户端在排队期间断开连接'), { code: 'CLIENT_CLOSED' }));
    w.resolve = (lease) => {
      if (w.done) return;
      w.done = true; clearTimeout(w.timer); req?.off?.('close', onClose);
      resolve(lease);
    };
    w.reject = (err) => {
      if (w.done) return;
      w.done = true; clearTimeout(w.timer); req?.off?.('close', onClose);
      reject(err);
    };
    w.timer = setTimeout(() => w.reject(Object.assign(new Error(`账号并发已满，排队 ${waitMs}ms 仍未取得名额`), { code: 'QUEUE_TIMEOUT' })), waitMs);
    pool.queue.push(w);
    req?.once?.('close', onClose);
  });
}

// 运行态快照（控制台展示实时并发占用与各账号的有效权重）
function poolSnapshot() {
  const limit = poolLimit();
  const weighted = config.accountMode === 'weighted';
  // 这里必须列出「全部账号（含禁用/无 key）」，且名字规则与 quotaSnapshot 完全一致：
  // 控制台表格是按配置里的账号逐行渲染的，运行态少一项就会让「按下标回退」把别的账号的数字显示到这一行上。
  const accounts = (config.accounts || []).map((a, i) => {
    const name = String(a?.name || `账号${i + 1}`);
    const enabled = !!(a && a.key && a.enabled !== false);
    const info = accountWeight(a, quotaByName(name));
    const weightBlocked = info.blocked || info.weight <= 0;
    return {
      name,
      enabled,
      active: activeOf(name),
      limit: accountConcurrencyOf(a),
      weight: Math.round(info.weight * 100) / 100,
      manualWeight: info.manual,
      headroom: info.headroom === null ? null : Math.round(info.headroom * 10) / 10,
      // blocked = 「当下真的不会参与选号」：只有 weighted 模式才由权重/额度阈值决定；
      // single / roundrobin 不看权重，标成"已让路"就是骗人（实测 weight=0 时仍 100% 走该账号）。
      blocked: weighted && enabled && weightBlocked,
      quotaStop: info.blocked,          // 原始原因：额度触顶
      weightZero: info.weight <= 0,     // 原始原因：手动倍数 0
    };
  });
  return {
    mode: config.accountMode,
    weighted,
    total: pool.total,
    limit: limit === Infinity ? 0 : limit,
    queued: pool.queue.filter((w) => !w.done).length,
    queueTimeoutMs: queueTimeout(),
    quotaAt: quotaCache.at || 0,
    quotaRefreshMs: quotaTtlMs(),   // 控制台据此显示「每 N 分钟自动刷新」
    accounts,
  };
}

// 同步选号（控制台探测/测试/校验等管理路径使用，不占用并发名额，避免管理操作被闸门卡住）
function pickAccount() {
  quotaPeek();   // 管理路径（探测/测试/校验）同样顺手续期额度快照
  const list = accountList();
  if (!list.length) return { name: '默认', key: config.apiKey || '' };
  if (config.accountMode === 'weighted') return weightedPick(list);
  if (config.accountMode === 'roundrobin' && list.length > 1) {
    const a = list[RR_COUNTER % list.length];
    RR_COUNTER = (RR_COUNTER + 1) % 1000000000;
    return a;
  }
  const all = config.accounts || [];
  const idx = clampInt(config.activeAccount, 0, Math.max(0, all.length - 1), 0);
  const want = all[idx];
  if (want && want.key && want.enabled !== false) {
    const name = String(want.name || `账号${idx + 1}`);
    const hit = list.find((a) => a.name === name);
    if (hit) return hit;
  }
  return list[0];
}
const chatHeaders = (key) => ({
  'Content-Type': 'application/json',
  Authorization: `Bearer ${key}`,
});

// 代理密钥：非空时，/v1/* 与 /api/* 均需鉴权（Authorization: Bearer <key> 或 X-Admin-Key: <key>）；
// 控制台页面本身保持开放（不含任何敏感数据，数据由带鉴权的 /api/* 提供）。
// 可通过 POST /api/security 在运行期修改（下游密钥 = 客户端访问代理的凭据）。
let PROXY_KEY = config.proxyKey || '';
function authOK(req) {
  if (!PROXY_KEY) return true;
  const bearer = String(req.headers['authorization'] || '').replace(/^Bearer\s+/i, '').trim();
  const admin = String(req.headers['x-admin-key'] || '').trim();
  return bearer === PROXY_KEY || admin === PROXY_KEY;
}
function unauthorized(res) {
  return sendJSON(res, 401, { error: { message: 'unauthorized: 代理密钥缺失或错误', type: 'auth_error' } });
}
function publicProxyBase() {
  return config.publicBaseUrl
    ? `${config.publicBaseUrl.replace(/\/+$/, '')}/v1`
    : `http://127.0.0.1:${config.port}/v1`;
}

const OR_API = 'https://openrouter.ai/api/v1';

async function fetchJSON(url, opts = {}, timeoutMs = 60000) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: ctrl.signal });
    const text = await res.text();
    let json = null;
    try { json = JSON.parse(text); } catch { json = { raw: text }; }
    return { status: res.status, json };
  } finally {
    clearTimeout(t);
  }
}

// ---------- OpenRouter 目录缓存与 slug 归一化 ----------
async function orModelList() {
  if (META.orModelList && Date.now() - META.orModelsFetchedAt < 6 * 3600e3) return META.orModelList;
  const { json } = await fetchJSON(`${OR_API}/models`);
  const ids = (json?.data || []).map((m) => m.id);
  if (ids.length) {
    META.orModelList = ids;
    META.orModelsFetchedAt = Date.now();
    saveMeta();
  }
  return ids;
}
const norm = (s) => s.toLowerCase().replace(/[^a-z0-9]/g, '');

async function orEndpoints(slug) {
  // canonicalSlug 与 OpenRouter 目录 id 可能存在连字符差异（zai/... vs z-ai/...），先精确后归一匹配
  const ids = await orModelList();
  let real = ids.find((id) => id === slug) || ids.find((id) => norm(id) === norm(slug));
  if (!real) return { slug, endpoints: [] };
  const { json } = await fetchJSON(`${OR_API}/models/${real}/endpoints`);
  const eps = json?.data?.endpoints || [];
  const detail = {};
  for (const e of eps) {
    const pSlug = String(e.tag || '').split('/')[0] || (e.provider_name || '').toLowerCase().replace(/\s+/g, '-');
    const d = (detail[pSlug] ||= { slug: pSlug, name: e.provider_name, endpoints: 0, context: 0, uptime: 0 });
    d.endpoints++;
    d.context = Math.max(d.context, e.context_length || 0);
    d.uptime = Math.max(d.uptime, Math.round(e.uptime_last_30m || 0));
  }
  return { slug: real, endpoints: Object.values(detail) };
}

// ---------- 探测单个模型 ----------
// 两条管道（实测）：
// - planner：响应带 provider_metadata.gateway.routing（canonicalSlug/finalProvider/fallbacksAvailable），
//   请求体 provider.* 被网关丢弃。
// - direct：响应顶层带 provider（显示名）与 model（真实 OpenRouter ID），provider.only 会透传到
//   OpenRouter，可精确钉住。
function slugify(s) { return String(s).toLowerCase().replace(/\s+/g, '-'); }

function parseRouting(json) {
  const d = json?.data && json.data.choices ? json.data : json;
  const msg = d?.choices?.[0]?.message;
  const rt = msg?.provider_metadata?.gateway?.routing || d?.provider_metadata?.gateway?.routing || {};
  const direct = typeof d?.provider === 'string' ? d.provider : null;
  return {
    content: msg?.content ?? null,
    usage: d?.usage || null,
    pipeline: rt.finalProvider ? 'planner' : direct ? 'direct' : null,
    canonicalSlug: rt.canonicalSlug || (typeof d?.model === 'string' && d.model.includes('/') ? d.model : null),
    finalProvider: rt.finalProvider || (direct ? slugify(direct) : null),
    finalProviderName: rt.finalProvider || direct,
    fallbacks: rt.fallbacksAvailable || [],
    plan: rt.planningReasoning || '',
  };
}

// 故意携带不存在的 only，让网关在路由层报错并列出可用上游（不产生 token 消耗）。
// - 直连管道（OpenRouter）：provider.only → 404 错误 JSON 里的 metadata.available_providers
// - 规划器管道（Vercel AI Gateway）：providerOptions.gateway.only → 400 错误文本里的 "Available providers are: ..."
async function harvestAvailableProviders(modelId, pipeline) {
  const acc = pickAccount();
  const base = { model: modelId, messages: [{ role: 'user', content: 'hi' }], max_tokens: 16 };
  const body = pipeline === 'planner'
    ? { ...base, providerOptions: { gateway: { only: ['__probe__'] } } }
    : { ...base, provider: { only: ['__probe__'] } };
  const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, { method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(body) }, 60000);
  const err = json?.error;
  if (typeof err !== 'string') return null;
  if (pipeline === 'planner') {
    const m = /Available providers are:\s*([^.]+)/.exec(err);
    if (!m) return null;
    // 错误文本里可能混有 JSON 片段（如 ","type":"invalid_request_error"），必须按 slug 格式过滤
    const toks = m[1].split(/,\s*/).map((s) => s.trim()).filter((t) => /^[a-z0-9][a-z0-9-]*$/.test(t));
    return toks.length ? toks : null;
  }
  const i = err.indexOf('{');
  if (i < 0) return null;
  try {
    return JSON.parse(err.slice(i))?.error?.metadata?.available_providers || null;
  } catch { return null; }
}
function parseTier0(plan) {
  const m = /([\w-]+) won tier 0 over ([^."]+)/.exec(plan || '');
  if (!m) return [];
  return [...new Set([m[1], ...m[2].split(/,\s*|\s+and\s+/).map((s) => s.trim()).filter(Boolean)])];
}

async function probeModel(modelId) {
  const acc = pickAccount();
  const t0 = Date.now();
  const body = { model: modelId, messages: [{ role: 'user', content: 'Reply with the word OK' }], max_tokens: 256 };
  const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
    method: 'POST',
    headers: chatHeaders(acc.key),
    body: JSON.stringify(body),
  }, 180000);
  const ms = Date.now() - t0;
  if (json?.error && !json?.data) {
    return { ok: false, error: typeof json.error === 'string' ? json.error : JSON.stringify(json.error) };
  }
  const r = parseRouting(json);
  let harvest = null;
  if (r.pipeline) harvest = await harvestAvailableProviders(modelId, r.pipeline);
  let endpoints = [];
  let orSlug = null;
  if (r.pipeline !== 'planner' && r.canonicalSlug) {
    // 规划器管道的钉住发生在 Vercel 侧，OpenRouter 的 endpoint 明细仅对直连管道有参考意义
    try {
      const res = await orEndpoints(r.canonicalSlug);
      endpoints = res.endpoints;
      orSlug = res.slug;
    } catch { /* 公开接口失败不影响探测结果 */ }
  }
  const prev = META.models[modelId] || {};
  const detail = { ...prev.upstreamDetail };
  for (const e of endpoints) detail[e.slug] = e;
  const upstreams = r.pipeline === 'planner'
    ? [...new Set([...(harvest || []), ...r.fallbacks])]
    : [...new Set([...r.fallbacks, ...(harvest || []), ...Object.keys(detail)])];
  const tier0 = [...new Set([...(prev.tier0 || []), ...parseTier0(r.plan)])];
  META.models[modelId] = {
    ...prev,
    ok: true,
    pipeline: r.pipeline,
    pinnable: !!r.pipeline,
    availableProviders: harvest || prev.availableProviders || [],
    canonicalSlug: r.canonicalSlug,
    openrouterSlug: orSlug,
    upstreamDetail: detail,
    upstreams,
    tier0,
    lastProvider: r.finalProvider || prev.lastProvider,
    lastMs: ms,
    probedAt: Date.now(),
  };
  saveMeta();
  return { ok: true, ms, ...META.models[modelId] };
}

// 上游渠道可用性分类：渠道被单独钉住时的真实状态
function classifyUpstreamError(msg) {
  const m = String(msg || '');
  if (/empty response content/i.test(m)) return 'ok';                     // 请求已到达模型（推理耗尽 max_tokens 导致内容为空）
  if (/429|rate-?limited|temporarily rate/i.test(m)) return 'limited';   // 渠道有效，共享池限流中
  if (/invalid_request|modelid|no allowed providers|no available providers|not found|unsupported/i.test(m)) return 'bad'; // 不可钉住
  if (/unauthorized|re-authenticate|401/i.test(m)) return 'auth';        // 账号 key 问题，与渠道无关
  return 'unknown';
}
// 钉住请求失败时自动学习该渠道状态（仅确定性失败，瞬时限流标 limited 不拉黑）
function learnUpstreamStatus(modelId, upstream, errMsg) {
  if (!upstream || !errMsg) return;
  const st = classifyUpstreamError(errMsg);
  if (st === 'unknown') return;
  const meta = (META.models[modelId] ||= {});
  meta.upstreamStatus = { ...(meta.upstreamStatus || {}), [upstream]: { status: st, note: String(errMsg).slice(0, 160), checkedAt: Date.now() } };
}

// 自动+排除模式：only 白名单与网关侧渠道清单不一致时，网关报错会附最新清单，合并学习
// （触发场景：探测缓存过期，网关侧新增了渠道而本地 known 列表没有——白名单漏掉新渠道）
function learnAvailableProviders(modelId, errMsg) {
  const m = /Available providers are:\s*([^.]+)/.exec(String(errMsg || ''));
  if (!m) return;
  const toks = m[1].split(/,\s*/).map((s) => s.trim()).filter((t) => /^[a-z0-9][a-z0-9-]*$/.test(t));
  if (!toks.length) return;
  const meta = (META.models[modelId] ||= {});
  const before = (meta.upstreams || []).length;
  meta.upstreams = [...new Set([...(meta.upstreams || []), ...toks])];
  if (meta.upstreams.length !== before) saveMeta();
}

// 批量校验：把模型的每个上游渠道用最小请求各钉一次，标记真实可用性
async function validateUpstreams(modelId) {
  const meta = META.models[modelId] || {};
  const list = meta.upstreams || [];
  const pipeline = meta.pipeline;
  const acc = pickAccount();
  const results = {};
  const batch = 5;
  for (let i = 0; i < list.length; i += batch) {
    await Promise.all(list.slice(i, i + batch).map(async (slug) => {
      const t0 = Date.now();
      const base = { model: modelId, messages: [{ role: 'user', content: 'hi' }], max_tokens: 16 };
      const body = pipeline === 'planner'
        ? { ...base, providerOptions: { gateway: { only: [slug] } } }
        : { ...base, provider: { only: [slug] } };
      const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
        method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(body),
      }, 60000).catch(() => ({ json: { error: 'network error' } }));
      let status = 'unknown';
      let note = '';
      if (json?.error && !json?.data) {
        const msg = typeof json.error === 'string' ? json.error : JSON.stringify(json.error);
        status = classifyUpstreamError(msg);
        note = msg.slice(0, 160);
      } else if (json?.data?.choices || json?.choices) {
        status = 'ok';
      }
      results[slug] = { status, ms: Date.now() - t0, note };
    }));
  }
  META.models[modelId] = { ...meta, upstreamStatus: { ...(meta.upstreamStatus || {}), ...results }, validatedAt: Date.now() };
  saveMeta();
  return results;
}

// 从官方接口、官方文档与社区注册表拉取最新 ClinePass 订阅模型清单（只增不删）
async function fetchOfficialModels() {
  const found = new Set();
  const sources = [];
  const addModel = (value) => {
    const id = typeof value === 'string' ? value : value?.id;
    if (typeof id !== 'string') return;
    const normalized = id.trim().toLowerCase();
    if (normalized.startsWith('cline-pass/')) found.add(normalized);
  };
  // 官方推荐模型接口：Cline 自己用来列出订阅模型，权威且更新最快（无需鉴权）
  try {
    const { json } = await fetchJSON('https://api.cline.bot/api/v1/ai/cline/recommended-models', {}, 30000);
    const list = json?.clinePass || json?.data?.clinePass;
    if (Array.isArray(list) && list.length) {
      list.forEach(addModel);
      sources.push('cline.api');
    }
  } catch { /* 来源不可用则跳过 */ }
  // 社区注册表 models.dev：历史响应包在 providers 下，新响应直接以 provider id 为顶层键
  try {
    const { json } = await fetchJSON('https://models.dev/api.json', {}, 30000);
    const cp = json?.providers?.['cline-pass'] || json?.['cline-pass'];
    if (cp?.models) {
      Object.keys(cp.models).forEach((id) => addModel(id.startsWith('cline-pass/') ? id : `cline-pass/${id}`));
      sources.push('models.dev');
    }
  } catch { /* 来源不可用则跳过 */ }
  // 官方文档表格兜底
  try {
    const res = await fetch('https://docs.cline.bot/getting-started/clinepass', { signal: AbortSignal.timeout(30000) });
    const text = await res.text();
    const ids = text.match(/cline-pass\/[a-z0-9._-]+/gi) || [];
    if (ids.length) { ids.forEach((id) => found.add(id.toLowerCase())); sources.push('docs.cline.bot'); }
  } catch { /* 来源不可用则跳过 */ }
  const valid = [...found].filter((id) => /^cline-pass\/[a-z0-9._-]+$/.test(id));
  const added = valid.filter((id) => !config.knownModels.includes(id));
  if (added.length) {
    config.knownModels.push(...added);
    saveConfig();
  }
  META.officialModelsFetch = { ts: Date.now(), sources, found: valid.length, added, total: config.knownModels.length };
  saveMeta();
  return { sources, found: valid.length, added, knownModels: config.knownModels, ...META.officialModelsFetch };
}

// META.models 以「任意 modelId」为键：只要有人拿随机模型名打代理就能无限撑大 metadata.json。
// 兜底裁剪：超过 MODEL_META_MAX 条时按最后活动时间淘汰最旧的（knownModels 的条目永不淘汰）。
const MODEL_META_MAX = 500;
function metaActivity(m) {
  return m?.lastUsed || m?.probedAt || m?.validatedAt || 0;
}
function pruneModelMeta() {
  const keys = Object.keys(META.models);
  if (keys.length <= MODEL_META_MAX) return;
  const keep = new Set(config.knownModels || []);
  const removable = keys.filter((k) => !keep.has(k)).sort((a, b) => metaActivity(META.models[a]) - metaActivity(META.models[b]));
  let over = keys.length - MODEL_META_MAX;
  for (const k of removable) {
    if (over <= 0) break;
    delete META.models[k];
    over -= 1;
  }
}

function record(modelId, info) {
  META.models[modelId] = { ...(META.models[modelId] || {}), ...info, lastUsed: Date.now() };
  pruneModelMeta();
  META.history.unshift({ ts: Date.now(), model: modelId, ...info });
  if (META.history.length > 100) META.history.length = 100;
  if (info.account) {
    META.stats = META.stats || {};
    const st = (META.stats[info.account] ||= { requests: 0, lastUsed: 0, lastError: null });
    st.requests += 1;
    st.lastUsed = Date.now();
    st.lastError = info.error || null;
  }
  saveMeta();
}

// ---------- 聊天代理 ----------
const CHAT_PATHS = new Set(['/chat/completions', '/v1/chat/completions', '/api/v1/chat/completions']);

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 50 * 1024 * 1024) { reject(new Error('body too large')); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function unwrap(json, httpStatus = 200) {
  const d = json?.data && json.data.choices ? json.data : json;
  if (d?.error && !d?.choices) {
    const msg = typeof d.error === 'string' ? d.error : JSON.stringify(d.error);
    // 上游 HTTP 状态要原样保留：网关限流时是 429，早期实现一律写 502，
    // 导致客户端把「限流」误判成「上游错误」而看不到 Retry-After 语义。
    const status = httpStatus !== 200 ? httpStatus : (/model not found/i.test(msg) ? 404 : 502);
    return { status, body: { error: { message: msg, type: 'upstream_error' } }, routing: {} };
  }
  const r = parseRouting(d);
  return { status: 200, body: d, routing: r };
}

// 按管道注入上游偏好（实测结论）：
// - 规划器管道（Vercel AI Gateway）：顶层 provider 简写里的 only/order 会被 Cline 吞掉，
//   必须用 providerOptions.gateway.{only,order,sort}；流式同样生效。
// - 直连管道（OpenRouter）：顶层 provider.{only,order,sort} 生效；providerOptions 被忽略。
// - 管道未知时两种形式同时注入，各自取用、互不干扰。
const OR_SORT = { cost: 'price', ttft: 'latency', tps: 'throughput' };

// upstream: 本次尝试钉住的上游（null=自动）；orderRest: preferred 模式下排在当前上游之后的回退序列；
// excludeList: 排除列表。网关不支持 exclude/ignore 字段（实测被静默忽略），因此排除统一换算成 only 白名单：
// 自动模式 only=已知上游-排除；preferred 钉住模式 order=[当前,...] 且 only=已知上游-排除（防止网关回退到被排除渠道）；
// 严格钉住模式 only=[当前上游]，天然排除其他一切渠道。
function injectPrefs(body, modelId, { upstream, orderRest = [], excludeList = [], strict = true, sort = null }) {
  const b = JSON.parse(JSON.stringify(body));
  const exclude = (excludeList || []).filter((u) => u !== upstream);
  const meta = META.models[modelId] || {};
  const known = meta.upstreams || [];
  const allowList = exclude.length ? known.filter((u) => !exclude.includes(u)) : null;
  if (!upstream && !sort && !(allowList && allowList.length)) return b;
  const pipeline = meta.pipeline || null;
  const useVercel = pipeline === 'planner' || pipeline === null;
  const useOpenRouter = pipeline === 'direct' || pipeline === null;
  if (useVercel) {
    const gw = {};
    if (upstream) {
      if (strict) gw.only = [upstream];
      else {
        gw.order = [upstream, ...orderRest];
        if (allowList && allowList.length) gw.only = allowList;
      }
    } else if (allowList && allowList.length) {
      gw.only = allowList;
    }
    if (sort) gw.sort = sort;
    b.providerOptions = { ...(b.providerOptions || {}), gateway: { ...(b.providerOptions?.gateway || {}), ...gw } };
  }
  if (useOpenRouter) {
    const p = { ...(b.provider || {}) };
    if (upstream) {
      if (strict) p.only = [upstream];
      else {
        p.order = [upstream, ...orderRest];
        if (allowList && allowList.length) p.only = allowList;
      }
    } else if (allowList && allowList.length) {
      p.only = allowList;
    }
    if (sort) p.sort = OR_SORT[sort] || sort;
    b.provider = p;
  }
  return b;
}

// 由 perModel 配置展开出故障转移候选序列：[{ upstream, orderRest, excludeList, strict, sort }, ...]
// - 勾选了上游（排除后非空）：逐个尝试，排除的永不在候选中
// - 未勾选：单候选自动模式，排除换算成 only 白名单注入（见 injectPrefs）
function buildAttempts(modelId, cfg) {
  const listed = (cfg?.upstreams || []).filter((u) => typeof u === 'string' && u);
  const exclude = (cfg?.exclude || []).filter((u) => typeof u === 'string' && u);
  const excl = new Set(exclude);
  const wanted = listed.filter((u) => !excl.has(u));
  const strict = (cfg?.pinMode || 'strict') === 'strict';
  const sort = cfg?.sort || null;
  const base = { strict, sort, excludeList: exclude };
  if (wanted.length) {
    // preferred 模式：当前上游排在 order 首位，其余勾选项作为网关侧回退序列；排除列表随行（限制网关回退范围）
    return wanted.map((u, i) => ({ ...base, upstream: u, orderRest: strict ? [] : wanted.filter((_, j) => j !== i) }));
  }
  return [{ ...base, upstream: null, orderRest: [], excludeList: exclude }];
}

// 把上游错误信息归一成短字符串（用于学习与尝试日志）
const errText = (e) => (e == null ? '' : typeof e === 'string' ? e : JSON.stringify(e));

// 单次向上游网关发起非流式请求；返回 { status, out, routing, netError, acc }
// acc 由调用方传入（并发闸门已经取号，整条请求固定用同一个账号）；
// 异常（网络错误/非 JSON/非 200）不抛出，由调用方决定切换
async function attemptOnce(modelId, body, attempt, acc, signal) {
  const send = injectPrefs(body, modelId, attempt);
  try {
    const res = await fetch(`${config.upstreamBase}/chat/completions`, {
      method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(send), signal,
    });
    const httpStatus = res.status;
    const text = await res.text().catch(() => '');
    let json = null;
    try { json = JSON.parse(text); } catch { /* 上游限流常返回 HTML 错误页，按文本兜底 */ }
    const retryAfter = res.headers.get('retry-after') || null;
    if (!json) {
      const msg = text.slice(0, 400) || `HTTP ${httpStatus}`;
      // 关键：非 JSON 也保留真实 HTTP 状态（429 就是 429），不能一律 502
      return { status: httpStatus, out: { error: { message: msg, type: 'upstream_error' } }, routing: {}, netError: httpStatus === 429 ? `HTTP 429` : null, acc, retryAfter };
    }
    const { status, body: out, routing } = unwrap(json, httpStatus);
    return { status, out, routing, netError: null, acc, retryAfter };
  } catch (e) {
    return { status: 502, out: { error: { message: `upstream fetch failed: ${e.message}`, type: 'upstream_error' } }, routing: {}, netError: e.message, acc };
  }
}

// 顺序故障转移：依次执行候选，非 200 / 网络失败 / 超时即切换下一个；全部失败返回最后一次结果。
// 流式：首包前（网关以 JSON 而非 SSE 应答错误）仍可切换；SSE 一旦开始即透传，无法重试。
// 每次尝试有独立的超时中止（attemptTimeoutMs）；客户端断开会中止当前尝试。
// 返回 { status, out, routing, acc, trace, streamUp? } —— trace 为逐次尝试 [{ upstream, status, ms, note }]
// account：并发闸门本次取到的账号（整条请求的所有尝试固定用它）；不传时退回同步选号（管理路径）
async function runChatChain(req, body, modelId, cfg, { stream = false, attemptTimeoutMs = 120000, account = null } = {}) {
  const attempts = buildAttempts(modelId, cfg);
  const trace = [];
  const t0 = Date.now();
  let last = null;
  let activeCtrl = null;            // 当前尝试的 AbortController；流式成功后保持指向该次 fetch，用于断连时中止上游 body
  let keepCloseHook = false;        // 流式 SSE 建立后，close 钩子要保留到流结束
  const onClientClose = () => { if (activeCtrl) activeCtrl.abort(); };
  req.on('close', onClientClose);
  try {
    for (const attempt of attempts) {
      const t1 = Date.now();
      const ctrl = new AbortController();
      activeCtrl = ctrl;
      const timer = setTimeout(() => ctrl.abort(), attemptTimeoutMs);
      try {
        if (stream) {
          const send = injectPrefs(body, modelId, attempt);
          const acc = account || pickAccount();
          let up = null;
          let netError = null;
          try {
            up = await fetch(`${config.upstreamBase}/chat/completions`, { method: 'POST', headers: chatHeaders(acc.key), body: JSON.stringify(send), signal: ctrl.signal });
          } catch (e) { netError = e.message; }
          const ctype = up?.headers?.get('content-type') || '';
          let isSSE = !!up && up.status === 200 && ctype.includes('event-stream');
          // 网关对流式错误可能返回 200 + text/event-stream，body 却是 {"error":...}：
          // 读首个数据块探测，真正的 SSE 第一行是 "data: {...}" 且非纯错误对象
          let firstChunk = null;
          if (isSSE) {
            let reader = null;
            try {
              reader = up.body.getReader();
              const { value, done } = await reader.read();
              if (done) {
                isSSE = false;
                netError = 'empty stream';
              } else {
                firstChunk = Buffer.from(value);
                const head = firstChunk.toString('utf8').trimStart().slice(0, 200);
                if (head.startsWith('data:')) {
                  const payload = head.replace(/^data:\s*/, '').slice(0, 160);
                  if (payload.startsWith('{"error"')) { isSSE = false; netError = `stream error: ${payload.slice(0, 120)}`; }
                } else {
                  isSSE = false;
                  netError = `unexpected stream head: ${head.slice(0, 60)}`;
                }
              }
            } catch (e) {
              isSSE = false;
              netError = e.message;
            } finally {
              try { reader?.releaseLock(); } catch {}
            }
          }
          const ms = Date.now() - t1;
          if (up && !isSSE) {
            let text = '';
            let json = null;
            if (firstChunk) {
              // 已消费的块 + 剩余 body 拼回完整错误文本
              const rest = await up.text().catch(() => '');
              text = firstChunk.toString('utf8') + rest;
            } else {
              text = await up.text().catch(() => '');
            }
            try { json = JSON.parse(text); } catch {}
            const msg = errText(json?.error) || text.slice(0, 160) || netError;
            trace.push({ upstream: attempt.upstream, status: up.status, ms, note: msg.slice(0, 160) });
            if (attempt.upstream) learnUpstreamStatus(modelId, attempt.upstream, msg);
            if (!attempt.upstream && (attempt.excludeList || []).length) learnAvailableProviders(modelId, msg);
            // 原样保留上游 HTTP 状态（429 就是 429，不能被抹成 502），并标注限流便于上层退避重试
            last = {
              status: up.status,
              out: json || { error: { message: text.slice(0, 400) || netError, type: 'upstream_error' } },
              routing: parseRouting(json || {}),
              acc,
              netError: up.status === 429 ? 'HTTP 429' : null,
              retryAfter: up.headers.get('retry-after') || null,
            };
            continue; // 错误：还未向客户端写任何字节，可切换下一候选
          }
          if (!up) {
            trace.push({ upstream: attempt.upstream, status: 502, ms, note: netError || 'no response' });
            last = { status: 502, out: { error: { message: `upstream fetch failed: ${netError || 'no response'}`, type: 'upstream_error' } }, routing: {}, acc, netError: netError || 'no response' };
            continue;
          }
          // 真 SSE：firstChunk 与剩余 body 串联透传（SSE 开始后无法重试）；close 钩子保留用于客户端断开时中止上游
          keepCloseHook = true;
          trace.push({ upstream: attempt.upstream, status: 200, ms, note: 'stream' });
          return { status: 200, streamUp: up, streamHead: firstChunk, acc, trace, t0 };
        }
        // 非流式
        const r = await attemptOnce(modelId, body, attempt, account || pickAccount(), ctrl.signal);
        const ms = Date.now() - t1;
        const note = r.netError || (r.status !== 200 ? errText(r.out?.error?.message).slice(0, 160) : 'ok');
        trace.push({ upstream: attempt.upstream, status: r.status, ms, note });
        if (r.status !== 200 && attempt.upstream) learnUpstreamStatus(modelId, attempt.upstream, errText(r.out?.error?.message));
        if (r.status !== 200 && !attempt.upstream && (attempt.excludeList || []).length) learnAvailableProviders(modelId, r.netError || note);
        last = r;
        if (r.status === 200) break;
      } finally {
        clearTimeout(timer);
      }
    }
  } finally {
    if (!keepCloseHook) req.off('close', onClientClose);
  }
  return { ...last, status: last?.status ?? 502, trace, t0, netError: last?.netError || null };
}

const sleep = (ms) => (ms > 0 ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve());

// 上游 429 判定：HTTP 状态或错误文本命中都算限流（网关限流时常常回 HTML 错误页，文本里带 429）
function isRateLimited(chain) {
  if (!chain) return false;
  if (chain.status === 429) return true;
  const msg = errText(chain.out?.error?.message ?? chain.out?.error);
  return /(^|\D)429(\D|$)|too many requests|rate[-\s]?limit/i.test(msg);
}
// 指数退避 + 抖动：多路请求被同时限流时错开回撞时间
function backoffDelayMs(round, base, max) {
  if (base <= 0) return 0;
  return Math.round(Math.min(max, base * 2 ** round) * (0.8 + Math.random() * 0.4));
}

async function handleChat(req, res) {
  const t0req = Date.now();   // 整条请求的起点：耗时/排队/退避都要算进去，历史里才看得出真实等待
  const raw = await readBody(req);
  let body;
  try { body = JSON.parse(raw.toString('utf8')); } catch { return sendJSON(res, 400, { error: { message: 'invalid JSON body' } }); }
  const modelId = body.model;
  if (!modelId) return sendJSON(res, 400, { error: { message: 'model is required' } });

  const cfg = config.perModel[modelId] || {};
  const targets = buildAttempts(modelId, cfg).map((a) => a.upstream).filter(Boolean);
  const isStream = !!body.stream;

  // 1) 取号：有空闲名额立即开工；并发满则排队等待（本地绝不产生 429），排队超时才回 503。
  // 2) 上游真回 429：先归还名额（让排在后面的请求先跑，不空占并发）→ 退避 → 重新取号重试；
  //    重试用尽仍是 429 才如实透传（附 Retry-After）。
  const { retries, baseDelayMs, maxDelayMs, totalTimeoutMs } = retry429Plan();
  const t0retry = Date.now();     // 429 重试预算的起点
  let lastBackoffMs = 0;          // 最后一次退避的实际时长（用于给客户端一个诚实的 Retry-After）
  let lease = null;
  let chain = null;
  let queueMs = 0;
  let rateRetries = 0;
  const allTrace = [];            // 逐轮累计：chain.trace 只含最后一轮，直接用会丢掉前几轮的尝试路径
  for (let round = 0; ; round++) {
    const q0 = Date.now();
    try {
      lease = await acquireSlot(req, res, queueTimeout());
    } catch (e) {
      if (e?.code === 'CLIENT_CLOSED') { try { res.destroy(); } catch {} return; }
      const type = e?.code === 'QUEUE_TIMEOUT' ? 'queue_timeout' : e?.code === 'BUSY' ? 'pool_busy' : 'no_account';
      return sendJSON(res, 503, { error: { message: `本地账号并发闸门：${e.message}`, type } });
    }
    queueMs += Date.now() - q0;
    // 取到号之后客户端可能已经走了（排队期间断开，或 429 退避等待期间断开）：
    // 归还名额直接收工，别替一个已经离开的客户端去打上游、白烧额度。
    if (res.destroyed || req.socket?.destroyed) {
      releaseSlot(lease); lease = null;
      try { res.destroy(); } catch {}
      return;
    }
    // 每次尝试的超时也要受总预算约束：剩余预算不足时不再给满 120s，避免一条请求拖到分钟级
    const budgetLeft = Math.max(5000, totalTimeoutMs - (Date.now() - t0retry));
    try {
      chain = await runChatChain(req, body, modelId, cfg, {
        stream: isStream, account: lease, attemptTimeoutMs: Math.min(120000, budgetLeft),
      });
    } catch (e) {
      releaseSlot(lease); lease = null;
      return sendJSON(res, 502, { error: { message: `proxy error: ${e.message}`, type: 'proxy_error' } });
    }
    if (chain.trace) allTrace.push(...chain.trace.map((t) => ({ ...t, round })));
    if (!isRateLimited(chain) || round >= retries) break;
    rateRetries += 1;
    const wait = backoffDelayMs(round, baseDelayMs, maxDelayMs);
    // 预算不够再等一轮就放弃重试，把最后一次的 429 如实返回（附上游/本次退避的 Retry-After）
    if (Date.now() - t0retry + wait > totalTimeoutMs) {
      console.warn(`[限流退避] ${modelId} 429 重试预算 ${totalTimeoutMs}ms 用尽，停止重试并透传 429`);
      break;
    }
    lastBackoffMs = wait;
    releaseSlot(lease); lease = null;
    console.warn(`[限流退避] ${modelId} 上游 429，第 ${rateRetries}/${retries} 次重试将于 ${wait}ms 后发起`);
    await sleep(wait);
  }
  const acc = chain.acc || lease;
  const accountLoad = () => headerSafe(`${activeOf(lease.name)}/${lease.concurrency}`);

  if (isStream && chain.streamUp) {
    // 流式透传：先写已探测的首块，再接剩余 body；tap 在结束时回读路由元数据并记录
    const up = chain.streamUp;
    const ctype = up.headers.get('content-type') || 'text/event-stream';
    // 并发名额必须持有到流结束：上游连接还开着，这条请求仍在占用该账号的并发额度
    let released = false;
    const releaseOnce = () => { if (!released) { released = true; releaseSlot(lease); } };
    try {
      res.writeHead(up.status, {
        'Content-Type': ctype,
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'Access-Control-Allow-Origin': '*',
        'X-Cline-Target-Upstream': targets.length ? targets.join('>') : 'auto',
        'X-Cline-Attempts': String(allTrace.length),
        'X-Cline-Account': headerSafe(acc.name),
        'X-Cline-Account-Load': accountLoad(),
        'X-Cline-Queue-Ms': String(queueMs),
        'X-Cline-RateLimit-Retries': String(rateRetries),
      });
      if (chain.streamHead) res.write(chain.streamHead);
    } catch (e) {
      // 客户端在建流前就断开：不抛出去（否则名额可能泄漏），顺手把上游 body 收掉
      releaseOnce();
      try { up.body?.cancel?.(); } catch {}
      try { res.destroy(); } catch {}
      return;
    }
    const buf = [];
    const tap = new Transform({
      transform(c, enc, cb) { buf.push(c); cb(null, c); },
      flush(cb) {
        const text = Buffer.concat(buf).toString('utf8');
        let provider = null, canonical = null;
        // direct 管道：最后一个 chunk 顶层带 provider（显示名）与 model
        const lines = text.split('\n');
        for (let i = lines.length - 1; i >= 0 && i >= lines.length - 10 && !provider; i--) {
          const l = lines[i];
          if (!l.startsWith('data: ') || l.includes('[DONE]')) continue;
          try {
            const c = JSON.parse(l.slice(6));
            if (typeof c.provider === 'string') { provider = slugify(c.provider); canonical = c.model || null; }
          } catch { /* 跳过不完整行 */ }
        }
        // planner 管道：final chunk 的 provider_metadata.gateway.routing
        if (!provider) {
          const fp = /"finalProvider":"([^"]+)"/.exec(text);
          const cs = /"canonicalSlug":"([^"]+)"/.exec(text);
          provider = fp ? fp[1] : null;
          canonical = cs ? cs[1] : null;
        }
        record(modelId, {
          provider, canonical, ms: Date.now() - t0req, stream: true, error: null, account: acc.name,
          attempts: allTrace.map((t) => t.upstream || 'auto'),
          traceAll: allTrace,
          queuedMs: queueMs, rateRetries,
        });
        cb();
      },
    });
    // 上游 body 出错必须就地消化：pipe() 只转发数据、不转发 error，
    // 未被监听的 'error' 事件会被 Node 升级成未捕获异常，直接终止整个进程。
    // 实测两种触发源：上游/本地代理断链（UND_ERR_SOCKET），以及 undici 默认 bodyTimeout（300s 无数据）。
    const src = Readable.fromWeb(up.body);
    src.on('error', (e) => {
      record(modelId, {
        provider: null, canonical: null, ms: Date.now() - t0req, stream: true,
        error: `stream aborted: ${e.message}`, account: acc.name,
        attempts: allTrace.map((t) => t.upstream || 'auto'),
        traceAll: allTrace,
        queuedMs: queueMs, rateRetries,
      });
      releaseOnce();
      try { res.destroy(); } catch {}
    });
    // 下游（客户端）中断会让 res 抛 error / close，一并把上游 body 收掉，避免悬挂连接
    // finish = 流正常推完；close = 客户端提前断开。两条路径都要归还并发名额（releaseOnce 幂等）。
    res.on('finish', releaseOnce);
    res.on('error', () => { try { src.destroy(); } catch {} });
    res.on('close', () => { try { src.destroy(); } catch {} releaseOnce(); });
    src.pipe(tap).pipe(res);
    return;
  }

  const { status, out, routing } = chain;
  // try/finally 是并发闸门的硬要求：无论写响应是否成功（客户端可能已断开导致抛错），
  // 名额都必须归还，否则一条异常请求就能把该账号的并发永久占死。
  try {
    if (!out) return sendJSON(res, 502, { error: { message: 'no upstream response', type: 'upstream_error' } });
    // 客户端实际使用成功的新订阅模型自动收录进列表
    if (status === 200 && /^cline-pass\//.test(String(modelId)) && !config.knownModels.includes(modelId)) {
      config.knownModels.push(modelId);
      saveConfig();
    }
    record(modelId, {
      provider: routing.finalProvider || null,
      canonical: routing.canonicalSlug || null,
      ms: Date.now() - t0req,   // 含排队 + 429 退避的真实总耗时
      stream: false,
      attempts: allTrace.map((t) => t.upstream || 'auto'),
      trace: allTrace,
      error: status !== 200 ? out?.error?.message || null : null,
      account: acc ? acc.name : null,
      queuedMs: queueMs,
      rateRetries,
    });
    const headers = {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'X-Cline-Target-Upstream': targets.length ? targets.join('>') : 'auto',
      'X-Cline-Actual-Upstream': routing.finalProvider || 'unknown',
      'X-Cline-Canonical-Model': routing.canonicalSlug || '',
      'X-Cline-Attempts': String(allTrace.length),
      'X-Cline-Account': headerSafe(acc ? acc.name : ''),
      'X-Cline-Account-Load': accountLoad(),
      'X-Cline-Queue-Ms': String(queueMs),
      'X-Cline-RateLimit-Retries': String(rateRetries),
    };
    // 本地闸门不会产生 429；走到这里的 429 只可能是上游真的限流，如实透传并给出 Retry-After。
    // 优先用上游自己给的值；没有就用本次真实退避量，而不是配置上限 maxDelayMs（那会明显偏大）。
    const ra = chain.retryAfter || (lastBackoffMs ? Math.ceil(lastBackoffMs / 1000) : 1);
    if (status === 429) headers['Retry-After'] = String(Math.max(1, Math.min(600, Number(ra) || 1)));
    res.writeHead(status, headers);
    res.end(JSON.stringify(out));
  } finally {
    releaseSlot(lease);
  }
}

// ---------- HTTP 服务 ----------
// no-store 是必须的：/api/runtime 这类轮询接口如果被浏览器按启发式缓存下来，
// 控制台会一直显示同一份旧数据（实测 Chrome 会缓存没有 Cache-Control 的 GET）。
// 另外整体 try 住：客户端可能已经断开，往死 socket 写会抛错，代理不该为这个报 500 或产生未处理拒绝。
function sendJSON(res, status, obj) {
  try {
    if (res.writableEnded || res.destroyed) return;
    res.writeHead(status, {
      'Content-Type': 'application/json; charset=utf-8',
      'Cache-Control': 'no-store',
      'Access-Control-Allow-Origin': '*',
    });
    res.end(JSON.stringify(obj));
  } catch { /* 客户端已断开：忽略 */ }
}

async function catalog() {
  if (META.catalog && Date.now() - (META.catalogFetchedAt || 0) < 3600e3) return META.catalog;
  const { json } = await fetchJSON(`${config.upstreamBase}/models`, { headers: chatHeaders(pickAccount().key) });
  const ids = (json?.data || []).map((m) => m.id);
  if (ids.length) { META.catalog = ids; META.catalogFetchedAt = Date.now(); saveMeta(); }
  return META.catalog || [];
}

// ---------- 订阅额度（Cline Pass 滚动窗口用量） ----------
// Cline 官方账单接口，sk_ 开头的 API Key 可直接鉴权（无需 OAuth 登录）：
//   GET /users/me/plan/usage-limits → limits[] = { type: five_hour|weekly|monthly, percentUsed, resetsAt }
//   GET /users/me/plan              → 套餐名、当期起止、上限阈值 entitlements.cline_pass.inferenceCapThreshold
// 这两条都是纯账单查询：不触发推理、不消耗额度、不计费（与「探测/测试/校验」完全不同）。
// 金额单位：costUsd 与阈值同为 1e-8 美元（对 /usages 明细做最小二乘拟合验证：
// 未缓存输入 $0.15/M、缓存 $0.003/M、输出 $0.60/M，比例符合标准价目形态）。
const MU_PER_USD = 1e8;
const QUOTA_WINDOWS = [
  ['five_hour', '5小时', 'last5HoursUsageCostUSDPerUser'],
  ['weekly', '本周', 'last7daysUsageCostUSDPerUser'],
  ['monthly', '本月', 'last30daysUsageCostUSDPerUser'],
];

async function fetchAccountQuota(key) {
  const headers = chatHeaders(key);
  const [lim, plan] = await Promise.all([
    fetchJSON(`${config.upstreamBase}/users/me/plan/usage-limits`, { headers }, 15000),
    fetchJSON(`${config.upstreamBase}/users/me/plan`, { headers }, 15000),
  ]);
  if (lim.status !== 200 || !Array.isArray(lim.json?.data?.limits)) {
    const raw = lim.json?.error?.message || lim.json?.error || `HTTP ${lim.status}`;
    return { ok: false, error: String(typeof raw === 'string' ? raw : JSON.stringify(raw)).slice(0, 200) };
  }
  const byType = {};
  for (const it of lim.json.data.limits) if (it && it.type) byType[it.type] = it;
  const planData = plan.status === 200 ? plan.json?.data : null;
  const cap = planData?.plan?.entitlements?.cline_pass?.inferenceCapThreshold || {};
  // 注意 Number(null)===0、Number('')===0，会把「没有这个字段」误判成 0，先挡掉空值
  const num = (v) => (v === null || v === undefined || v === '' ? null : (Number.isFinite(Number(v)) ? Number(v) : null));
  return {
    ok: true,
    planName: planData?.plan?.displayName || null,
    planActive: planData?.plan?.isActive ?? null,
    periodStart: planData?.currentPeriodStart || null,
    periodEnd: planData?.currentPeriodEnd || null,
    cancelAt: planData?.cancelAt || null,   // 非空 = 当期结束后不再续费
    windows: QUOTA_WINDOWS.map(([type, label, capKey]) => {
      const w = byType[type] || {};
      const capMu = num(cap[capKey]);
      return {
        type,
        label,
        percent: num(w.percentUsed),
        resetsAt: w.resetsAt || null,
        limitUsd: capMu === null ? null : +(capMu / MU_PER_USD).toFixed(2),
      };
    }),
  };
}

// 逐账号按 key 查自己的额度（/users/me/* 是「key 所属账号」，多账号必须逐把查），带 TTL 缓存。
// 缓存同时供两处使用：
//   - 控制台额度表与加权选号：TTL 都取 quotaRefreshMs（默认 15 分钟），见 quotaTtlMs()
//   - 「↻ 刷新额度」按钮走 GET /api/quota?refresh=1 强制立刻重拉，不受 TTL 限制
//   - 加权选号：只读缓存、绝不在请求路径上打网络；过期时后台补一次（见 quotaPeek）
// inflight 用来合并并发触发的重复抓取，避免同一时刻把账单接口打爆。
const quotaCache = { at: 0, accounts: [], inflight: null };
// 额度快照的有效期：统一由 quotaRefreshMs 决定（默认 15 分钟）。
// 控制台额度表、加权选号、/api/quota 三处共用同一个 TTL —— 额度是账单接口，
// 没必要时时刻刻去拉；要立刻看最新的就点控制台的「↻ 刷新额度」（走 ?refresh=1 强制）。
function quotaTtlMs() {
  return clampInt(config.quotaRefreshMs, 0, 24 * 3600e3, 900000);
}
async function quotaSnapshot({ force = false, ttlMs = quotaTtlMs() } = {}) {
  if (!force && quotaCache.accounts.length && Date.now() - quotaCache.at < ttlMs) return quotaCache;
  if (quotaCache.inflight) return quotaCache.inflight;
  const p = (async () => {
    const list = config.accounts || [];
    const accounts = await Promise.all(list.map(async (a, i) => {
      const name = a?.name || `账号${i + 1}`;
      const enabled = a?.enabled !== false;
      if (!a?.key) return { name, enabled, ok: false, error: '未配置 key' };
      try {
        return { name, enabled, ...(await fetchAccountQuota(a.key)) };
      } catch (e) {
        return { name, enabled, ok: false, error: String(e?.message || e).slice(0, 200) };
      }
    }));
    quotaCache.at = Date.now();
    quotaCache.accounts = accounts;
    quotaCache.inflight = null;
    return quotaCache;
  })().catch((e) => { quotaCache.inflight = null; throw e; });
  quotaCache.inflight = p;
  return p;
}

// 选号取额度：只读缓存。缓存超过 quotaRefreshMs 就丢一个后台刷新（不 await，不阻塞本次请求）
function quotaPeek() {
  const stale = !quotaCache.accounts.length || Date.now() - quotaCache.at >= quotaTtlMs();
  if (stale && !quotaCache.inflight && (config.accounts || []).some((a) => a && a.key)) {
    quotaSnapshot({ force: true }).catch(() => { /* 账单接口抖动不影响代理 */ });
  }
  return quotaCache;
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const p = url.pathname;
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
      'Access-Control-Allow-Headers': '*',
    });
    return res.end();
  }
  try {
    if (req.method === 'GET' && p === '/api/meta') {
      return sendJSON(res, 200, { authRequired: !!PROXY_KEY, proxyBase: publicProxyBase(), configured: isConfigured() });
    }
    if (p.startsWith('/api/') || p.startsWith('/v1/') || CHAT_PATHS.has(p)) {
      if (!authOK(req)) return unauthorized(res);
    }
    if (req.method === 'GET' && (p === '/' || p === '/index.html')) {
      // 控制台页面同样不缓存：升级代理代码后，刷新就能拿到新界面，不用清浏览器缓存
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
      return res.end(fs.readFileSync(path.join(PUBLIC_DIR, 'index.html')));
    }
    if (req.method === 'GET' && p === '/api/models') {
      const cat = await catalog();
      const sub = config.knownModels.map((id) => ({ id, config: config.perModel[id] || {}, meta: META.models[id] || null }));
      return sendJSON(res, 200, { subscription: sub, catalogCount: cat.length, catalog: cat, proxyBase: publicProxyBase(), officialFetch: META.officialModelsFetch || null });
    }
    if (req.method === 'POST' && p === '/api/probe') {
      const { model } = await JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: 'model required' });
      const r = await probeModel(model);
      return sendJSON(res, r.ok ? 200 : 502, r);
    }
    if (req.method === 'POST' && p === '/api/test') {
      // 临时配置可带 upstreams/exclude（数组）或旧版 upstream（单值），完整走故障转移链路
      const { model, upstream, upstreams, exclude } = await JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: 'model required' });
      const t0 = Date.now();
      const cfg = { ...(config.perModel[model] || {}) };
      if (upstreams !== undefined) cfg.upstreams = upstreams;
      else if (upstream !== undefined) cfg.upstreams = upstream ? [upstream] : [];
      if (exclude !== undefined) cfg.exclude = exclude;
      const body = { model, messages: [{ role: 'user', content: 'Reply with the word OK' }], max_tokens: 256 };
      const chain = await runChatChain(req, body, model, cfg, { stream: false, attemptTimeoutMs: 180000 });
      const trace = chain.trace || [];
      if (chain.status !== 200) {
        return sendJSON(res, 200, {
          ok: false, error: (chain.out?.error?.message || 'upstream error').slice?.(0, 400) || 'upstream error',
          targets: (cfg.upstreams || []).filter(Boolean), exclude: cfg.exclude || [], trace,
        });
      }
      const r = parseRouting(chain.out);
      record(model, { provider: r.finalProvider, canonical: r.canonicalSlug, ms: Date.now() - t0, stream: false, attempts: trace.map((t) => t.upstream || 'auto'), error: null, account: chain.acc?.name || null });
      return sendJSON(res, 200, {
        ok: true, ms: Date.now() - t0,
        targets: (cfg.upstreams || []).filter(Boolean), exclude: cfg.exclude || [],
        actual: r.finalProvider, actualName: r.finalProviderName, pipeline: r.pipeline, pinnable: r.pipeline !== null,
        canonicalSlug: r.canonicalSlug, fallbacks: r.fallbacks, content: (r.content || '').slice(0, 120),
        account: chain.acc?.name || null, trace,
      });
    }
    if (req.method === 'GET' && p === '/api/accounts') {
      return sendJSON(res, 200, {
        accounts: config.accounts,
        mode: config.accountMode,
        active: config.activeAccount,
        stats: META.stats || {},
        tuning: {
          accountConcurrency: clampInt(config.accountConcurrency, 1, 1000, 5),
          poolConcurrency: clampInt(config.poolConcurrency, 0, 10000, 0),
          queueTimeoutMs: queueTimeout(),
          retry429: retry429Plan().retries,
        },
        runtime: poolSnapshot(),
      });
    }
    // 轻量运行态：控制台轮询用（不含密钥，只报并发占用与各账号当前有效权重）
    if (req.method === 'GET' && p === '/api/runtime') {
      return sendJSON(res, 200, poolSnapshot());
    }
    if (req.method === 'GET' && p === '/api/quota') {
      // 默认走 15 分钟 TTL 缓存；refresh=1（页面上的「↻ 刷新额度」）强制立刻重拉
      const force = url.searchParams.get('refresh') === '1';
      const snap = await quotaSnapshot({ force });
      return sendJSON(res, 200, { fetchedAt: snap.at, accounts: snap.accounts, ttlMs: quotaTtlMs(), forced: force });
    }
    if (req.method === 'POST' && p === '/api/accounts') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      const defConc = clampInt(body?.tuning?.accountConcurrency, 1, 1000, clampInt(config.accountConcurrency, 1, 1000, 5));
      // 账号名是并发计数、统计、额度对齐的共同键：重名会让两个账号共用同一份计数，必须在入口去重。
      // 用「已占用集合 + 循环找空位」而不是简单计数，避免生成的名字又撞上已存在的名字。
      const taken = new Set();   // 只记录「本次结果里已经用掉的名字」，不能拿输入去种，否则每个账号都会被改名
      const uniqName = (n) => {
        if (!taken.has(n)) { taken.add(n); return n; }
        for (let k = 2; ; k++) {
          const cand = `${n} (${k})`;
          if (!taken.has(cand)) { taken.add(cand); return cand; }
        }
      };
      const accs = (Array.isArray(body.accounts) ? body.accounts : [])
        .map((a, i) => ({
          name: uniqName(String(a.name || `账号${i + 1}`).slice(0, 50)),
          key: String(a.key || '').trim(),
          enabled: a.enabled !== false,
          // 并发与权重必须原样保留：旧版保存逻辑只留 name/key/enabled，会把这两个新增字段抹掉
          concurrency: clampInt(a.concurrency, 1, 1000, defConc),
          weight: Math.round(clampNum(a.weight, 0, 100, 1) * 100) / 100,
        }))
        .filter((a) => a.key);
      if (!accs.length) return sendJSON(res, 400, { error: { message: '至少需要一个有效账号（key 非空）' } });
      config.accounts = accs;
      config.accountMode = ACCOUNT_MODES.has(body.mode) ? body.mode : 'single';
      config.activeAccount = Math.min(Math.max(0, Number(body.active) || 0), accs.length - 1);
      // 闸门调优参数（默认并发 / 池总并发 / 排队超时 / 429 重试次数）
      const t = body.tuning || {};
      if (t.accountConcurrency !== undefined) config.accountConcurrency = defConc;
      if (t.poolConcurrency !== undefined) config.poolConcurrency = clampInt(t.poolConcurrency, 0, 10000, config.poolConcurrency);
      if (t.queueTimeoutMs !== undefined) config.queueTimeoutMs = clampInt(t.queueTimeoutMs, 0, 3600e3, config.queueTimeoutMs);
      if (t.retry429 !== undefined) config.retry429 = { ...(config.retry429 || {}), retries: clampInt(t.retry429, 0, 10, retry429Plan().retries) };
      saveConfig();
      RR_COUNTER = 0;
      quotaCache.at = 0; quotaCache.accounts = [];   // 账号池变了，额度缓存作废（否则会串号）
      // 后台立刻按新账号池重拉一次额度：加权选号和 /api/runtime 的权重都要用它，
      // 不 await，避免保存动作被账单接口的耗时拖住
      quotaSnapshot({ force: true }).catch(() => { /* 账单接口抖动不影响保存 */ });
      // 已删除账号的并发计数要清掉，避免残留计数把新账号的名额算错
      const names = new Set(config.accounts.map((a) => a.name));
      for (const n of [...pool.active.keys()]) if (!names.has(n)) pool.active.delete(n);
      return sendJSON(res, 200, { ok: true, accounts: config.accounts.length, mode: config.accountMode, active: config.activeAccount, tuning: { accountConcurrency: config.accountConcurrency, poolConcurrency: config.poolConcurrency, queueTimeoutMs: queueTimeout(), retry429: retry429Plan().retries } });
    }
    if (req.method === 'POST' && p === '/api/accounts/test') {
      const { key } = JSON.parse(await readBody(req).then((b) => b.toString()));
      const k = String(key || '').trim();
      if (!k) return sendJSON(res, 400, { error: { message: 'key required' } });
      const t0 = Date.now();
      const model = config.knownModels[0] || 'cline-pass/glm-5.3-flash';
      const { json } = await fetchJSON(`${config.upstreamBase}/chat/completions`, {
        method: 'POST',
        headers: chatHeaders(k),
        body: JSON.stringify({ model, messages: [{ role: 'user', content: 'Say OK' }], max_tokens: 512 }),
      }, 120000);
      if (json?.error && !json?.data) {
        const msg = typeof json.error === 'string' ? json.error : JSON.stringify(json.error);
        const authFail = /unauthorized|re-authenticate|invalid\s*api|401/i.test(msg);
        // 密钥无效会直接 Unauthorized；其他错误（如推理模型耗尽 max_tokens 的 empty response）
        // 说明鉴权已通过，不应误报为密钥问题
        return sendJSON(res, 200, authFail
          ? { ok: false, ms: Date.now() - t0, error: `密钥无效或未授权：${msg.slice(0, 160)}` }
          : { ok: true, ms: Date.now() - t0, model, note: `密钥鉴权通过；网关提示：${msg.slice(0, 120)}` });
      }
      return sendJSON(res, 200, { ok: true, ms: Date.now() - t0, model });
    }
    if (req.method === 'GET' && p === '/api/security') {
      return sendJSON(res, 200, { proxyKey: config.proxyKey || '', publicBaseUrl: config.publicBaseUrl || '', authRequired: !!PROXY_KEY, exposeCatalog: !!config.exposeCatalog });
    }
    if (req.method === 'POST' && p === '/api/security') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (body.proxyKey !== undefined) config.proxyKey = String(body.proxyKey).trim();
      if (body.publicBaseUrl !== undefined) config.publicBaseUrl = String(body.publicBaseUrl).trim().replace(/\/+$/, '');
      if (body.exposeCatalog !== undefined) config.exposeCatalog = !!body.exposeCatalog;
      saveConfig();
      PROXY_KEY = config.proxyKey || '';
      return sendJSON(res, 200, { ok: true, proxyKey: config.proxyKey, publicBaseUrl: config.publicBaseUrl, authRequired: !!PROXY_KEY, proxyBase: publicProxyBase(), exposeCatalog: !!config.exposeCatalog });
    }
    if (req.method === 'POST' && p === '/api/validate-upstreams') {
      const { model } = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (!model) return sendJSON(res, 400, { error: { message: 'model required' } });
      const results = await validateUpstreams(model);
      const summary = { ok: 0, limited: 0, bad: 0, auth: 0, unknown: 0 };
      for (const r of Object.values(results)) summary[r.status] = (summary[r.status] || 0) + 1;
      return sendJSON(res, 200, { ok: true, summary, results, upstreams: META.models[model]?.upstreams || [] });
    }
    if (req.method === 'POST' && p === '/api/fetch-official-models') {
      const r = await fetchOfficialModels();
      return sendJSON(res, 200, { ok: true, ...r });
    }
    if (req.method === 'GET' && p === '/api/history') return sendJSON(res, 200, { history: META.history });
    if (req.method === 'GET' && p === '/api/config') return sendJSON(res, 200, { port: config.port, perModel: config.perModel, knownModels: config.knownModels });
    if (req.method === 'POST' && p === '/api/config') {
      const body = JSON.parse(await readBody(req).then((b) => b.toString()));
      if (body.perModel) {
        for (const [m, c] of Object.entries(body.perModel)) {
          const normalize = (v) => [...new Set((Array.isArray(v) ? v : []).map((s) => String(s).trim()).filter(Boolean))].slice(0, 10);
          let upstreams = normalize(c.upstreams);
          const exclude = normalize(c.exclude);
          const excl = new Set(exclude);
          upstreams = upstreams.filter((u) => !excl.has(u)); // 同时出现以 exclude 为准
          const upstream = upstreams[0] || null; // 旧字段兼容镜像
          config.perModel[m] = {
            upstream,
            upstreams,
            exclude,
            pinMode: c.pinMode === 'preferred' ? 'preferred' : 'strict',
            sort: ['cost', 'ttft', 'tps'].includes(c.sort) ? c.sort : null,
          };
        }
        saveConfig();
      }
      return sendJSON(res, 200, { ok: true });
    }
    if (req.method === 'GET' && (p === '/v1/models' || p === '/api/v1/models' || p === '/models')) {
      // 默认只暴露订阅模型，避免目录模型淹没客户端的模型选择器；exposeCatalog=true 时合并完整目录
      const ids = config.exposeCatalog
        ? [...new Set([...config.knownModels, ...(await catalog())])]
        : [...new Set([...config.knownModels, ...Object.keys(config.perModel)])];
      return sendJSON(res, 200, { object: 'list', data: ids.map((id) => ({ id, object: 'model' })) });
    }
    if (CHAT_PATHS.has(p) && req.method === 'POST') return handleChat(req, res);
    return sendJSON(res, 404, { error: { message: `no route: ${req.method} ${p}` } });
  } catch (e) {
    return sendJSON(res, 500, { error: { message: e.message } });
  }
});

// HTTP 响应头只允许 Latin-1，账号名里的中文等字符需要清洗（历史/统计仍用原名）
const headerSafe = (s) => String(s ?? '').replace(/[^\x20-\x7E]/g, '').trim().slice(0, 80) || '-';

server.on('error', (e) => {
  console.error(`[错误] 端口 ${config.port} 监听失败（可能被占用）：${e.message}`);
  process.exit(1);
});

const BIND_HOST = process.env.BIND_HOST || '127.0.0.1';
server.listen(config.port, BIND_HOST, () => {
  console.log(`Cline Pass 上游控制台:  http://127.0.0.1:${config.port}/`);
  console.log(`OpenAI 兼容代理地址:   http://127.0.0.1:${config.port}/v1`);
  const tun = poolSnapshot();
  console.log(`账号模式: ${config.accountMode} · 池并发上限: ${tun.limit || '不限'} · 单账号默认并发: ${clampInt(config.accountConcurrency, 1, 1000, 5)} · 排队上限: ${queueTimeout()}ms`);
  // 预热额度快照：加权选号依赖它，先拉一次，避免头几条请求都用「额度未知」的中性权重
  if (enabledAccounts().length) quotaSnapshot({ force: true }).catch(() => {});
});

// 进程级兜底：代理是常驻服务，任何未捕获异常/未处理拒绝只记录日志，不退出进程。
// 历史故障：单条流式请求的上游 body 中断触发 unhandled 'error'，把整个代理带走。
// 元数据是合并写盘的（最多 1 秒一次），收到退出信号时补一次 flush，避免丢最后一段历史
for (const sig of ['SIGINT', 'SIGTERM']) {
  process.on(sig, () => { flushMeta(); process.exit(0); });
}
process.on('uncaughtException', (e) => console.error('[未捕获异常]', e));
process.on('unhandledRejection', (e) => console.error('[未处理拒绝]', e));
