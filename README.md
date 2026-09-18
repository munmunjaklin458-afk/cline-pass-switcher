# Cline Pass 上游控制台（cline-pass-switcher）

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Node](https://img.shields.io/badge/node-%E2%89%A5%2018-green)
![Docker](https://img.shields.io/badge/docker-ready-2496ED)
![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)

**零依赖**的 Node.js 代理 + 网页控制台，面向 [Cline Pass](https://cline.bot/cline-pass) 订阅：

把订阅模型背后的**上游渠道**枚举出来、实测可用性、精确钉住；把多个 Cline Pass 账号做成一个
**带并发闸门和额度加权调度**的账号池；对下游暴露**标准 OpenAI 接口**，任何 OpenAI 客户端把 Base URL
指过来即可使用，不改客户端一行代码。

---

## 📌 仓库来源

| | 地址 |
|---|---|
| **原项目（upstream）** | <https://github.com/munmunjaklin458-afk/cline-pass-switcher> |
| **本仓库（二次开发版）** | <https://github.com/somehow-paul/cline-pass-switcher-update> |

本仓库基于原项目二次开发，**原项目功能全部保留**，另外做了三件事（详见
[「本仓库相对原项目新增的功能」](#本仓库相对原项目新增的功能)）：

1. 账号管理里增加**订阅额度显示**（5 小时 / 本周 / 本月滚动窗口）；
2. **常驻代理稳定性加固**（流式断链不再带走整个进程）；
3. 账号池增加**并发闸门 + 额度加权选号 + 上游 429 退避重试**。

钉住 / 探测 / 观测等核心机制未做破坏性改动，只在其上扩展。

---

## 功能总览

- 🔍 **上游枚举与校验** —— 列出每个订阅模型背后**每一条**上游渠道，一键实测标出「✔可用 / ⏳限流 / ✘不可钉 / 🔑key错」
- 🎯 **精确钉住上游** —— 严格钉住 / 优先+回退两种模式，支持按**最低成本 / 最快首字 / 最高吞吐**排序
- 🧬 **多上游优先级 + 故障转移** —— 勾选多个上游即按勾选顺序逐个尝试，第一个异常（报错 / 网络失败 / 超时）自动顺切下一个，全部失败才把错误透传给客户端
- 🚫 **上游排除** —— 被排除的渠道永不被使用；自动模式下会向网关注入排除偏好（已知上游 − 排除项 的白名单）
- 👥 **账号池** —— 多账号管理、手动切换、轮询均衡、逐账号连通性与用量统计
- 📈 **订阅额度显示（本仓库新增）** —— 每个账号还剩多少额度一眼可见：三条滚动窗口进度条 + 重置时间 + 套餐状态，只读查询、不计费
- ⚖️ **额度加权选号（本仓库新增）** —— 账号模式新增 `weighted`：按剩余额度加权随机选号，剩得越多越容易被抽中
- 🚦 **并发闸门（本仓库新增）** —— 单账号并发 + 池级总并发；超限**排队等名额**而不是报错，本地绝不产生 429
- ♻️ **上游 429 退避重试（本仓库新增）** —— 上游真限流时归还名额 → 指数退避 → 重新取号重试，用尽才如实透传并附 `Retry-After`
- 🛡️ **稳定性加固（本仓库新增）** —— 流式透传的上游断链 / `bodyTimeout` 就地消化在单条连接上，不再升级为进程级崩溃
- 📊 **观测** —— 每条请求记录账号、实际命中渠道、背后模型、耗时、尝试序列（含流式）
- 🔑 **代理密钥** —— 给下游客户端一把独立密钥，可随时在页面轮换
- 🌐 **OpenAI 兼容** —— 解包 Cline 的 `{"data":...}` 包装、错误归一化、附加 `X-Cline-*` 响应头

![控制台截图](docs/screenshot-top.png)

---

## 本仓库相对原项目新增的功能

### 1. 账号管理里的订阅额度显示

账号列表新增「额度（5 小时 / 本周 / 本月）」列，**每个账号各查各的**、独立成行、互不干扰：

- 三条滚动窗口各一条迷你进度条 + 已用百分比，≥50% 转橙、≥80% 转红；悬停显示上限金额与重置时间
- 上游没返回上限时显示「上限未知」，**不会假装成 0**（`Number(null) === 0` 这类坑已在后端挡掉）
- 标注重置时间、套餐名，以及当期到期时间、`到期后不续费`、套餐已失效
- 数据来自 Cline 官方账单接口（`GET /users/me/plan/usage-limits` + `GET /users/me/plan`），**纯查询：不触发推理、不消耗额度、不计费**
- 额度按**账号名**对齐而不是数组下标，在还没保存的账号表里增删行时不会串成别人的额度
- 页面提供 `↻ 刷新额度`（`GET /api/quota?refresh=1` 跳过缓存）；额度异步补渲染，既不阻塞首屏，也不会顶掉正在编辑的账号表

### 2. 常驻代理的稳定性加固

原实现下，**一条异常请求可以把整个代理进程带走，所有在线用户同时断线**。本仓库修掉了这两处：

- **流式透传异常就地消化**：原先 `pipe(up.body)` 只搬数据、不转发 `error` 事件。上游半路断链（`UND_ERR_SOCKET` / other side closed）或触发 undici 默认 300s `bodyTimeout` 时，该事件没有监听者，被 Node 升级为 `Unhandled 'error' event`，进程直接退出。现在监听上游 body 的 `error` 与响应的 `error` / `close`：
  - 上游断了 → 记录 `stream aborted` 元数据 + 主动 `destroy` 响应，客户端**立刻**收到断流，而不是挂到 300s
  - 客户端先断开 → 反向销毁上游 body，避免上游连接悬挂、长时间空占账号并发
- **非流式分支补偿**：`up.text()` 补 `.catch(() => '')`，body 读取本身失败时退化为空串，错误信息仍由 `netError` 兜底
- **进程级兜底**：`uncaughtException` / `unhandledRejection` 只记录日志、不退出进程，作为第二道保险；端口监听失败仍按原逻辑 `process.exit(1)`（服务起不来时不应假装存活）

> SSE 一旦已向客户端写出 200 与部分内容，就无法重试、也无法改写状态码。所以这里的目标是
> **让失败终止在单条连接上，而不是终止在进程上**。

### 3. 账号池并发闸门 · 额度加权选号 · 上游 429 退避重试

账号模式在原来的 `single` / `roundrobin` 之外新增 `weighted`，并把「**并发**」和「**限流**」当成两个
独立问题处理：并发是自己的事（排队解决），限流是上游的事（退避重试解决）。

- **额度加权选号（`weighted`）**
  - 有效权重 = 三窗口额度剩余按 `quotaWeights` 加权平均 **×** 账号手动 `weight`，**剩余额度越多，被抽中的概率越高**
  - 任一窗口「已用」达到 `quotaStopPercent`（默认 100）的账号退出加权；额度读不到时给 50 的中性余量，避免账单接口抖动把账号饿死
  - 全部账号都被阈值挡下时退化为等概率，保证账号池不会直接不可用
  - 权重**只在取号这一刻算一次，不绑定会话、不做粘性**；每条请求独立按权重抽一次
- **并发闸门**
  - 单账号并发：账号级 `concurrency`，缺省用 `accountConcurrency`（默认 5）；池级总并发 `poolConcurrency`（`0` = 不限）
  - 超限不报错，而是进 **FIFO 队列**排队等名额（`queueTimeoutMs`，默认 120s；`0` = 不排队、立即 503）——**本地闸门绝不产生 429，429 只可能来自上游**
  - 名额在**整条请求生命周期内持有**（含流式透传），请求结束 / 断开 / 出错都会归还；排队期间客户端断开则立刻出队，不占名额
  - 管理路径（探测 / 测试 / 校验）走同步选号，不占并发名额，避免管理操作被闸门卡住
- **上游真 429 退避重试**
  - 命中 429（HTTP 状态或错误文本，网关限流时常常返回带 429 的 HTML 错误页）→ **先归还名额**（让排在后面的请求先跑，不空占并发）→ 指数退避 + 抖动（`retry429`：`retries` 默认 3、`baseDelayMs` 1.5s 起、`maxDelayMs` 20s 封顶）→ 重新取号重试
  - 重试用尽仍是 429 才如实透传，并附 `Retry-After`；上游 HTTP 状态全程原样保留，不再把限流抹成 502
- **额度快照缓存**：`quotaRefreshMs`（默认 15 分钟）过期后只丢一个后台刷新，本次请求仍用现有缓存，账单接口抖动不影响代理转发；启动时预热一次，避免头几条请求都按「额度未知」算权重
- **新增接口与字段**：只读 `GET /api/runtime`（池内并发占用、排队长度、各账号当前有效权重 / 余量 / 是否被挡下）；`POST /api/accounts` 支持并保留 `tuning`（`accountConcurrency` / `poolConcurrency` / `queueTimeoutMs` / `retry429`）与账号级 `concurrency` / `weight` 字段（原保存逻辑只留 `name/key/enabled`，会把这两个字段抹掉）

> `weighted` 模式与并发调优参数都可以直接在控制台「账号管理」里选 / 填（模式下拉框含
> 「按额度权重（剩余多优先）」，下面是默认并发 / 池总并发 / 排队超时 / 429 重试），
> 也可以改 `config.json`（或调 `POST /api/accounts` 的 `tuning`）。注意：**直接改文件需要重启才生效**，
> 在页面上保存则立即生效（并写回文件）。

---

## 快速开始

### 1. 本地运行（30 秒）

```bash
# 本仓库（二次开发版）
git clone https://github.com/somehow-paul/cline-pass-switcher-update.git cline-pass-switcher
cd cline-pass-switcher
# 原项目（无二次开发内容）：git clone https://github.com/munmunjaklin458-afk/cline-pass-switcher.git

node server.js        # 仅需 Node ≥ 18，无需 npm install
```

打开 <http://127.0.0.1:3123/>，在「账号管理」里添加 Cline Pass 账号（`sk_` 开头的 key）并保存即可。
**没有 key 也能启动**：页面会提示配置入口。

> Cline Pass key 从哪里来？购买 Cline Pass 订阅后，在 Cline 的账户设置里创建 API Key。
> 订阅模型 ID 均为 `cline-pass/*` 前缀，例如 `cline-pass/glm-5.2`。
> 默认内置 15 个订阅模型清单，也可以点「⬇ 拉取官方最新模型」从官方接口 / 官方文档 / 社区注册表合并最新清单。

### 2. Docker 部署

**方式 A：All-in-one（自带 Caddy 自动 HTTPS，推荐新手）**

```bash
mkdir -p data && cp config.example.json data/config.json   # 编辑 data/config.json 填入账号 key

# 有域名（A 记录指向服务器，自动签发 Let's Encrypt 受信证书）：
CPASS_DOMAIN=pass.example.com docker compose -f deploy/docker-compose.all-in-one.yml up -d --build

# 只有 IP（自签证书，浏览器需手动信任一次）：
docker compose -f deploy/docker-compose.all-in-one.yml up -d --build
```

访问 `https://你的域名/`（或 `https://服务器IP/`），在控制台设置代理密钥即可对外服务。

**方式 B：已有自己的反向代理（nginx 门户等）**

根目录 `docker-compose.yml` 只启动应用并绑定 `127.0.0.1:3123`，TLS 交给你现有网关：

```nginx
location / {
    proxy_pass http://127.0.0.1:3123;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-Proto https;
    proxy_buffering off;            # 流式响应必须
    proxy_read_timeout 600s;
}
```

### 3. 环境变量

| 变量 | 说明 |
|---|---|
| `CLINE_PASS_KEY` | 上游 Cline Pass API Key（无 config 时自动创建账号） |
| `PROXY_KEY` | 下游代理密钥（客户端访问代理的凭据） |
| `PUBLIC_BASE_URL` | 门户展示的公网代理地址，如 `https://pass.example.com` |
| `PORT` / `BIND_HOST` / `DATA_DIR` | 端口 / 绑定地址（容器内为 `0.0.0.0`）/ 配置目录 |

环境变量在启动时覆盖 `config.json`；此后通过控制台保存设置，会以当前生效值写回文件。

---

## 接入下游客户端

任何 OpenAI 兼容客户端 / Cline 扩展，把 Base URL 指向代理即可：

```
Base URL: http://127.0.0.1:3123/v1
API Key:  （控制台「访问与安全」里设置的代理密钥；本地留空 = 不鉴权）
Model:    cline-pass/glm-5.2 等
```

```bash
curl http://127.0.0.1:3123/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer <代理密钥>' \
  -d '{"model":"cline-pass/glm-5.2","messages":[{"role":"user","content":"hi"}]}'
```

代理对下游做了兼容性标准化，并回写命中的真实信息：

| 响应头 | 含义 |
|---|---|
| `X-Cline-Target-Upstream` | 本次配置的目标上游序列（`a>b`），`auto` = 未指定 |
| `X-Cline-Actual-Upstream` | 网关**实际**命中的上游 |
| `X-Cline-Canonical-Model` | 背后的真实模型 ID |
| `X-Cline-Account` | 本次使用的账号 |
| `X-Cline-Account-Load` / `X-Cline-Queue-Ms` / `X-Cline-RateLimit-Retries` | 该账号当时并发占用 / 本地排队等待毫秒 / 因 429 退避重试次数（本仓库新增） |
| `X-Cline-Attempts` | 尝试了几次（故障转移次数） |
| `Retry-After` | 仅当最终错误是上游真 429 时给出（本仓库新增） |

错误统一为 `{"error":{"message":...}}`，并**保留上游真实 HTTP 状态**（429 就是 429，模型不存在是 404，
其余网络/未知错误才是 502）；成功响应里的 `{"data":...}` 包装会被解包成标准 OpenAI 格式。

---

## 控制台功能一览

| 卡片 | 功能 |
|---|---|
| 账号管理 | 账号池增删改、显隐密钥、逐账号连通性测试、单账号/轮询模式、用量统计；每账号的额度进度（5 小时 / 本周 / 本月，含上限、重置时间与套餐状态，可手动刷新） |
| 访问与安全 | 修改下游代理密钥（即时生效）、公网代理地址、鉴权开关 |
| 订阅模型 | 背后模型 / 渠道数 / 最近实际渠道；上游优先级与排除（逐渠道「优先 / 排除」药丸 + ↑↓ 排序）；严格钉住 / 优先+回退；成本 / 首字 / 吞吐排序 |
| 操作按钮 | 探测（刷新渠道清单）、测试（单次钉住验证）、校验（全渠道实测地图）、⬇ 拉取官方最新模型 |
| 测试台 | 任选模型 + 渠道发一条小请求，直接看网关是否采纳 |
| 请求历史 | 自动记录每条请求的账号、实际渠道、耗时、尝试序列（最近 100 条，含流式） |
| 完整模型目录 | Cline 公开目录模型，`:free` 变体可精确钉住；可选是否并入代理的 `/v1/models` |

---

## 核心机制（实测结论）

### 两条路由管道，钉住写法完全不同

Cline Pass 订阅模型在 Cline 网关之后分成两条管道：

| 管道 | 实际后端 | 识别特征 | 钉住方式 |
|---|---|---|---|
| **直连**（direct） | OpenRouter | 响应顶层带 `provider` 与真实 `model` 字段 | 顶层 `provider.only / order / sort` |
| **规划器**（planner） | **Vercel AI Gateway** | 响应带 `provider_metadata.gateway.routing` | **`providerOptions.gateway.only / order / sort`** |

**关键发现**：规划器管道的请求由 Vercel AI Gateway 执行，请求体里的顶层 `provider.only/order` 会被 Cline 丢弃
（这也是官方 API 上「换上游不生效」的原因），但 `providerOptions.gateway` 嵌套形式会**原样透传**：

```json
{
  "model": "cline-pass/glm-5.2",
  "messages": [],
  "providerOptions": { "gateway": { "only": ["alibaba"] } }
}
```

实测响应：`finalProvider: "alibaba"`，规划器理由变为 `Provider set restricted to: alibaba`。
参考：[Vercel AI Gateway — Provider Filtering, Ordering & Sorting](https://vercel.com/docs/ai-gateway/models-and-providers/provider-filtering-and-ordering)

排序字段两套命名不同，工具已自动换算：`cost → price`、`ttft → latency`、`tps → throughput`（OpenRouter），
Vercel 侧直接用 `cost / ttft / tps`。管道未知时**两种形式同时注入**，各自取用、互不干扰。

### 排除与故障转移怎么落地

- **排除**：网关不支持 `exclude/ignore` 字段（实测被静默忽略），因此排除统一换算成 **only 白名单**：
  自动模式 `only = 已知上游 − 排除项`；优先+回退模式 `order = [当前, ...其余勾选]` 且 `only = 已知上游 − 排除项`
  （防止网关回退到被排除的渠道）；严格钉住模式 `only = [当前上游]`，天然排除其他一切渠道。
- **故障转移**：勾选的上游按勾选顺序逐个尝试，每次尝试有**独立 120s 超时**；流式请求在**首包（SSE 开始）前**
  同样可切换，SSE 一旦开始就只透传、不重试。
- **自动学习**：钉住失败会按错误文本把渠道标成 `limited / bad / auth`（瞬时限流只标限流、不拉黑）；
  白名单因网关新增渠道而过期时，报错里附带的「Available providers are: ...」最新清单会被自动合并。

### 上游枚举的三种手段

1. **响应元数据回读**：规划器管道带 `canonicalSlug` / `fallbacksAvailable` / `finalProvider`；直连管道顶层 `provider` 即实际上游；
2. **假上游探测**（零 token）：带不存在的 `only:["__probe__"]` 让网关在路由层报错并列出精确的可用渠道清单
   （两条管道的清单**不一致**，要分别取：直连取 `available_providers`，规划器解析错误文本）；
3. **OpenRouter 公开接口** `GET /api/v1/models/{slug}/endpoints`：补充上下文长度 / 在线率（对直连管道有直接参考意义）。

### 实测记录（2026-09）

| 实验 | 结果 |
|---|---|
| glm-5.2 + 顶层 `provider.only/ignore/order` | 全部被网关丢弃，恒选同一渠道 |
| glm-5.2 + `providerOptions.gateway.only:["alibaba"]` | ✔ `finalProvider: alibaba` |
| glm-5.2 流式 + `only:["baseten"]` | ✔ 流式同样生效 |
| glm-5.2 + `providerOptions.gateway.sort:"cost"` | ✔ 按成本重排执行顺序 |
| glm-5.3-flash（直连）+ 顶层 `provider.only:["gmicloud"]` | ✔ `provider: "GMICloud"` |
| glm-5.3-flash + `providerOptions.gateway` | ✘ 无效（直连管道只认顶层 provider 形式） |

> 管道归属由 Cline 侧决定、可能随时间变化，控制台的「探测」会刷新每个模型的管道类型与渠道清单。

---

## 配置参考（config.json）

位于 `DATA_DIR`（默认程序目录）下，与 `metadata.json`（运行数据：模型元数据、请求历史、统计）同目录。
环境变量在启动时覆盖它，控制台保存时以当前生效值写回。

| 字段 | 说明 |
|---|---|
| `accounts` | 账号池：`[{ name, key, enabled, concurrency?, weight? }]`（`concurrency` 该账号并发上限，`weight` 手动权重默认 1；均可在控制台保存） |
| `accountMode` | `single` 手动指定 / `roundrobin` 轮询 / `weighted` 按额度剩余加权随机（控制台可选） |
| `activeAccount` | 单账号模式下使用的下标 |
| `accountConcurrency` | 未单独设置 `concurrency` 的账号使用的默认并发，默认 `5` |
| `poolConcurrency` | 账号池总并发上限，`0` = 不限制 |
| `queueTimeoutMs` | 并发满时的排队等待上限，默认 `120000`；`0` = 不排队、立即回 503 |
| `retry429` | 上游真 429 的退避重试：`{ retries, baseDelayMs, maxDelayMs, totalTimeoutMs }`，默认 `3 / 1500 / 20000 / 120000`；`totalTimeoutMs` 限住整条请求花在重试上的总时长 |
| `quotaWeights` | 三窗口额度剩余做加权平均时各窗口的权重，默认 `{ five_hour: 1, weekly: 1, monthly: 1 }` |
| `quotaStopPercent` | 任一窗口「已用」达到该值即退出加权，默认 `100`（真正用满才让路） |
| `quotaRefreshMs` | 选号用额度快照的后台刷新间隔，默认 `900000`（15 分钟） |
| `proxyKey` | 下游代理密钥；空 = 不鉴权 |
| `publicBaseUrl` | 公网代理地址（控制台展示与复制用） |
| `exposeCatalog` | `true` 时代理的 `/v1/models` 会合并 Cline 公开目录模型；默认 `false` 只返回订阅模型（避免客户端模型列表被淹没） |
| `knownModels` | 订阅模型清单（控制台主表；「拉取官方最新模型」只增不删） |
| `perModel` | 每模型的钉住配置：`{ upstreams: string[], exclude: string[], pinMode: 'strict'\|'preferred', sort: 'cost'\|'ttft'\|'tps'\|null }` |
| `apiKey` | 旧版单 key 字段，启动时自动迁移进 `accounts` |

> `perModel.upstream`（旧版单上游字段）保留为兼容镜像，`maxRetries` 已退役（旧值仅作回滚兼容保留在文件里）。

---

## HTTP 接口一览

| 方法与路径 | 作用 |
|---|---|
| `GET /` | 网页控制台（页面本身不含敏感数据；数据由带鉴权的 `/api/*` 提供） |
| `GET /api/meta` | 是否需要鉴权、代理地址、是否已配置账号 |
| `GET /api/models` | 订阅模型 + 目录模型 + 探测元数据 + 上次拉取官方清单的结果 |
| `POST /api/probe` | 探测某模型的管道类型与上游渠道清单 |
| `POST /api/test` | 用临时钉住配置发一条真实小请求，返回实际上游与逐次尝试明细 |
| `POST /api/validate-upstreams` | 把某模型的每个渠道各钉一次，标记真实可用性（批量 5 并发） |
| `POST /api/fetch-official-models` | 从官方接口 / 官方文档 / 社区注册表拉取最新订阅模型（只增不删） |
| `GET /api/accounts` | 账号池、模式、用量统计、`tuning` 与运行态快照 |
| `POST /api/accounts` | 保存账号池与闸门调优参数 |
| `POST /api/accounts/test` | 单账号连通性测试 |
| `GET /api/quota` | 各账号订阅额度（`?refresh=1` 跳过缓存；默认 TTL = `quotaRefreshMs`） |
| `GET /api/runtime` | 只读运行态：池内并发、排队长度、各账号有效权重 / 余量（本仓库新增） |
| `GET` / `POST /api/security` | 读取 / 修改代理密钥、公网地址与 `exposeCatalog` 开关 |
| `GET` / `POST /api/config` | 读取 / 保存每模型的钉住配置 |
| `GET /api/history` | 最近 100 条请求历史 |
| `GET /v1/models` | 代理的模型列表（同义路径 `/api/v1/models`、`/models`） |
| `POST /v1/chat/completions` | 聊天补全代理（同义路径 `/chat/completions`、`/api/v1/chat/completions`） |

鉴权：**设置了 `proxyKey` 时**，所有 `/api/*` 与 `/v1/*` 都需要
`Authorization: Bearer <proxyKey>` 或 `X-Admin-Key: <proxyKey>`；控制台页面会弹出登录框，
密钥存在浏览器 `localStorage`，改密钥后自动更新、不会被锁在外面。

---

## 常见问题

**Q：为什么选了某个渠道会报 `invalid_request_error`？**
部分渠道被单独钉住时会因模型 ID 映射失败，还有渠道处于共享池限流（429）状态。点该模型行的「校验」，
把所有渠道实测一遍，下拉框会标注 ✔可用 / ⏳限流 / ✘不可钉 / 🔑key错。钉住失败的渠道会被自动学习标记。

**Q：限流的渠道还能用吗？**
能。限流是共享池的临时状态，过段时间重新「校验」即可；或改用「优先+回退」模式，限流时自动跳到其他渠道。
本仓库还会对上游真 429 自动退避重试。

**Q：直接用官方 API 写 `provider.only` 为什么不生效？**
对规划器管道（走 Vercel AI Gateway 的模型）会被 Cline 网关丢弃，请改用 `providerOptions.gateway`，见上文。

**Q：两条管道的渠道清单为什么不一样？**
钉住发生在不同后端（OpenRouter vs Vercel AI Gateway），各自支持的渠道池不同，要以对应清单为准。

**Q：多个请求打满并发会怎样？会互相抢账号吗？**
不会报错，会按 FIFO 排队等名额（默认最多等 120s，超时才回 503）。想要更主动的分配，可用 `weighted` 模式
按剩余额度加权选号；想限制单账号压力就调账号级 `concurrency` 或 `poolConcurrency`。

**Q：订阅额度怎么计？经代理会不会更贵？**
经代理的请求与直连官方 API 计费一致；「探测 / 测试 / 校验」会产生极小额的真实请求（每次约 0.0002 美元级），
而额度查询和 `/api/runtime` 是纯只读、**不计费**。

---

## 安全提醒

- `config.json` / `metadata.json` / `data/` 含明文密钥，已在 `.gitignore` 排除，**不要提交或分享**；
- 对外部署务必设置 `proxyKey`（控制台可随时轮换），并让反向代理开启 TLS；
- 「探测 / 测试 / 校验」会产生极小额真实请求；`retry429.retries > 0` 会放大请求量，注意额度消耗。

## License

[MIT](LICENSE)
