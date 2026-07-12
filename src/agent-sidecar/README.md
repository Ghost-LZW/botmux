# botmux-goal-v1 — headless 单-run 机器协议（canonical wire contract）

> 本文件是 `src/agent-sidecar/` 的协议真相源：wire 类型与 canonical hash 以 `contract.ts` 为机器可读版本，两者必须逐字一致；motivation 仓库只按本页镜像消费，不 deep-import botmux。

> 状态：Phase 0/1 施工契约（2026-07-12）。上游设计：《botmux 作为 agentd Runtime 的接入方案》（飞书 docx rev4）、motivation `docs/ADR_BOTMUX_BOUNDARY.md`、`docs/SPIKE_BOTMUX_AGENTD_BACKEND.md`。
> 所有权：协议与 sidecar 实现归 botmux 仓库（`src/agent-sidecar/`）；motivation 仅按本页镜像 wire 类型消费，**不 deep-import botmux**。
> 硬边界：本协议只描述「单次 ephemeral headless goal run」。不是聊天会话、不是 workflow/DAG 编排、不进 planner 热回路。
> 三道门（transport / 安全 enforcement / 完整 per-run cost）未过前：**不注册 fleet、不进入自动候选池、不得声称生产安全**。

## 0. 版本与能力声明

- 协议常量：`BOTMUX_GOAL_PROTOCOL = 'botmux-goal-v1'`，URL 前缀 `/v1/`。
- **v1 能力声明恒为 `{ input: false, human: false }`**：不提供 `POST /runs/:id/input`；ASK_HUMAN 只作为结构化非成功证据回传（见 §6），永不映射为 motivation escalation。
- 传输：Unix domain socket（本机），HTTP/1.1，raw `node:http`。socket 默认 `~/.botmux/agent-sidecar.sock`，父目录 mode 0700。**单一属主协议**：绑定前先以 `<socket>.lock` **O_EXCL ownership lock** 串行化（活 pid 持锁 → 拒绝启动；死 pid 锁经原子 rename 回收），拿锁后普通文件占位 → **硬失败绝不删除**（非 socket 不是可证 stale），既有 socket 活 listener → 拒绝启动（绝不夺址），仅可证 stale（ECONNREFUSED/ENOENT）unlink——单一属主是 journal 串行化与 cancel 可达性的前提，lock 关闭并发 stale 清理的 TOCTOU。v1 无 token 鉴权（UDS 文件权限即信任面；如未来走 loopback TCP 必须加独立 token，沿 botmux `daemon-internal-auth.ts` 全信封 HMAC 惯例）。

## 1. 端点总览

| 端点 | 语义 | 幂等 |
|---|---|---|
| `GET /v1/health` | `{ protocol, capabilities:{input:false,human:false}, pid, startedAt }` | — |
| `POST /v1/runs` | create-or-attach（runId + canonical requestHash） | 同 runId+同 hash 附着；同 runId+异 hash → **409** |
| `GET /v1/runs/:id/events?since=N[&follow=0]` | seq 严格递增的 NDJSON 事件帧，断线后按 cursor 重放 | 纯读，可重放 |
| `GET /v1/runs/:id/result` | 202=运行中；200=durable terminal record | 终态持久化后永久可重放 |
| `POST /v1/runs/:id/cancel` | 请求中断（grace 后强停），写终态 | 重复 cancel 返回一致结果 |

- 未知 runId：一律 **404** `{error:{code:'UNKNOWN_RUN'}}`（**禁止 200+空数组伪装**）。410 语义保留给未来 run GC（v1 不实现 GC，不发 410）。
- runId 校验：`^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$`，**先校验后拼路径**（路径遍历防御，沿 botmux `RUN_ID_RE` 惯例）。

## 2. Wire 类型（双仓逐字镜像；TS，English）

```ts
export const BOTMUX_GOAL_PROTOCOL = 'botmux-goal-v1';

/** POST /v1/runs body. requestHash covers ALL other fields (see §3). */
export interface SidecarRunRequest {
  protocol: typeof BOTMUX_GOAL_PROTOCOL;
  runId: string;        // idempotency key, ^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$
  requestHash: string;  // sha256 hex over canonicalJson(request minus requestHash)
  profileRef: string;   // node-local profile name; NEVER credentials
  goal: string;         // fully rendered instruction text (caller folds context in).
                        // Persisted VERBATIM in the run ledger: callers must not
                        // embed secrets in goal text (see §4 note on secrets scope).
  cwd: string;          // must resolve inside sidecar's allowed workspace roots
  timeoutMs: number;    // hard wall-clock limit for the run
  /** Execution mode. v1 accepts ONLY 'discovery' and enforces it at admission:
   * the resolved profile must be discovery-safe (sandbox=true AND
   * sandboxNetwork=false AND disableCliBypass=true) or the run is rejected
   * with 403 PROFILE_NOT_DISCOVERY_SAFE. Hash-covered like every other field. */
  mode: 'discovery';
  taskId?: string;      // opaque caller identity passthrough (journal/display only)
  threadId?: string;    // opaque caller identity passthrough
}

export type SidecarRunState = 'running' | 'succeeded' | 'failed' | 'blocked' | 'cancelled';

/** POST /v1/runs response (201 created / 200 attached). */
export interface SidecarRunAccepted {
  protocol: typeof BOTMUX_GOAL_PROTOCOL;
  runId: string;
  state: SidecarRunState;
  created: boolean; // true only on first creation
  capabilities: { input: false; human: false };
}

/** One NDJSON line on GET /v1/runs/:id/events. seq starts at 1, strictly increasing, gapless. */
export interface SidecarEventFrame {
  seq: number;
  ts: number; // epoch ms
  event: SidecarRunEvent;
}

export type SidecarRunEvent =
  | { type: 'run.accepted' }
  | { type: 'session'; sessionId: string } // resumable reference only; NEVER tokens/ports
  | { type: 'log'; text: string }          // structured phase logs; NEVER screen-scraped PTY
  | { type: 'terminal'; state: Exclude<SidecarRunState, 'running'> };

/** GET /v1/runs/:id/result 200 body — persisted as terminal.json BEFORE first 200. */
export interface SidecarTerminalRecord {
  protocol: typeof BOTMUX_GOAL_PROTOCOL;
  runId: string;
  state: Exclude<SidecarRunState, 'running'>;
  summary: string; // validated manifest summary, or error message
  error?: { code: string; message: string; retryable?: boolean }; // e.g. ASK_HUMAN, TIMEOUT, WORKER_ERROR, CANCELLED
  ask?: { question: string; options?: string[]; freeText?: boolean }; // ASK_HUMAN evidence ONLY (no reply channel)
  artifacts: SidecarArtifact[]; // ONLY from a validator-passed manifest; [] otherwise
  sessionId?: string;
  usage?: SidecarUsage;   // best-effort token usage; may be absent
  costComplete: boolean;  // true ONLY if usage fully collected AND normalized usd present (v1 real wiring: always false)
  startedAt: number;
  finishedAt: number;
  lastSeq: number; // seq of the terminal event frame
}

/** Validated-manifest file entry. path is outputDir-relative; absolute paths never appear. */
export interface SidecarArtifact {
  name: string;
  path: string;
  kind: string;   // markdown|json|text|code|log|binary|directory
  bytes: number;
  sha256: string; // '' for kind:'directory'
  mime: string;
}

export interface SidecarUsage {
  model: string;
  inputTokens: number;         // cache-EXCLUSIVE (botmux 'uncached' semantics)
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  turns: number;
  usd?: number;                // absent until a versioned pricing snapshot exists (Phase 2 gate)
}

/** POST /v1/runs/:id/cancel response. */
export interface SidecarCancelResponse {
  runId: string;
  state: SidecarRunState;
  cancelRequested: boolean;  // true while non-terminal
  alreadyTerminal: boolean;
}

export interface SidecarErrorBody {
  error: { code: string; message: string; expectedHash?: string; receivedHash?: string };
}
```

错误码（`SidecarErrorBody.error.code`）：`UNKNOWN_RUN`(404) / `IDEMPOTENCY_CONFLICT`(409) / `HASH_MISMATCH`(400，客户端提交的 requestHash 与服务端重算不符) / `MALFORMED_REQUEST`(400) / `UNKNOWN_PROFILE`(400) / `PROFILE_NOT_SANDBOXED`(403) / `PROFILE_NOT_DISCOVERY_SAFE`(403，profile 缺 sandboxNetwork=false 或 disableCliBypass=true) / `REAL_RUNS_DISABLED`(403，v1 真实组合根恒开启的 fail-closed 门，见 §9) / `CWD_NOT_ALLOWED`(403) / `INTERNAL`(500)。

## 3. canonical request hash（双仓必须逐字节一致）

算法：对 request 对象**除 `requestHash` 外**的全部字段做递归 canonical JSON 序列化，`sha256` hex：

1. `undefined` 字段整体省略；
2. 对象键按 UTF-16 code unit 升序排序（`Object.keys().sort()`）；
3. 无任何空白；字符串/数字用 `JSON.stringify` 标准转义；非有限数字直接抛错；
4. 数组保序。

**黄金向量（两仓 contract test 都必须钉死这些字面值）**：

```
fixture1 = { protocol:'botmux-goal-v1', runId:'run-0001-e2e', profileRef:'sandbox-claude',
             goal:'Write a haiku about idempotency into out.md', cwd:'/tmp/ws/demo', timeoutMs:600000,
             mode:'discovery' }
canonical(fixture1) = {"cwd":"/tmp/ws/demo","goal":"Write a haiku about idempotency into out.md","mode":"discovery","profileRef":"sandbox-claude","protocol":"botmux-goal-v1","runId":"run-0001-e2e","timeoutMs":600000}
sha256(fixture1)    = a591a05407c2113911a7c647949342c6e9c4baf3e1d3133800a71815c6c25490

fixture2 = fixture1 with goal += ' (changed)'
sha256(fixture2)    = 2147c1a7eb21c74e410610f26cfd1c250142d40688e746625c3908a43a6ab746

fixture3 = fixture1 + { taskId:'task-42', threadId:'thread-7' }   // 身份字段参与 hash
sha256(fixture3)    = 50264ec35037e92c0dd5d4b4887e3b6d41696906ff2bf15053735180023e1eb5
```

语义：服务端**总是重算** hash——与客户端提交值不符 → 400 `HASH_MISMATCH`；runId 已存在时与 ledger 中 `request.json` 的 hash 比对：一致 → attach（200），不一致 → 409 `IDEMPOTENCY_CONFLICT`。

## 4. Durable run ledger（sidecar 侧，crash-safe 的根基）

```
<runsRoot>/<runId>/            # runsRoot 默认 ~/.botmux/agent-runs，env BOTMUX_AGENT_RUNS_DIR 可覆盖
  request.json                 # 原始 SidecarRunRequest（atomicWrite；spawn 之前必须已落盘）
  lease.json                   # { pid, startedAt }（活跃驱动锁；attach 判断是否 re-drive）
  cancel.json                  # cancel 请求标记（幂等；先落盘再触发 abort）
  terminal.json                # SidecarTerminalRecord（atomicWrite；任何 /result 200 之前必须已落盘）
  v3/<runId>/...               # 内嵌 v3 引擎 runDir（journal.ndjson 等，引擎自管）
```

不变量：
1. **ledger-before-spawn**：`request.json` 落盘成功之前不得 fork 任何 worker；
2. **terminal-before-ack**：`terminal.json` 原子落盘之前，`/result` 只能回 202、events 流不得发 `terminal` 帧；
3. **journal 终态 → terminal.json 可幂等重建**：进程在「journal 已终态、terminal.json 未写」窗口崩溃后，重启的 finalize 必须产出同一逻辑终态（同 state/同 error code；ts 类字段除外），**绝不产生第二个逻辑终态**；
4. **lease fencing（本机版）**：`lease.json` 内 pid 存活 → attach 不 re-drive（不双跑）；pid 已死且 journal 未终态 → attach 触发 re-drive（v3 journal 重放天然跳过已完成节点，新 attempt 是重试而非重复执行）；
5. **sidecar 侧凭证与 launch env 永不落 ledger**：`request.json` 无凭证字段；BotSnapshot 落盘沿 botmux 既有契约省略 `larkAppSecret`；goal 经文件传递不进 argv。**范围澄清（诚实边界）**：`goal` 文本按原样持久化于 `request.json` 与 goal.txt——调用方（motivation 会把 supervisor context 折进 goal）**不得在 goal 中携带 secret**；sidecar 不做（也无法做）语义级 secret 识别/擦除。

## 5. 事件流（seq cursor 重放）

- wire 事件由 v3 `journal.ndjson` 派生（sidecar 不改 journal 格式）：`seq = 已成功解析的 journal 行序（1-based）` 的映射结果；torn final line 按 botmux 既有语义忽略直到写全。
- 映射：`runStarted→run.accepted`；`nodeSessionReady→session`（**剥离 webPort/token，只留 sessionId**）；`nodeDispatched/nodeRetryRequested/nodeSucceeded/nodeFailed/nodeBlocked→log`（结构化摘要文本）；`runSucceeded/runFailed/runBlocked→terminal`（cancel 场景按 §7 折算为 `cancelled`）。
- `?since=N`：返回 `seq>N` 的帧；`follow` 缺省=流式跟随直到 terminal 帧后关闭；`follow=0` 重放完即关（不管是否终态）。
- 同一 run 的事件序列在任意次读取间必须前缀一致（append-only；重启不重排、不缩水——除 torn tail 补全）。

## 6. ASK_HUMAN 语义（v1 硬裁定）

- botmux goal-mode 的 ASK_HUMAN = worker 写 `ask.json` + fail manifest（`error.code='ASK_HUMAN'`, `retryable:true`）→ v3 终态 `blocked`。
- wire 如实回 `state:'blocked'` + `error:{code:'ASK_HUMAN',...}` + `ask`（结构化证据）。
- **motivation 侧一律映射为 `error` + `result outcome:'failed'`**（resultText 含机器可读 ask JSON），**绝不**映射为 escalation/postInput——v1 没有回答通道，两边不得同时长出人工卡片。未来接入需单独 ADR（durable needs_human 或真 live-input 合同）。

## 7. cancel 语义

- 非终态：落 `cancel.json` → AbortSignal → SIGINT，grace（5s）后 SIGKILL → 引擎终态按 cancelled 折算 → 写 terminal.json(state:'cancelled', error.code:'CANCELLED')。响应 202 `{cancelRequested:true, alreadyTerminal:false}`。
- 已终态：200 `{state:<terminal>, cancelRequested:false, alreadyTerminal:true}`。
- 重复 cancel（任意次、任意时机）返回与当时状态一致的同构结果；**cancel 后 attach/result 永远收敛到同一个终态**。

## 8. cost 语义（诚实优先）

- sidecar 是 cost 采集 owner：终态后、teardown 前，按冻结在 ledger 里的 `(cliId, sessionId, cwd)` fold CLI 原生 transcript（claude 族：确定性 `--session-id`、`~/.claude/projects/<realpath(cwd) sanitized>/<sessionId>.jsonl`），产出 4-bucket token usage。
- 采集范围 = **整个 run 的全部 attempt session**（journal `nodeSessionReady` 真相，retry 的花费也是本 run 的花费），collector 必须跨 attempt 聚合。
- `costComplete` 的**记录级 gate 由 sidecar own**（collector 声明不足信）：`model` 非空、四个 token bucket 与 `turns` 均为非负整数、`usd` 有限非负，全部满足才可为 true（NaN 经 JSON 持久化变 null 会毒化消费方）——collector 谎报 true 而缺 usd 的记录会被钉回 false（canonical owner 不产出自相矛盾记录）。**v1 生产装配恒为 false**（botmux 无定价表；usd 字段缺省不填）——这就是「成本门未过」的诚实暴露，测试经注入 fake collector 覆盖 true 分支。
- **禁止**：usage 采集失败时编造 0；`usd:0` 冒充已计费；costComplete=false 的 runtime 进自动候选池。
- motivation 适配器：`costComplete=false` → **不发 cost 事件**（宁缺毋假，避免污染 append-only 的 run.cost 聚合），usage 以 log 事件形式留观测痕迹。

## 9. 安全门（v1 结构性拒绝面）

1. sidecar 侧（v1 只收 `mode:'discovery'`）：profileRef 解析出的 BotSnapshot **必须 `sandbox=true`**（403 `PROFILE_NOT_SANDBOXED`）且 **`sandboxNetwork=false` + `disableCliBypass=true`**（403 `PROFILE_NOT_DISCOVERY_SAFE`）。**诚实边界：这些 profile 条件是必要不充分**——当前 botmux sandbox 仍把 daemon-mediated relay outbox（`botmux send` 经 host watcher 真实外发）与真实 auth 路径以 rw bind 放进沙箱（`src/adapters/backend/sandbox.ts`），因此**即使断网也不构成"无外发"**，v1 不声明 discovery-safe/零副作用。**因此 v1 真实组合根（main.ts）恒开启 `REAL_RUNS_DISABLED` fail-closed 门：拒绝一切真实 worker run（合同证明只经测试注入的 fake runNode 进行）**；解除该门=代码改动而非配置，前置：sidecar sandbox policy（不 bind relay outbox、不注入 `BOTMUX_SEND_RELAY`/shim、auth 只读或隔离 overlay）+ Linux 运行时负向测试证明 worker 无法外发/写真实资源。这仍不等于 motivation 的 preSideEffect 逐调用契约。
2. sidecar 侧：`cwd` realpath 必须落在 `allowedWorkspaceRoots` 内（默认空=全拒），否则 403 `CWD_NOT_ALLOWED`；symlink 逃逸按 realpath 判。
3. motivation 侧：`BotmuxGoalExecutorAdapter.preSideEffect` **抛异常**（fail-closed；本 runtime 无逐调用 seam，任何对它的调用都是装配错误）；`run()` 对 `mode==='execute'` 同步抛错拒绝；adapter 不注册进 resolver/fleet-sync/candidate 路径（独立 composition root 结构性不可达）。
4. **单一属主**：启动绑定前探测既有 socket，活 listener 拒绝启动（见 §0）——防第二进程夺址后旧 run 失去 cancel 可达性。
5. Manifest 校验（botmux 既有 validator）：路径 containment、绝对路径拒绝、sha256/bytes/mime 核验；未过校验 → artifacts=[]、state 按 manifestInvalid 处理，**不发布任何 artifact**。

## 10. 验收矩阵（两仓 contract tests 的最低覆盖；全自动化，不靠人工日志）

| # | 场景 | 断言 |
|---|---|---|
| A1 | 同 runId+同 hash：live / 终态后 / sidecar 重启后 三态各 attach | 只执行一次；三态均可 attach 且 /result 收敛同一终态 |
| A2 | 同 runId+异 hash | 409 IDEMPOTENCY_CONFLICT，不触发执行 |
| A3 | 客户端 hash 与自身 payload 不符 | 400 HASH_MISMATCH，不落 ledger |
| A4 | sidecar 在 journal 终态前 / 终态后-terminal.json 前 崩溃重启 | 无双终态、无静默成功；重建后 /result 幂等 |
| A5 | events 断连后按 since 重放 | 前缀一致、seq 无缝、terminal 帧唯一 |
| A6 | /result 响应丢失后重取 | 永久可重放，字节级同一逻辑记录 |
| A7 | cancel 连击两次（运行中/终态后） | 幂等；终态收敛 cancelled |
| A8 | manifest 路径逃逸 / 绝对路径 / sha256 不符 | 失败且 artifacts=[] |
| A9 | cwd 越界 / symlink 逃逸 | 403 CWD_NOT_ALLOWED |
| A10 | 非 sandbox profile | 403 PROFILE_NOT_SANDBOXED |
| A11 | usage 采集失败（transcript 缺失/超限） | costComplete=false、usage 缺省，不编造 0 |
| A12 | costComplete=false 进 motivation 适配器 | 不发 cost 事件；unroutable 声明成立 |
| A13 | ASK_HUMAN | state:'blocked' + ask 证据；motivation 侧= error+failed result，无 escalation |
| A14 | 未知 runId（events/result/cancel） | 404 UNKNOWN_RUN |
| A15 | secrets 扫描 | ledger/argv/wire/日志内无凭证（以 marker 值注入断言） |
| A16 | 黄金向量 §3 | 两仓 hash 字面值一致 |
| A17 | mode 缺失/非 discovery | 400 MALFORMED_REQUEST，不触发执行 |
| A18 | sandbox 开但网开 / bypass 未武装 | 403 PROFILE_NOT_DISCOVERY_SAFE，门先于账本 |
| A19 | collector 谎报 costComplete=true 无 usd（或 usd 非有限/负） | 记录级 gate 钉回 false |
| A20 | 活 socket 上二次启动 | 硬失败不夺址；旧 server 照常服务、活 run 可 cancel |
| A21 | 多 attempt run | collector 收到全部 attempt session 并聚合 |
| A22 | v1 真实组合根 | 一切 create/attach → 403 REAL_RUNS_DISABLED，零账本写 |
| A23 | 两进程并发面对同一 stale socket | ownership lock 保证恰好一个赢家，赢家地址不被 unlink |
| A24 | 普通文件占位 socket 路径 | 硬失败且文件不被删除 |
| A25 | usage turns NaN/负 或 model 空 | costComplete 钉回 false |
