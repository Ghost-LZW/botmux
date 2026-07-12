/**
 * agent-sidecar 测试夹具：内存内 UDS server + fake runNode（写真实 manifest 文件）
 * + 真实 manifest validator。不 spawn 任何真实 CLI。
 *
 * 注意 macOS sun_path 104 字节上限：socket 建在 os.tmpdir() 下的短 mkdtemp 路径。
 */

import { createHash } from 'node:crypto';
import { mkdtempSync, mkdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { request as httpRequest } from 'node:http';
import type { Server } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  GOAL_ENV,
  type BotSnapshot,
  type Manifest,
  type RunNode,
  type ValidateManifest,
} from '../src/workflows/v3/contract.js';
import { ManifestValidationError, readAndValidateManifest } from '../src/workflows/v3/manifest.js';
import {
  BOTMUX_GOAL_PROTOCOL,
  canonicalRequestHash,
  type SidecarEventFrame,
  type SidecarRunRequest,
} from '../src/agent-sidecar/contract.js';
import { createRunStore, type CollectUsage, type RunStore } from '../src/agent-sidecar/run-store.js';
import { createRunDriver, type RunDriver } from '../src/agent-sidecar/driver.js';
import { createSidecarServer, listenOnSocket } from '../src/agent-sidecar/server.js';

// ─── real-validator adapter（cli-run.ts 同款） ──────────────────────────────

export const realValidateManifest: ValidateManifest = async (manifestPath, outputDir) => {
  try {
    return { ok: true, manifest: await readAndValidateManifest(manifestPath, outputDir) };
  } catch (e) {
    return { ok: false, problems: e instanceof ManifestValidationError ? e.problems : [String(e)] };
  }
};

// ─── manifest 产物助手 ──────────────────────────────────────────────────────

export function product(outputDir: string, name: string, content: string): Manifest['files'][number] {
  writeFileSync(join(outputDir, name), content);
  return {
    name,
    path: name,
    kind: 'markdown',
    bytes: Buffer.byteLength(content),
    sha256: createHash('sha256').update(content).digest('hex'),
    mime: 'text/markdown',
  };
}

export function writeManifest(req: Parameters<RunNode>[0], manifest: Manifest): string {
  const p = req.env[GOAL_ENV.MANIFEST_PATH]!;
  writeFileSync(p, JSON.stringify(manifest));
  return p;
}

// ─── fake runNode 家族 ──────────────────────────────────────────────────────

/** 正常成功：写一个真实产物 + ok manifest；可选上报 session（带脏 webPort/token，
 *  wire 层必须剥掉）。 */
export function okRunNode(opts: { sessionId?: string; content?: string } = {}): RunNode {
  return async (req) => {
    if (opts.sessionId) {
      await req.onSessionReady?.({ sessionId: opts.sessionId, webPort: 40123, token: 'tok-must-never-leak' });
    }
    const file = product(req.outputDir, 'out.md', opts.content ?? `# done\n${req.node.goal}`);
    const manifestPath = writeManifest(req, {
      schemaVersion: 1, status: 'ok', summary: `done: ${req.node.id}`, files: [file],
    });
    return { status: 'ok', manifestPath };
  };
}

/** ASK_HUMAN：写 ask.json + fail manifest（error.code=ASK_HUMAN, retryable:true）。 */
export function askRunNode(question: string, options: string[]): RunNode {
  return async (req) => {
    writeFileSync(join(req.attemptDir, 'ask.json'), JSON.stringify({ question, options }));
    const manifestPath = writeManifest(req, {
      schemaVersion: 1, status: 'fail', summary: question, files: [],
      error: { code: 'ASK_HUMAN', message: question, retryable: true },
    });
    return { status: 'ok', manifestPath };
  };
}

/** A8 三连：路径逃逸 / 绝对路径 / sha256 不符 —— 全部交给真实 validator 拒。 */
export function badManifestRunNode(kind: 'escape' | 'absolute' | 'sha'): RunNode {
  return async (req) => {
    let entry: Manifest['files'][number];
    if (kind === 'escape') {
      writeFileSync(join(req.outputDir, '..', 'evil.md'), 'escaped');
      entry = { name: 'evil', path: '../evil.md', kind: 'markdown', bytes: 7, sha256: createHash('sha256').update('escaped').digest('hex'), mime: 'text/markdown' };
    } else if (kind === 'absolute') {
      entry = { name: 'abs', path: '/etc/hosts', kind: 'text', bytes: 1, sha256: 'a'.repeat(64), mime: 'text/plain' };
    } else {
      entry = { ...product(req.outputDir, 'out.md', 'real content'), sha256: 'b'.repeat(64) };
    }
    const manifestPath = writeManifest(req, {
      schemaVersion: 1, status: 'ok', summary: 'claims ok', files: [entry],
    });
    return { status: 'ok', manifestPath };
  };
}

/** 挂起直到 cancelSignal —— cancel 语义测试用。 */
export function hangUntilAbortRunNode(): RunNode {
  return (req) =>
    new Promise((resolve) => {
      const settle = (): void =>
        resolve({ status: 'fail', manifestPath: req.env[GOAL_ENV.MANIFEST_PATH]! });
      if (req.cancelSignal?.aborted) return settle();
      req.cancelSignal?.addEventListener('abort', settle, { once: true });
    });
}

/** 手动闸门：release() 之前 runNode 停在门口（live attach 测试用）。 */
export function gatedRunNode(inner: RunNode): { runNode: RunNode; release: () => void } {
  let open!: () => void;
  const gate = new Promise<void>((r) => { open = r; });
  return {
    release: () => open(),
    runNode: async (req) => {
      await gate;
      return inner(req);
    },
  };
}

// ─── wire 请求构造 ──────────────────────────────────────────────────────────

export function buildRunBody(partial: {
  runId: string;
  goal: string;
  cwd: string;
  profileRef?: string;
  timeoutMs?: number;
  taskId?: string;
  threadId?: string;
}): SidecarRunRequest {
  const base = {
    protocol: BOTMUX_GOAL_PROTOCOL,
    runId: partial.runId,
    profileRef: partial.profileRef ?? 'sandbox-claude',
    goal: partial.goal,
    cwd: partial.cwd,
    timeoutMs: partial.timeoutMs ?? 60_000,
    ...(partial.taskId !== undefined ? { taskId: partial.taskId } : {}),
    ...(partial.threadId !== undefined ? { threadId: partial.threadId } : {}),
  };
  return { ...base, requestHash: canonicalRequestHash(base) } as SidecarRunRequest;
}

// ─── UDS HTTP 客户端 ────────────────────────────────────────────────────────

export interface WireResponse {
  status: number;
  text: string;
}

export function wire(
  socketPath: string,
  method: string,
  path: string,
  body?: unknown,
): Promise<WireResponse> {
  return new Promise((resolve, reject) => {
    const r = httpRequest(
      { socketPath, path, method, headers: body !== undefined ? { 'content-type': 'application/json' } : {} },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, text: Buffer.concat(chunks).toString('utf-8') }));
      },
    );
    r.on('error', reject);
    if (body !== undefined) r.write(typeof body === 'string' ? body : JSON.stringify(body));
    r.end();
  });
}

export function parseFrames(ndjson: string): SidecarEventFrame[] {
  return ndjson.split('\n').filter((l) => l.trim() !== '').map((l) => JSON.parse(l) as SidecarEventFrame);
}

export async function awaitTerminal(
  socketPath: string,
  runId: string,
  timeoutMs = 10_000,
): Promise<{ status: number; record: any }> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = await wire(socketPath, 'GET', `/v1/runs/${runId}/result`);
    if (res.status === 200) return { status: 200, record: JSON.parse(res.text) };
    if (Date.now() > deadline) throw new Error(`run ${runId} not terminal after ${timeoutMs}ms (last: ${res.status})`);
    await new Promise((r) => setTimeout(r, 25));
  }
}

// ─── 组合根夹具 ─────────────────────────────────────────────────────────────

export interface Stack {
  root: string;
  runsRoot: string;
  wsRoot: string;
  cwd: string;
  socketPath: string;
  store: RunStore;
  driver: RunDriver;
  server: Server;
  runNodeCalls: () => number;
  close(): Promise<void>;
}

export const DEFAULT_PROFILES: Record<string, BotSnapshot> = {
  'sandbox-claude': {
    larkAppId: 'cli_test_app',
    cliId: 'claude-code',
    sandbox: true,
    disableCliBypass: true,
    workingDir: '/tmp',
  },
  'no-sandbox': {
    larkAppId: 'cli_test_app',
    cliId: 'claude-code',
    workingDir: '/tmp',
  },
};

export async function makeStack(opts: {
  runNode: RunNode;
  collectUsage?: CollectUsage;
  profiles?: Record<string, BotSnapshot>;
  /** 复用既有 runsRoot（模拟 sidecar 重启：同一账本上起新 server/store）。 */
  reuseRunsRoot?: string;
  extraWorkspaceRoots?: string[];
}): Promise<Stack> {
  const root = mkdtempSync(join(tmpdir(), 'bmx-as-'));
  const runsRoot = opts.reuseRunsRoot ?? join(root, 'runs');
  const wsRoot = join(root, 'ws');
  const cwd = join(wsRoot, 'proj');
  mkdirSync(cwd, { recursive: true });

  let calls = 0;
  const countedRunNode: RunNode = async (req) => {
    calls++;
    return opts.runNode(req);
  };

  const profiles = opts.profiles ?? DEFAULT_PROFILES;
  const store = createRunStore(runsRoot);
  const driver = createRunDriver({
    store,
    resolveProfile: (ref) => profiles[ref],
    allowedWorkspaceRoots: [wsRoot, ...(opts.extraWorkspaceRoots ?? [])],
    runNode: countedRunNode,
    validateManifest: realValidateManifest,
    collectUsage: opts.collectUsage,
    log: () => {},
  });
  const server = createSidecarServer({ store, driver, followPollMs: 20, log: () => {} });
  const socketPath = join(root, 's.sock');
  await listenOnSocket(server, socketPath);

  return {
    root,
    runsRoot,
    wsRoot,
    cwd: realpathSync(cwd),
    socketPath,
    store,
    driver,
    server,
    runNodeCalls: () => calls,
    close: async () => {
      await new Promise<void>((resolve) => server.close(() => resolve()));
      rmSync(root, { recursive: true, force: true });
    },
  };
}
