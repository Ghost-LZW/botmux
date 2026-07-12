/**
 * botmux-goal-v1 HTTP surface — raw node:http over a Unix domain socket.
 *
 * Endpoints (spec §1):
 *   GET  /v1/health                       → protocol + capabilities
 *   POST /v1/runs                         → create-or-attach (201 / 200 / 409 / 400 / 403)
 *   GET  /v1/runs/:id/events?since=N[&follow=0] → seq-gapless NDJSON replay/follow
 *   GET  /v1/runs/:id/result              → 202 running / 200 durable terminal record
 *   POST /v1/runs/:id/cancel              → idempotent cancel
 *
 * House router style: handleXxx(req, res, url, deps) => Promise<boolean>.
 * v1 trust surface is the UDS file permission (parent dir 0700, socket 0600,
 * stale socket unlinked on start); no token.  Unknown runId is ALWAYS 404
 * UNKNOWN_RUN — never a 200 with an empty shape.  runId is validated against
 * the spec regex BEFORE any path join.
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { connect as netConnect } from 'node:net';
import { chmodSync, existsSync, mkdirSync, unlinkSync } from 'node:fs';
import { dirname } from 'node:path';

import {
  BOTMUX_GOAL_PROTOCOL,
  SIDECAR_ERROR_CODES,
  canonicalRequestHash,
  isValidSidecarRunId,
  type SidecarErrorCode,
  type SidecarRunRequest,
} from './contract.js';
import { deriveWireFrames } from './events.js';
import type { RunStore } from './run-store.js';
import { SidecarGateError, type RunDriver } from './driver.js';

export interface SidecarServerDeps {
  store: RunStore;
  driver: RunDriver;
  /** follow-mode journal poll interval (ms); tests shrink it. */
  followPollMs?: number;
  log?: (msg: string) => void;
}

const MAX_BODY_BYTES = 1024 * 1024;

function jsonRes(res: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) });
  res.end(payload);
}

function errRes(
  res: ServerResponse,
  code: SidecarErrorCode,
  message: string,
  extra?: { expectedHash?: string; receivedHash?: string },
): void {
  jsonRes(res, SIDECAR_ERROR_CODES[code], { error: { code, message, ...extra } });
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let bytes = 0;
    req.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      if (bytes > MAX_BODY_BYTES) {
        reject(new Error('request body too large'));
        req.destroy();
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf-8')));
    req.on('error', reject);
  });
}

const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** Shape-validate a POST /v1/runs body.  Returns a problem string on failure. */
function validateRunRequest(body: unknown): SidecarRunRequest | string {
  if (typeof body !== 'object' || body === null || Array.isArray(body)) return 'body must be a JSON object';
  const o = body as Record<string, unknown>;
  if (o.protocol !== BOTMUX_GOAL_PROTOCOL) return `protocol must be "${BOTMUX_GOAL_PROTOCOL}"`;
  if (!isValidSidecarRunId(o.runId)) return 'runId must match ^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$';
  if (typeof o.requestHash !== 'string' || !/^[0-9a-f]{64}$/.test(o.requestHash)) return 'requestHash must be sha256 hex';
  if (typeof o.profileRef !== 'string' || o.profileRef === '') return 'profileRef must be a non-empty string';
  if (typeof o.goal !== 'string' || o.goal === '') return 'goal must be a non-empty string';
  if (typeof o.cwd !== 'string' || o.cwd === '') return 'cwd must be a non-empty string';
  if (typeof o.timeoutMs !== 'number' || !Number.isFinite(o.timeoutMs) || o.timeoutMs <= 0) return 'timeoutMs must be a positive number';
  if (o.mode !== 'discovery') return 'mode must be "discovery" (the only mode botmux-goal-v1 accepts)';
  if (o.taskId !== undefined && typeof o.taskId !== 'string') return 'taskId must be a string when present';
  if (o.threadId !== undefined && typeof o.threadId !== 'string') return 'threadId must be a string when present';
  return {
    protocol: BOTMUX_GOAL_PROTOCOL,
    runId: o.runId,
    requestHash: o.requestHash,
    profileRef: o.profileRef,
    goal: o.goal,
    cwd: o.cwd,
    timeoutMs: o.timeoutMs,
    mode: 'discovery',
    ...(o.taskId !== undefined ? { taskId: o.taskId as string } : {}),
    ...(o.threadId !== undefined ? { threadId: o.threadId as string } : {}),
  };
}

async function handleCreateRun(
  req: IncomingMessage,
  res: ServerResponse,
  deps: SidecarServerDeps,
): Promise<void> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(await readBody(req));
  } catch {
    errRes(res, 'MALFORMED_REQUEST', 'body is not valid JSON');
    return;
  }
  const request = validateRunRequest(parsed);
  if (typeof request === 'string') {
    errRes(res, 'MALFORMED_REQUEST', request);
    return;
  }

  // Server ALWAYS recomputes the hash (§3); mismatch never touches the ledger.
  const computed = canonicalRequestHash(request as unknown as Record<string, unknown>);
  if (computed !== request.requestHash) {
    errRes(res, 'HASH_MISMATCH', 'requestHash does not match canonical hash of payload', {
      expectedHash: computed,
      receivedHash: request.requestHash,
    });
    return;
  }

  const stored = deps.store.exists(request.runId) ? deps.store.readRequest(request.runId) : undefined;
  if (stored) {
    if (stored.requestHash !== request.requestHash) {
      errRes(res, 'IDEMPOTENCY_CONFLICT', `runId "${request.runId}" exists with a different request`, {
        expectedHash: stored.requestHash,
        receivedHash: request.requestHash,
      });
      return;
    }
    jsonRes(res, 200, await deps.driver.attach(request.runId));
    return;
  }

  jsonRes(res, 201, deps.driver.create(request));
}

async function handleEvents(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  runId: string,
  deps: SidecarServerDeps,
): Promise<void> {
  const sinceRaw = url.searchParams.get('since') ?? '0';
  const since = Number(sinceRaw);
  if (!Number.isInteger(since) || since < 0) {
    errRes(res, 'MALFORMED_REQUEST', 'since must be a non-negative integer');
    return;
  }
  const follow = url.searchParams.get('follow') !== '0';
  const pollMs = deps.followPollMs ?? 200;

  res.writeHead(200, { 'content-type': 'application/x-ndjson' });
  let closed = false;
  res.on('close', () => { closed = true; });

  let lastSent = since;
  let terminalSent = false;
  for (;;) {
    const terminal = await deps.driver.ensureFinalized(runId);
    const frames = deriveWireFrames(deps.store.readJournalEvents(runId), terminal);
    for (const frame of frames) {
      if (frame.seq <= lastSent) continue;
      res.write(JSON.stringify(frame) + '\n');
      lastSent = frame.seq;
      if (frame.event.type === 'terminal') terminalSent = true;
    }
    if (terminalSent || !follow || closed) break;
    await sleep(pollMs);
  }
  res.end();
}

export async function handleSidecarApi(
  req: IncomingMessage,
  res: ServerResponse,
  url: URL,
  deps: SidecarServerDeps,
  startedAt: number,
): Promise<boolean> {
  if (req.method === 'GET' && url.pathname === '/v1/health') {
    jsonRes(res, 200, {
      protocol: BOTMUX_GOAL_PROTOCOL,
      capabilities: { input: false, human: false },
      pid: process.pid,
      startedAt,
    });
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/v1/runs') {
    try {
      await handleCreateRun(req, res, deps);
    } catch (err) {
      if (err instanceof SidecarGateError) errRes(res, err.code, err.message);
      else throw err;
    }
    return true;
  }

  const m = url.pathname.match(/^\/v1\/runs\/([^/]+)\/(events|result|cancel)$/);
  if (!m) return false;
  const runId = decodeURIComponent(m[1]!);
  const action = m[2]!;

  // Validate BEFORE any path join (traversal defense); an ill-shaped or
  // unknown runId is indistinguishable on the wire: 404 UNKNOWN_RUN.
  if (!isValidSidecarRunId(runId) || !deps.store.exists(runId)) {
    errRes(res, 'UNKNOWN_RUN', `unknown run "${runId}"`);
    return true;
  }

  if (action === 'events' && req.method === 'GET') {
    await handleEvents(req, res, url, runId, deps);
    return true;
  }

  if (action === 'result' && req.method === 'GET') {
    const record = await deps.driver.ensureFinalized(runId);
    if (!record) {
      jsonRes(res, 202, { runId, state: 'running' });
      return true;
    }
    // Serve the persisted bytes so every replay of the record is identical.
    const raw = deps.store.readTerminalRaw(runId);
    if (raw) {
      res.writeHead(200, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(raw) });
      res.end(raw);
    } else {
      jsonRes(res, 200, record);
    }
    return true;
  }

  if (action === 'cancel' && req.method === 'POST') {
    try {
      const outcome = await deps.driver.cancel(runId);
      jsonRes(res, outcome.alreadyTerminal ? 200 : 202, outcome);
    } catch (err) {
      if (err instanceof SidecarGateError) errRes(res, err.code, err.message);
      else throw err;
    }
    return true;
  }

  return false;
}

export function createSidecarServer(deps: SidecarServerDeps): Server {
  const startedAt = Date.now();
  const log = deps.log ?? ((msg: string) => console.error(msg));
  const server = createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', 'http://sidecar.local');
      const handled = await handleSidecarApi(req, res, url, deps, startedAt);
      if (!handled && !res.headersSent) {
        errRes(res, 'MALFORMED_REQUEST', `no such endpoint: ${req.method} ${url.pathname}`);
      }
    } catch (err) {
      log(`agent-sidecar: request error: ${err instanceof Error ? err.message : String(err)}`);
      if (!res.headersSent) errRes(res, 'INTERNAL', 'internal error');
      else res.end();
    }
  });
  return server;
}

/** Probe an existing socket file for a live listener.  connect() succeeding
 *  (or refusing us for any reason OTHER than ECONNREFUSED/ENOENT) means some
 *  process still owns the address. */
function socketHasLiveListener(socketPath: string, timeoutMs = 500): Promise<boolean> {
  return new Promise((resolve) => {
    const sock = netConnect(socketPath);
    const settle = (live: boolean): void => {
      sock.removeAllListeners();
      sock.destroy();
      resolve(live);
    };
    sock.setTimeout(timeoutMs, () => settle(true)); // unresponsive but bound: treat as live
    sock.once('connect', () => settle(true));
    sock.once('error', (err: NodeJS.ErrnoException) => {
      // ECONNREFUSED = bound-but-dead, ENOENT = vanished, ENOTSOCK = a plain
      // file squatting on the path: all provably stale. Anything else (e.g.
      // EACCES, EAGAIN) errs on the side of "someone owns this".
      settle(!(err.code === 'ECONNREFUSED' || err.code === 'ENOENT' || err.code === 'ENOTSOCK'));
    });
  });
}

/**
 * Bind on a Unix domain socket: parent dir created 0700, 'error' listener
 * installed BEFORE listen (an unhandled listener 'error' crashes the process —
 * repo hard-won lesson), socket chmod 0600.
 *
 * Single-owner guard: an existing socket file is probed first — a LIVE
 * listener means another sidecar owns this address and startup FAILS HARD
 * (silently unlinking would steal the address while the old process keeps
 * driving its runs, breaking cancel and the single-runtime-loop journal
 * serialization).  Only a provably stale socket (ECONNREFUSED/ENOENT) is
 * unlinked.
 */
export async function listenOnSocket(server: Server, socketPath: string): Promise<void> {
  const dir = dirname(socketPath);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true, mode: 0o700 });
  if (existsSync(socketPath)) {
    if (await socketHasLiveListener(socketPath)) {
      throw new Error(
        `agent-sidecar: another process is already listening on ${socketPath} — refusing to steal the address`,
      );
    }
    try {
      unlinkSync(socketPath);
    } catch { /* a real conflict resurfaces as a listen error below */ }
  }
  await new Promise<void>((resolve, reject) => {
    const onError = (err: Error): void => reject(err);
    server.once('error', onError);
    server.listen(socketPath, () => {
      server.removeListener('error', onError);
      resolve();
    });
  });
  // Keep a persistent handler so a post-listen socket error can't crash us.
  server.on('error', (err) => console.error(`agent-sidecar: server error: ${String(err)}`));
  try {
    chmodSync(socketPath, 0o600);
  } catch { /* best-effort — dir mode 0700 is the primary trust boundary */ }
}
