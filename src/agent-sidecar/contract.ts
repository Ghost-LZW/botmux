/**
 * botmux-goal-v1 wire contract — types + canonical request hash.
 *
 * CANONICAL source: docs mirrored in ./README.md (and consumed verbatim by the
 * motivation repo).  This file is the machine half: the wire types of spec §2,
 * the canonical-JSON request hash of §3, the runId shape of §1 and the error
 * codes of §2.  Types + pure functions only — no IO, no engine imports, so the
 * peer repo can mirror it byte-for-byte.
 */

import { createHash } from 'node:crypto';

export const BOTMUX_GOAL_PROTOCOL = 'botmux-goal-v1';

/** runId shape (spec §1).  MUST be validated BEFORE any path join. */
export const SIDECAR_RUN_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$/;

export function isValidSidecarRunId(runId: unknown): runId is string {
  return typeof runId === 'string' && SIDECAR_RUN_ID_RE.test(runId);
}

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

/** Error codes (spec §2) with their HTTP status. */
export const SIDECAR_ERROR_CODES = {
  UNKNOWN_RUN: 404,
  IDEMPOTENCY_CONFLICT: 409,
  HASH_MISMATCH: 400,
  MALFORMED_REQUEST: 400,
  UNKNOWN_PROFILE: 400,
  PROFILE_NOT_SANDBOXED: 403,
  PROFILE_NOT_DISCOVERY_SAFE: 403,
  CWD_NOT_ALLOWED: 403,
  INTERNAL: 500,
} as const;

export type SidecarErrorCode = keyof typeof SIDECAR_ERROR_CODES;

// ─── Canonical request hash (spec §3 — MUST match the peer repo byte-for-byte) ──

/**
 * Recursive canonical JSON: undefined fields omitted, object keys sorted by
 * UTF-16 code unit (`Object.keys().sort()`), no whitespace, arrays keep order,
 * strings/numbers via standard JSON.stringify escaping, non-finite numbers throw.
 */
export function canonicalJson(value: unknown): string {
  if (value === null) return 'null';
  const t = typeof value;
  if (t === 'string' || t === 'boolean') return JSON.stringify(value);
  if (t === 'number') {
    if (!Number.isFinite(value as number)) throw new Error('canonicalJson: non-finite number');
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => canonicalJson(v === undefined ? null : v)).join(',')}]`;
  }
  if (t === 'object') {
    const obj = value as Record<string, unknown>;
    const parts: string[] = [];
    for (const k of Object.keys(obj).sort()) {
      const v = obj[k];
      if (v === undefined) continue;
      parts.push(`${JSON.stringify(k)}:${canonicalJson(v)}`);
    }
    return `{${parts.join(',')}}`;
  }
  throw new Error(`canonicalJson: unsupported type ${t}`);
}

/** sha256 hex over canonicalJson(request minus `requestHash`). */
export function canonicalRequestHash(request: Record<string, unknown>): string {
  const { requestHash: _omitted, ...rest } = request;
  return createHash('sha256').update(canonicalJson(rest)).digest('hex');
}
