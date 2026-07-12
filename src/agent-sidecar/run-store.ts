/**
 * Durable run ledger (spec §4) — the crash-safety root of the sidecar.
 *
 *   <runsRoot>/<runId>/
 *     request.json   — original SidecarRunRequest (atomic; BEFORE any spawn)
 *     session.json   — frozen cost anchor { cliId, sessionId, cwd } (BEFORE spawn)
 *     lease.json     — { pid, startedAt } active-driver lock (attach fencing)
 *     cancel.json    — cancel marker (idempotent; written BEFORE abort fires)
 *     terminal.json  — SidecarTerminalRecord (atomic; BEFORE any /result 200)
 *     v3/<runId>/…   — embedded v3 engine runDir (journal.ndjson etc., engine-owned)
 *
 * Invariants owned here: ledger-before-spawn, terminal-before-ack, and the
 * idempotent finalize that can rebuild terminal.json from a terminal v3
 * journal after a crash (same logical terminal — state + error code — every
 * time; only ts-class fields may differ).
 *
 * Callers MUST validate runId against SIDECAR_RUN_ID_RE before touching the
 * store; every path join here re-asserts it (traversal defense in depth).
 */

import { existsSync, mkdirSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';

import { atomicWriteFileSync } from '../utils/atomic-write.js';
import { readJournal, type StoredEvent, type V3ErrorClass } from '../workflows/v3/journal.js';
import type { ValidateManifest } from '../workflows/v3/contract.js';
import {
  BOTMUX_GOAL_PROTOCOL,
  isValidSidecarRunId,
  type SidecarArtifact,
  type SidecarRunRequest,
  type SidecarTerminalRecord,
  type SidecarUsage,
} from './contract.js';
import { terminalSeqFor } from './events.js';

export interface RunLease {
  pid: number;
  startedAt: number;
}

/** Cost anchor frozen BEFORE spawn: the claude transcript path is a pure
 *  function of these three (spec §8).  Never contains credentials. */
export interface FrozenSession {
  cliId: string;
  sessionId: string;
  cwd: string;
}

export interface CancelMarker {
  requestedAt: number;
}

/** Best-effort per-run usage collection after terminal (spec §8).  Receives
 *  ONE FrozenSession per attempt of the run (same cliId/cwd, per-attempt
 *  sessionId — derived from the journal's nodeSessionReady lines, frozen
 *  anchor as the pre-spawn fallback) and must aggregate across ALL of them:
 *  a retry's usage is part of the run's cost.  Real wiring never knows usd →
 *  costComplete always false; tests inject a fake. */
export type CollectUsage = (
  sessions: FrozenSession[],
) => { usage?: SidecarUsage; costComplete: boolean } | Promise<{ usage?: SidecarUsage; costComplete: boolean }>;

/** costComplete may be attested by a collector, but the RECORD-level gate is
 *  owned here (wire contract: true ONLY if usage is fully collected AND a
 *  normalized usd is present): every field of a complete record must be sane —
 *  token buckets and turns are non-negative integers, usd is finite and
 *  non-negative, model is a non-empty string.  Anything less (NaN turns would
 *  persist as null and poison the consumer's CostRecord) keeps the usage for
 *  observability but pins costComplete to false regardless of the collector. */
export function usageSatisfiesCostComplete(usage: SidecarUsage | undefined): boolean {
  if (!usage) return false;
  const intNonNeg = (n: unknown): boolean => typeof n === 'number' && Number.isInteger(n) && n >= 0;
  const finiteNonNeg = (n: unknown): boolean => typeof n === 'number' && Number.isFinite(n) && n >= 0;
  return (
    typeof usage.model === 'string' &&
    usage.model !== '' &&
    intNonNeg(usage.inputTokens) &&
    intNonNeg(usage.outputTokens) &&
    intNonNeg(usage.cacheReadTokens) &&
    intNonNeg(usage.cacheCreationTokens) &&
    intNonNeg(usage.turns) &&
    finiteNonNeg(usage.usd)
  );
}

export function defaultRunsRoot(env: NodeJS.ProcessEnv = process.env): string {
  return env.BOTMUX_AGENT_RUNS_DIR || join(homedir(), '.botmux', 'agent-runs');
}

export interface RunStore {
  runsRoot: string;
  runDir(runId: string): string;
  exists(runId: string): boolean;
  readRequest(runId: string): SidecarRunRequest | undefined;
  writeRequest(request: SidecarRunRequest): void;
  readSession(runId: string): FrozenSession | undefined;
  writeSession(runId: string, session: FrozenSession): void;
  readLease(runId: string): RunLease | undefined;
  writeLease(runId: string, lease: RunLease): void;
  readCancel(runId: string): CancelMarker | undefined;
  writeCancel(runId: string): CancelMarker;
  readTerminal(runId: string): SidecarTerminalRecord | undefined;
  readTerminalRaw(runId: string): string | undefined;
  writeTerminal(record: SidecarTerminalRecord): void;
  /** The v3 engine baseDir for this run; the engine's own runDir nests inside. */
  engineBaseDir(runId: string): string;
  journalPath(runId: string): string;
  readJournalEvents(runId: string): StoredEvent[];
}

function readJson<T>(path: string): T | undefined {
  if (!existsSync(path)) return undefined;
  try {
    return JSON.parse(readFileSync(path, 'utf-8')) as T;
  } catch {
    return undefined; // torn write of a non-atomic reader window — treat as absent
  }
}

export function createRunStore(runsRoot: string): RunStore {
  const runDir = (runId: string): string => {
    if (!isValidSidecarRunId(runId)) throw new Error(`run-store: invalid runId ${JSON.stringify(runId)}`);
    return join(runsRoot, runId);
  };
  const file = (runId: string, name: string): string => join(runDir(runId), name);
  const journalPath = (runId: string): string => join(file(runId, 'v3'), runId, 'journal.ndjson');

  return {
    runsRoot,
    runDir,
    exists: (runId) => existsSync(file(runId, 'request.json')),
    readRequest: (runId) => readJson<SidecarRunRequest>(file(runId, 'request.json')),
    writeRequest: (request) => {
      const dir = runDir(request.runId);
      mkdirSync(dir, { recursive: true });
      atomicWriteFileSync(join(dir, 'request.json'), JSON.stringify(request, null, 2));
    },
    readSession: (runId) => readJson<FrozenSession>(file(runId, 'session.json')),
    writeSession: (runId, session) => {
      atomicWriteFileSync(file(runId, 'session.json'), JSON.stringify(session, null, 2));
    },
    readLease: (runId) => readJson<RunLease>(file(runId, 'lease.json')),
    writeLease: (runId, lease) => {
      atomicWriteFileSync(file(runId, 'lease.json'), JSON.stringify(lease, null, 2));
    },
    readCancel: (runId) => readJson<CancelMarker>(file(runId, 'cancel.json')),
    writeCancel: (runId) => {
      const existing = readJson<CancelMarker>(file(runId, 'cancel.json'));
      if (existing) return existing; // idempotent — first request's ts wins
      const marker: CancelMarker = { requestedAt: Date.now() };
      atomicWriteFileSync(file(runId, 'cancel.json'), JSON.stringify(marker, null, 2));
      return marker;
    },
    readTerminal: (runId) => readJson<SidecarTerminalRecord>(file(runId, 'terminal.json')),
    readTerminalRaw: (runId) => {
      const p = file(runId, 'terminal.json');
      return existsSync(p) ? readFileSync(p, 'utf-8') : undefined;
    },
    writeTerminal: (record) => {
      atomicWriteFileSync(file(record.runId, 'terminal.json'), JSON.stringify(record, null, 2));
    },
    engineBaseDir: (runId) => file(runId, 'v3'),
    journalPath,
    readJournalEvents: (runId) => readJournal(journalPath(runId)),
  };
}

// ─── Terminal record building / idempotent finalize ─────────────────────────

const ERROR_CLASS_CODE: Record<V3ErrorClass, string> = {
  workerError: 'WORKER_ERROR',
  manifestInvalid: 'MANIFEST_INVALID',
  resultInvalid: 'RESULT_INVALID',
  timeout: 'TIMEOUT',
  gateRejected: 'GATE_REJECTED',
  cancelled: 'CANCELLED',
};

/** Journal-level run terminal, before cancel folding. */
export function journalTerminalState(events: StoredEvent[]): 'succeeded' | 'failed' | 'blocked' | undefined {
  for (let i = events.length - 1; i >= 0; i--) {
    const t = events[i]!.type;
    if (t === 'runSucceeded') return 'succeeded';
    if (t === 'runFailed') return 'failed';
    if (t === 'runBlocked') return 'blocked';
  }
  return undefined;
}

export interface FinalizeDeps {
  validateManifest: ValidateManifest;
  collectUsage?: CollectUsage;
  now?: () => number;
}

export interface FinalizeOptions {
  /** Drive-crash fallback: fold a NON-terminal journal (no cancel marker) to a
   *  failed record instead of returning undefined.  Only the in-process drive
   *  error path may use this — a reader must never invent a terminal. */
  forceFailure?: { code: string; message: string };
}

/**
 * Idempotent finalize (spec §4 invariant 3).  Returns the existing
 * terminal.json when present; otherwise rebuilds it from the journal (+ the
 * cancel marker, folding per spec §7) and persists it atomically.  Returns
 * undefined when the run is not finalizable yet (journal non-terminal and no
 * cancel/force reason) — the caller keeps answering 202.
 */
export async function finalizeRun(
  store: RunStore,
  runId: string,
  deps: FinalizeDeps,
  opts: FinalizeOptions = {},
): Promise<SidecarTerminalRecord | undefined> {
  const existing = store.readTerminal(runId);
  if (existing) return existing;

  const events = store.readJournalEvents(runId);
  const cancel = store.readCancel(runId);
  const journalState = journalTerminalState(events);
  if (!journalState && !cancel && !opts.forceFailure) return undefined;

  const now = deps.now ?? Date.now;
  // Cancel folding (§7): failed / never-terminal + cancel marker → cancelled.
  // A run that genuinely succeeded or self-reported blocked before the cancel
  // took effect keeps its honest state (deterministic across rebuilds).
  const state: SidecarTerminalRecord['state'] =
    journalState === 'succeeded' ? 'succeeded'
    : journalState === 'blocked' ? 'blocked'
    : cancel ? 'cancelled'
    : 'failed';

  let summary = '';
  let error: SidecarTerminalRecord['error'];
  let ask: SidecarTerminalRecord['ask'];
  let artifacts: SidecarArtifact[] = [];

  if (state === 'succeeded') {
    const succeeded = [...events].reverse().find((e) => e.type === 'nodeSucceeded');
    if (succeeded && succeeded.type === 'nodeSucceeded') {
      // Re-validate at finalize time (crash-rebuild path has no in-memory
      // verdict; artifacts may ONLY come from a validator-passed manifest).
      const outputDir = join(dirname(succeeded.manifestPath), 'work');
      const verdict = await deps.validateManifest(succeeded.manifestPath, outputDir);
      if (verdict.ok && verdict.manifest) {
        summary = verdict.manifest.summary;
        artifacts = verdict.manifest.files.map((f) => ({
          name: f.name, path: f.path, kind: f.kind, bytes: f.bytes, sha256: f.sha256, mime: f.mime,
        }));
      } else {
        summary = `manifest re-validation failed: ${(verdict.problems ?? []).join('; ')}`;
      }
    } else {
      summary = 'run succeeded';
    }
  } else if (state === 'blocked') {
    const blocked = [...events].reverse().find((e) => e.type === 'nodeBlocked');
    if (blocked && blocked.type === 'nodeBlocked') {
      const code = blocked.errorCode ?? ERROR_CLASS_CODE[blocked.errorClass];
      summary = blocked.message ?? `blocked [${code}]`;
      error = { code, message: blocked.message ?? summary, retryable: true };
      if (blocked.ask) {
        ask = blocked.ask.freeText === true
          ? { question: blocked.ask.question, freeText: true }
          : { question: blocked.ask.question, options: blocked.ask.options };
      }
    } else {
      summary = 'run blocked';
      error = { code: 'BLOCKED', message: summary, retryable: true };
    }
  } else if (state === 'cancelled') {
    summary = 'run cancelled';
    error = { code: 'CANCELLED', message: 'run cancelled by caller', retryable: false };
  } else {
    const failed = [...events].reverse().find((e) => e.type === 'nodeFailed');
    if (failed && failed.type === 'nodeFailed') {
      const code = failed.errorCode ?? ERROR_CLASS_CODE[failed.errorClass];
      summary = failed.message ?? `failed [${code}]`;
      error = { code, message: failed.message ?? summary, retryable: false };
    } else if (opts.forceFailure) {
      summary = opts.forceFailure.message;
      error = { code: opts.forceFailure.code, message: opts.forceFailure.message, retryable: false };
    } else {
      const runFailed = [...events].reverse().find((e) => e.type === 'runFailed');
      const detail = runFailed && runFailed.type === 'runFailed' ? runFailed.detail : undefined;
      summary = detail ?? 'run failed';
      error = { code: 'WORKER_ERROR', message: summary, retryable: false };
    }
  }

  // Session identity: journal truth first (survives retries), frozen anchor as
  // the pre-spawn fallback.  Never webPort/token — the journal already omits
  // the token and we drop webPort here.
  const frozen = store.readSession(runId);
  const attemptSessionIds: string[] = [];
  for (const e of events) {
    if (e.type === 'nodeSessionReady' && !attemptSessionIds.includes(e.sessionInfo.sessionId)) {
      attemptSessionIds.push(e.sessionInfo.sessionId);
    }
  }
  const sessionId = attemptSessionIds[attemptSessionIds.length - 1] ?? frozen?.sessionId;

  // Cost scope = the WHOLE run: one FrozenSession per attempt (a retry's
  // usage is still this run's spend).  The record-level costComplete gate is
  // usageSatisfiesCostComplete — a collector cannot attest completeness for a
  // record with missing/non-finite buckets or absent usd (wire contract §8).
  let usage: SidecarUsage | undefined;
  let costComplete = false;
  if (deps.collectUsage && frozen) {
    const sessions: FrozenSession[] = (attemptSessionIds.length > 0 ? attemptSessionIds : [frozen.sessionId])
      .map((sid) => ({ cliId: frozen.cliId, cwd: frozen.cwd, sessionId: sid }));
    try {
      const collected = await deps.collectUsage(sessions);
      usage = collected.usage;
      costComplete = collected.costComplete === true && usageSatisfiesCostComplete(collected.usage);
    } catch {
      // Honest cost stance: collection failure → absent usage, never zeros.
    }
  }

  const startedAt = events[0]?.ts ?? store.readLease(runId)?.startedAt ?? now();
  const record: SidecarTerminalRecord = {
    protocol: BOTMUX_GOAL_PROTOCOL,
    runId,
    state,
    summary,
    ...(error ? { error } : {}),
    ...(ask ? { ask } : {}),
    artifacts,
    ...(sessionId ? { sessionId } : {}),
    ...(usage ? { usage } : {}),
    costComplete,
    startedAt,
    finishedAt: now(),
    lastSeq: terminalSeqFor(events),
  };

  // Lost race with a concurrent finalize → keep the first write (idempotent).
  const raced = store.readTerminal(runId);
  if (raced) return raced;
  store.writeTerminal(record);
  return record;
}
