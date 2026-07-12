/**
 * Run driver — composes the EXISTING v3 engine into single-goal-run semantics.
 *
 * One sidecar run = a single-node goal DAG through `runWorkflow` (runDir
 * ledger, journal, manifest validation, ASK_HUMAN classification and replay
 * resumability all come from the engine; nothing is reinvented here).  The
 * driver owns what the engine does not have (recon gaps): the wire-level
 * admission gates (spec §9 — checked BEFORE any ledger write), create-or-attach
 * with lease fencing (§4), cancel by runId (§7), and the terminal record
 * finalize (delegated to run-store).
 *
 * Security gates run in this order, all before mkdir of the runDir:
 *   profileRef resolves → UNKNOWN_PROFILE
 *   snapshot.sandbox === true → PROFILE_NOT_SANDBOXED
 *   realpath(cwd) inside allowedWorkspaceRoots (default empty = deny all,
 *   symlink escape resolved) → CWD_NOT_ALLOWED
 * `disableCliBypass` passes through the snapshot untouched (P2 red line).
 */

import { realpathSync } from 'node:fs';
import { homedir } from 'node:os';
import { join, sep } from 'node:path';

import { validateDag } from '../workflows/v3/dag.js';
import {
  latestAttemptIdFor,
  nextAttemptIdFor,
  runWorkflow,
  type V3RuntimeDeps,
} from '../workflows/v3/runtime.js';
import { appendEvent, readJournal } from '../workflows/v3/journal.js';
import { materialize } from '../workflows/v3/state.js';
import { syntheticSessionUuid } from '../workflows/daemon-spawn.js';
import { getClaudeSessionJsonlPath } from '../services/transcript-resolver.js';
import { readSessionTokenUsageFile } from '../core/cost-calculator.js';
import type { BotSnapshot, RunNode, ValidateManifest } from '../workflows/v3/contract.js';
import {
  BOTMUX_GOAL_PROTOCOL,
  type SidecarCancelResponse,
  type SidecarErrorCode,
  type SidecarRunAccepted,
  type SidecarRunRequest,
  type SidecarRunState,
  type SidecarTerminalRecord,
} from './contract.js';
import {
  finalizeRun,
  journalTerminalState,
  type CollectUsage,
  type FrozenSession,
  type RunStore,
} from './run-store.js';

/** The single goal node's id inside the embedded v3 DAG. */
export const SIDECAR_NODE_ID = 'goal';

/** Typed admission failure — the server maps `code` to its HTTP status. */
export class SidecarGateError extends Error {
  constructor(public readonly code: SidecarErrorCode, message: string) {
    super(message);
    this.name = 'SidecarGateError';
  }
}

export interface RunDriverDeps {
  store: RunStore;
  /** profileRef → secret-free BotSnapshot; undefined = UNKNOWN_PROFILE. */
  resolveProfile: (profileRef: string) => BotSnapshot | undefined;
  /** realpath-containment allowlist for request.cwd.  Empty = deny all. */
  allowedWorkspaceRoots: string[];
  runNode: RunNode;
  validateManifest: ValidateManifest;
  collectUsage?: CollectUsage;
  /** When set, EVERY create/attach admission fails 403 REAL_RUNS_DISABLED with
   *  this reason.  The v1 REAL composition root sets it unconditionally: the
   *  current botmux sandbox still leaves daemon-mediated egress open (relay
   *  outbox `botmux send` via host watcher; rw-bound real auth paths), so the
   *  profile flags are necessary-but-NOT-sufficient and no real execution
   *  face may be exposed.  Contract tests inject fake runNodes and leave this
   *  unset. */
  realRunsDisabledReason?: string;
  log?: (msg: string) => void;
}

export interface RunDriver {
  /** Admission gates + ledger-before-spawn + background drive.  Synchronous up
   *  to (and including) the ledger writes so concurrent creates can't race. */
  create(request: SidecarRunRequest): SidecarRunAccepted;
  /** Same runId + same hash re-POST: attach.  Re-drives a dead-lease
   *  non-terminal run (journal replay makes the new attempt a retry). */
  attach(runId: string): Promise<SidecarRunAccepted>;
  cancel(runId: string): Promise<SidecarCancelResponse>;
  /** terminal.json if present, else rebuild when legitimately finalizable. */
  ensureFinalized(runId: string): Promise<SidecarTerminalRecord | undefined>;
  isLive(runId: string): boolean;
  /** Test/shutdown hook: the in-process drive promise for a live run. */
  settled(runId: string): Promise<void> | undefined;
}

/** The deterministic first-attempt session id (pool mints
 *  syntheticSessionUuid(`v3-${runId}-${attemptId}`); first dispatch of the
 *  single node is `goal#001/attempts/001`).  Frozen pre-spawn as the cost
 *  anchor; the journal's nodeSessionReady overrides it if attempts differ. */
export function frozenSessionIdFor(runId: string): string {
  return syntheticSessionUuid(`v3-${runId}-${SIDECAR_NODE_ID}#001/attempts/001`);
}

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err) {
    return (err as NodeJS.ErrnoException).code === 'EPERM';
  }
}

export function createRunDriver(deps: RunDriverDeps): RunDriver {
  const { store } = deps;
  const log = deps.log ?? (() => {});
  const live = new Map<string, { controller: AbortController; done: Promise<void> }>();
  const finalizeDeps = { validateManifest: deps.validateManifest, collectUsage: deps.collectUsage };

  const accepted = (runId: string, state: SidecarRunState, created: boolean): SidecarRunAccepted => ({
    protocol: BOTMUX_GOAL_PROTOCOL,
    runId,
    state,
    created,
    capabilities: { input: false, human: false },
  });

  /** Spec §9 gates — MUST all pass before any ledger write. */
  const admit = (request: SidecarRunRequest): { snapshot: BotSnapshot; realCwd: string } => {
    if (deps.realRunsDisabledReason) {
      throw new SidecarGateError('REAL_RUNS_DISABLED', deps.realRunsDisabledReason);
    }
    const profile = deps.resolveProfile(request.profileRef);
    if (!profile) {
      throw new SidecarGateError('UNKNOWN_PROFILE', `unknown profile "${request.profileRef}"`);
    }
    if (profile.sandbox !== true) {
      throw new SidecarGateError('PROFILE_NOT_SANDBOXED', `profile "${request.profileRef}" is not sandboxed`);
    }
    // Discovery-safe gate (v1 accepts only mode:'discovery', and enforces it at
    // the EXECUTING boundary, not just the caller): the physical sandbox must
    // have network egress disabled (bwrap net unshare) and the no-escalation
    // red line armed, or a "discovery" run could still produce external side
    // effects. sandboxNetwork defaults to ON in botmux, so require the explicit
    // false; disableCliBypass can never be cleared downstream (P2 red line).
    if (profile.sandboxNetwork !== false || profile.disableCliBypass !== true) {
      throw new SidecarGateError(
        'PROFILE_NOT_DISCOVERY_SAFE',
        `profile "${request.profileRef}" is not discovery-safe: requires sandboxNetwork=false and disableCliBypass=true`,
      );
    }
    let realCwd: string;
    try {
      realCwd = realpathSync(request.cwd);
    } catch {
      throw new SidecarGateError('CWD_NOT_ALLOWED', `cwd does not resolve: ${request.cwd}`);
    }
    const inside = deps.allowedWorkspaceRoots.some((root) => {
      let realRoot: string;
      try {
        realRoot = realpathSync(root);
      } catch {
        return false;
      }
      return realCwd === realRoot || realCwd.startsWith(realRoot + sep);
    });
    if (!inside) {
      throw new SidecarGateError('CWD_NOT_ALLOWED', 'cwd resolves outside allowed workspace roots');
    }
    // Freeze the run's working dir to the REALPATH (the claude transcript
    // project key is realpath-derived — spec §8).
    return { snapshot: { ...profile, workingDir: realCwd }, realCwd };
  };

  const drive = (request: SidecarRunRequest, snapshot: BotSnapshot): void => {
    const runId = request.runId;
    const controller = new AbortController();
    const entry = { controller, done: Promise.resolve() };
    live.set(runId, entry);
    entry.done = (async () => {
      try {
        if (store.readCancel(runId)) controller.abort(); // cancel won before spawn
        const dag = validateDag({
          runId,
          nodes: [{
            id: SIDECAR_NODE_ID,
            type: 'goal',
            goal: request.goal,
            depends: [],
            inputs: [],
            timeoutSec: Math.max(1, Math.ceil(request.timeoutMs / 1000)),
          }],
        });
        const runtimeDeps: V3RuntimeDeps = {
          runNode: deps.runNode,
          validateManifest: deps.validateManifest,
          resolveBotSnapshot: () => snapshot,
        };
        await runWorkflow(dag, runtimeDeps, {
          baseDir: store.engineBaseDir(runId),
          cancelSignal: controller.signal,
        });
        await finalizeRun(store, runId, finalizeDeps);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        log(`agent-sidecar: drive for run ${runId} threw: ${message}`);
        try {
          await finalizeRun(store, runId, finalizeDeps, {
            forceFailure: { code: 'WORKER_ERROR', message: `drive error: ${message}` },
          });
        } catch (finalizeErr) {
          log(`agent-sidecar: finalize for run ${runId} failed: ${String(finalizeErr)}`);
        }
      } finally {
        live.delete(runId);
      }
    })();
  };

  const isLive = (runId: string): boolean => {
    if (live.has(runId)) return true;
    const lease = store.readLease(runId);
    // A foreign pid holding the lease means another sidecar process is driving
    // (spec §4 invariant 4).  Our own pid without a live entry = settled/stale.
    return !!lease && lease.pid !== process.pid && pidAlive(lease.pid);
  };

  const ensureFinalized = async (runId: string): Promise<SidecarTerminalRecord | undefined> => {
    const existing = store.readTerminal(runId);
    if (existing) return existing;
    const events = store.readJournalEvents(runId);
    if (journalTerminalState(events)) {
      // Journal already terminal → rebuild is idempotent even if the drive's
      // own finalize is racing (both produce the same logical record).
      return finalizeRun(store, runId, finalizeDeps);
    }
    if (isLive(runId)) return undefined; // still running
    if (store.readCancel(runId)) {
      // Dead driver + cancel marker → fold to cancelled now (§7).
      return finalizeRun(store, runId, finalizeDeps);
    }
    return undefined; // crashed mid-run; attach re-drives it
  };

  /** Dead-lease recovery: a dispatched-but-unsettled attempt replays as
   *  'running' forever, so reserve a fresh attempt via the journal's own retry
   *  vocabulary — the re-drive then retries instead of hanging (§4 inv. 4). */
  const repairDanglingAttempts = (runId: string): void => {
    const journalPath = store.journalPath(runId);
    const events = readJournal(journalPath);
    if (events.length === 0) return;
    const snap = materialize(events);
    for (const [nodeId, s] of snap.nodes) {
      if (s.status !== 'running') continue;
      const key = s.effectiveInstanceId ?? nodeId;
      appendEvent(journalPath, {
        type: 'nodeRetryRequested',
        nodeId,
        ...(s.effectiveInstanceId ? { instanceId: s.effectiveInstanceId } : {}),
        previousAttemptId: latestAttemptIdFor(events, key) ?? `${key}/attempts/001`,
        nextAttemptId: nextAttemptIdFor(events, key),
        reason: 'blockedRetry',
        previousErrorClass: 'workerError',
        previousErrorCode: 'DRIVER_CRASH',
      });
    }
  };

  return {
    create(request) {
      const { snapshot, realCwd } = admit(request);
      // Ledger-before-spawn (§4 invariant 1): request + cost anchor + lease
      // are all durable before the engine may fork anything.
      store.writeRequest(request);
      const frozen: FrozenSession = {
        cliId: snapshot.cliId,
        sessionId: frozenSessionIdFor(request.runId),
        cwd: realCwd,
      };
      store.writeSession(request.runId, frozen);
      store.writeLease(request.runId, { pid: process.pid, startedAt: Date.now() });
      drive(request, snapshot);
      return accepted(request.runId, 'running', true);
    },

    async attach(runId) {
      const terminal = await ensureFinalized(runId);
      if (terminal) return accepted(runId, terminal.state, false);
      if (isLive(runId)) return accepted(runId, 'running', false);
      // Dead lease + non-terminal journal → re-drive (retry, not re-execution:
      // the journal replay skips whatever already settled).
      const request = store.readRequest(runId);
      if (!request) throw new SidecarGateError('INTERNAL', `run ${runId} has no request.json`);
      const { snapshot } = admit(request); // profile is re-resolved live
      repairDanglingAttempts(runId);
      store.writeLease(runId, { pid: process.pid, startedAt: Date.now() });
      drive(request, snapshot);
      return accepted(runId, 'running', false);
    },

    async cancel(runId) {
      const terminal = await ensureFinalized(runId);
      if (terminal) {
        return { runId, state: terminal.state, cancelRequested: false, alreadyTerminal: true };
      }
      store.writeCancel(runId); // marker BEFORE abort (idempotent)
      const entry = live.get(runId);
      if (entry) {
        entry.controller.abort();
      } else if (!isLive(runId)) {
        // Nobody is driving — fold to the cancelled terminal immediately and
        // report the post-fold snapshot (not a stale 'running').
        const folded = await finalizeRun(store, runId, finalizeDeps);
        if (folded) return { runId, state: folded.state, cancelRequested: true, alreadyTerminal: false };
      }
      return { runId, state: 'running', cancelRequested: true, alreadyTerminal: false };
    },

    ensureFinalized,
    isLive,
    settled: (runId) => live.get(runId)?.done,
  };
}

// ─── Real usage collection (spec §8 — claude family via native transcript) ──

/**
 * Fold the CLI-native transcripts for ALL attempt sessions of the run (same
 * cliId/cwd, per-attempt sessionId) and aggregate — a retry's usage is part of
 * the run's cost.  Honest by construction: a non-claude CLI, or ANY attempt
 * whose transcript is missing/oversized, drops completeness (partial usage is
 * still reported, never fabricated zeros); and collected usage always reports
 * costComplete false in v1 because no usd pricing exists (the record-level
 * gate usageSatisfiesCostComplete would reject it anyway).
 */
export function createClaudeUsageCollector(opts: { claudeDataDir?: string } = {}): CollectUsage {
  const dataDir = opts.claudeDataDir ?? join(homedir(), '.claude');
  return (sessions: FrozenSession[]) => {
    let anyResolved = false;
    let allResolved = true;
    const sum = { inputTokens: 0, outputTokens: 0, cacheReadTokens: 0, cacheCreationTokens: 0, turns: 0 };
    let model = '';
    for (const s of sessions) {
      if (s.cliId !== 'claude-code') {
        allResolved = false;
        continue;
      }
      const transcriptPath = getClaudeSessionJsonlPath(s.sessionId, s.cwd, dataDir);
      const usage = transcriptPath ? readSessionTokenUsageFile(transcriptPath, 'claude', { fresh: true }) : null;
      if (!usage) {
        allResolved = false;
        continue;
      }
      anyResolved = true;
      sum.inputTokens += usage.inputTokens;
      sum.outputTokens += usage.outputTokens;
      sum.cacheReadTokens += usage.cacheReadTokens;
      sum.cacheCreationTokens += usage.cacheCreateTokens;
      sum.turns += usage.turns;
      if (!model) model = usage.model;
    }
    if (!anyResolved) return { costComplete: false };
    void allResolved; // v1: even a fully resolved run stays incomplete (no usd pricing)
    return { usage: { model, ...sum }, costComplete: false };
  };
}
