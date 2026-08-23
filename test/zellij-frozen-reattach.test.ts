/**
 * Regression for PR #836 (F1 follow-up): a caller that has already frozen the
 * reattach-vs-fresh decision (the worker's tri-state probe, reset to 'missing'
 * after any teardown gate) must have that decision honoured VERBATIM by
 * ZellijBackend.spawn(). The pre-existing `this.reattaching ||= hasSession()`
 * self-heal would otherwise re-run the same load-fragile `list-sessions` and,
 * on a post-kill session that has not fully died yet, flip a frozen `false`
 * back to attach — reattaching to the very pane the gate just removed.
 *
 * Run:  pnpm vitest run test/zellij-frozen-reattach.test.ts
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';

// Mock child_process so ZellijBackend.hasSession()/probeSession() are
// controllable: a 'true'-looking list-sessions output simulates the not-yet-
// reaped session a live re-probe would still see.
vi.mock('node:child_process', () => ({
  execFileSync: vi.fn(),
  spawnSync: vi.fn(),
}));

vi.mock('node-pty', () => ({
  spawn: vi.fn(() => ({
    onData: vi.fn(),
    onExit: vi.fn(),
    write: vi.fn(),
    resize: vi.fn(),
    kill: vi.fn(),
    pid: 4242,
  })),
}));

import { execFileSync } from 'node:child_process';
import * as pty from 'node-pty';
import { ZellijBackend } from '../src/adapters/backend/zellij-backend.js';

const mockedExecFileSync = vi.mocked(execFileSync);
const mockedPtySpawn = vi.mocked(pty.spawn);

const SESSION = 'bmx-frozen01';
const spawnOpts = { cwd: '/tmp', cols: 80, rows: 24, env: { PATH: '/usr/bin' } };

// Make list-sessions report the session as LIVE — i.e. hasSession() === true.
function listSessionsReportsLive() {
  mockedExecFileSync.mockReturnValue(`${SESSION} [Created 1s ago] \n` as unknown as Buffer);
}

describe('ZellijBackend frozen reattach decision', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    listSessionsReportsLive();
  });

  it('honours a frozen isReattach:false even when a live re-probe would say the session exists', () => {
    // This is the teardown case: the gate killed the pane and froze the
    // decision to fresh, but the session lingers in list-sessions.
    const be = new ZellijBackend(SESSION, { ownsSession: true, isReattach: false, reattachDecision: 'frozen' });
    be.spawn('claude', [], spawnOpts);

    expect(be.isReattach).toBe(false);
    // The self-heal probe must NOT have run.
    expect(mockedExecFileSync).not.toHaveBeenCalled();
    // And the spawn must be a FRESH session (--new-session-with-layout), not an
    // attach to the pane the teardown removed.
    const args = mockedPtySpawn.mock.calls[0][1] as string[];
    expect(args).toContain('--new-session-with-layout');
    expect(args).not.toContain('attach');
  });

  it('honours a frozen isReattach:true without re-probing', () => {
    const be = new ZellijBackend(SESSION, { ownsSession: true, isReattach: true, reattachDecision: 'frozen' });
    be.spawn('claude', [], spawnOpts);

    expect(be.isReattach).toBe(true);
    expect(mockedExecFileSync).not.toHaveBeenCalled();
    const args = mockedPtySpawn.mock.calls[0][1] as string[];
    expect(args).toContain('attach');
    expect(args).not.toContain('--new-session-with-layout');
  });

  it('still self-heals in auto mode (default) — a live session flips isReattach true', () => {
    // Default callers (no frozen decision) keep the PR#249 self-heal: a daemon
    // restart that left the CLI running must reattach.
    const be = new ZellijBackend(SESSION, { ownsSession: true, isReattach: false });
    be.spawn('claude', [], spawnOpts);

    expect(be.isReattach).toBe(true);
    // The self-heal probe DID run (list-sessions).
    expect(mockedExecFileSync).toHaveBeenCalled();
    const args = mockedPtySpawn.mock.calls[0][1] as string[];
    expect(args).toContain('attach');
  });

  it('auto mode with a genuinely missing session stays fresh', () => {
    mockedExecFileSync.mockReturnValue('' as unknown as Buffer); // no live sessions
    const be = new ZellijBackend(SESSION, { ownsSession: true, isReattach: false });
    be.spawn('claude', [], spawnOpts);

    expect(be.isReattach).toBe(false);
    const args = mockedPtySpawn.mock.calls[0][1] as string[];
    expect(args).toContain('--new-session-with-layout');
  });
});
