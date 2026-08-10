import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve as resolvePath } from 'node:path';
import { afterEach, describe, expect, it } from 'vitest';

import {
  BOTS_CONFIG_ENV,
  resolveBotmuxConfigDir,
  resolveBotsConfigFile,
  resolveChildBotsConfig,
} from '../src/core/config-dir.js';
import {
  BOTMUX_INJECTED_ENV_KEYS,
  isBotmuxManagedTmuxEnvKey,
  isBotmuxManagedTmuxServerGlobalEnvKey,
} from '../src/utils/child-env.js';
import { isReservedPerBotEnvKey, sanitizePerBotEnv } from '../src/core/per-bot-env.js';
import { shellWrapperScript } from '../src/adapters/backend/tmux-backend.js';
import { tmuxEnv } from '../src/setup/ensure-tmux.js';

const roots: string[] = [];

afterEach(() => {
  while (roots.length) rmSync(roots.pop()!, { recursive: true, force: true });
});

function root(): string {
  const value = mkdtempSync(join(tmpdir(), 'botmux-config-dir-'));
  roots.push(value);
  return value;
}

describe('resolveBotmuxConfigDir', () => {
  it('derives the config dir from HOME', () => {
    const home = root();
    expect(resolveBotmuxConfigDir({ env: { HOME: home } })).toBe(join(home, '.botmux'));
  });

  it('falls back to USERPROFILE when HOME is absent (Windows)', () => {
    const home = root();
    expect(resolveBotmuxConfigDir({ env: { USERPROFILE: home } })).toBe(join(home, '.botmux'));
  });

  it('prefers the explicit homeDir test seam over env HOME', () => {
    const seam = root();
    expect(resolveBotmuxConfigDir({ env: { HOME: '/nonexistent' }, homeDir: seam }))
      .toBe(join(seam, '.botmux'));
  });
});

describe('resolveBotsConfigFile', () => {
  it('honours BOTS_CONFIG as the exact file, above the HOME-derived default', () => {
    const home = root();
    const explicit = join(root(), 'fleet-a.json');
    expect(resolveBotsConfigFile({ env: { HOME: home, [BOTS_CONFIG_ENV]: explicit } }))
      .toBe(explicit);
    expect(resolveBotsConfigFile({ env: { HOME: home } }))
      .toBe(join(home, '.botmux', 'bots.json'));
  });

  it('mirrors the loader: BOTS_CONFIG may name ANY filename, not just bots.json', () => {
    // The whole reason the child is pinned to a FILE and not a config DIR: a
    // dir + hardcoded 'bots.json' cannot express this deployment at all.
    const custom = join(root(), 'nested', 'fleet-a.json');
    expect(resolveBotsConfigFile({ env: { HOME: root(), [BOTS_CONFIG_ENV]: custom } }))
      .toBe(custom);
  });
});

describe('resolveChildBotsConfig — what the daemon pins onto its CLI child', () => {
  it('pins the daemon-frozen loaded path verbatim, including a custom filename', () => {
    const loaded = join(root(), 'fleet-a.json');
    writeFileSync(loaded, '[]');
    expect(resolveChildBotsConfig(loaded, { exists: existsSync })).toBe(loaded);
  });

  it('returns null with no loaded path, so the caller DELETES an inherited value', () => {
    // Leaving a stale ambient BOTS_CONFIG in place would redirect the child to a
    // foreign registry — BOTS_CONFIG is the TOP of the precedence chain.
    expect(resolveChildBotsConfig(undefined)).toBeNull();
    expect(resolveChildBotsConfig('')).toBeNull();
    expect(resolveChildBotsConfig('   ')).toBeNull();
  });

  it('declines to pin a path that does not exist (core-only synthesis)', () => {
    // Core-only pins loadedConfigPath to the DEFAULT ~/.botmux/bots.json without
    // ever reading it. BOTS_CONFIG naming a missing file is a hard throw in the
    // loader, whereas the unpinned default degrades gracefully — so pinning a
    // ghost would be strictly worse than not pinning.
    const missing = join(root(), 'never-written.json');
    expect(resolveChildBotsConfig(missing, { exists: existsSync })).toBeNull();
    // Without the exists seam the value is trusted as-is (pure path logic).
    expect(resolveChildBotsConfig(missing)).toBe(missing);
  });

  it('absolute-izes a relative loaded path (daemon/worker/pane share no cwd)', () => {
    expect(resolveChildBotsConfig('rel/bots.json')).toBe(resolvePath('rel/bots.json'));
  });
});

describe('regression: a daemon under a non-default HOME and its child agree', () => {
  it('the child resolves the daemon registry despite its own HOME differing', () => {
    const fleetHome = root();
    const defaultHome = root();
    const fleetConfig = join(fleetHome, '.botmux', 'bots.json');

    // Before: the child re-derived the registry from its own (default) HOME.
    expect(resolveBotsConfigFile({ env: { HOME: defaultHome } })).not.toBe(fleetConfig);

    // After: the daemon pins the exact file it loaded, so the child agrees even
    // though its HOME still points at the default home.
    const pinned = resolveChildBotsConfig(fleetConfig);
    expect(pinned).toBe(fleetConfig);
    expect(resolveBotsConfigFile({ env: { HOME: defaultHome, [BOTS_CONFIG_ENV]: pinned! } }))
      .toBe(fleetConfig);
  });

  it('the pin OUTRANKS a stale ambient BOTS_CONFIG (the original blocker)', () => {
    // A shared tmux server carries a co-tenant's BOTS_CONFIG in its global env.
    // Pinning a config DIR would rank BELOW it and lose; pinning BOTS_CONFIG
    // itself replaces it.
    const stale = join(root(), 'stale-other.json');
    const correct = join(root(), 'correct.json');
    const childEnv: NodeJS.ProcessEnv = { HOME: root(), [BOTS_CONFIG_ENV]: stale };
    childEnv[BOTS_CONFIG_ENV] = resolveChildBotsConfig(correct)!;
    expect(resolveBotsConfigFile({ env: childEnv })).toBe(correct);
  });
});

describe('BOTS_CONFIG plumbing — all four leak vectors', () => {
  it('vector 1: injected into panes, so tmux matches the direct-spawn path', () => {
    // buildBotmuxEnvAssignments iterates this list; omitting the key would fix
    // only the pty backend and leave tmux sessions failing.
    expect(BOTMUX_INJECTED_ENV_KEYS).toContain(BOTS_CONFIG_ENV);
  });

  it('vector 2: stripped from the tmux CLIENT env, so no server global is seeded', () => {
    expect(isBotmuxManagedTmuxEnvKey(BOTS_CONFIG_ENV)).toBe(true);
    expect(isBotmuxManagedTmuxServerGlobalEnvKey(BOTS_CONFIG_ENV)).toBe(true);
    expect(tmuxEnv({ BOTS_CONFIG: '/tmp/stale.json' }).BOTS_CONFIG).toBeUndefined();
  });

  it('vector 3: unset in the pane wrapper, so a stale server global cannot win', () => {
    // The pane inherits the tmux SERVER's global env, which the client env
    // cannot override — so stripping the client env is not sufficient.
    expect(shellWrapperScript('/tmp/bin')).toMatch(/\bunset\b[^&]*\bBOTS_CONFIG\b/);
  });

  it('vector 4: reserved from per-bot env — a bot cannot redirect its own registry', () => {
    expect(isReservedPerBotEnvKey(BOTS_CONFIG_ENV)).toBe(true);
    expect(sanitizePerBotEnv({ [BOTS_CONFIG_ENV]: '/tmp/evil.json', KEEP: 'yes' }))
      .toEqual({ KEEP: 'yes' });
  });
});

describe('worker spawnCli wiring (source lock)', () => {
  // P3 was a param-SHAPE bug that no resolver-level test could ever catch:
  // `resolveBotmuxConfigDir(process.env)` type-checks (a ProcessEnv is
  // structurally a valid options bag), so only the real call site proves it.
  const workerSource = readFileSync(resolvePath('src/worker.ts'), 'utf8');

  it('pins the DAEMON-FROZEN loaded path, not an env re-derivation', () => {
    expect(workerSource).toContain(
      'resolveChildBotsConfig(cfg.loadedBotsConfigPath, { exists: existsSync })',
    );
    // The old shape must be gone: a bare env map read as an options bag meant
    // any lowercase `env=` / `homeDir=` in the environment silently hijacked it.
    expect(workerSource).not.toContain('resolveBotmuxConfigDir(process.env)');
    // And the child must never be handed a re-derivation of its OWN environment.
    expect(workerSource).not.toContain('childEnv.BOTS_CONFIG = resolveBotsConfigFile');
  });

  it('assigns OR deletes, never leaving an inherited BOTS_CONFIG to chance', () => {
    const start = workerSource.indexOf('const pinned = resolveChildBotsConfig(');
    expect(start).toBeGreaterThan(-1);
    const block = workerSource.slice(start, start + 400);
    expect(block).toContain('if (pinned) childEnv.BOTS_CONFIG = pinned;');
    expect(block).toContain('else delete childEnv.BOTS_CONFIG;');
  });
});
