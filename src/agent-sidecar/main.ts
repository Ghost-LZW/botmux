/**
 * agent-sidecar composition root (independent of the interactive daemon).
 *
 * Wires the REAL deps: createEphemeralPool (worker spawns), the
 * readAndValidateManifest adapter, bots.json profile resolution (BotSnapshot
 * with workingDir overridden to the request cwd by the driver), and the claude
 * transcript usage collector.  Config via env/flags only:
 *
 *   BOTMUX_AGENT_SIDECAR_SOCKET   (--socket)           default ~/.botmux/agent-sidecar.sock
 *   BOTMUX_AGENT_RUNS_DIR         (--runs-dir)         default ~/.botmux/agent-runs
 *   BOTMUX_AGENT_WORKSPACE_ROOTS  (--workspace-roots)  colon-separated; default EMPTY = deny all
 *
 * Runnable directly (`node dist/agent-sidecar/main.js`) or via
 * `botmux agent-sidecar` (lazy-imported cli.ts case, v3 precedent).
 */

import { homedir } from 'node:os';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { realpathSync } from 'node:fs';

import { createEphemeralPool } from '../workflows/v3/ephemeral-pool.js';
import { ManifestValidationError, readAndValidateManifest } from '../workflows/v3/manifest.js';
import { resolveBotConfig, botToSnapshot } from '../workflows/v3/bot-resolve.js';
import type { BotSnapshot, ValidateManifest } from '../workflows/v3/contract.js';
import { loadBotConfigs } from '../bot-registry.js';
import { createRunStore, defaultRunsRoot } from './run-store.js';
import { createClaudeUsageCollector, createRunDriver } from './driver.js';
import { createSidecarServer, listenOnSocket } from './server.js';

export interface AgentSidecarConfig {
  socketPath: string;
  runsRoot: string;
  workspaceRoots: string[];
}

function argValue(args: string[], flag: string): string | undefined {
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === flag && i + 1 < args.length) return args[i + 1];
    if (a?.startsWith(flag + '=')) return a.slice(flag.length + 1);
  }
  return undefined;
}

export function parseSidecarConfig(argv: string[], env: NodeJS.ProcessEnv = process.env): AgentSidecarConfig {
  const socketPath = argValue(argv, '--socket')
    ?? env.BOTMUX_AGENT_SIDECAR_SOCKET
    ?? join(homedir(), '.botmux', 'agent-sidecar.sock');
  const runsRoot = argValue(argv, '--runs-dir') ?? defaultRunsRoot(env);
  const rootsRaw = argValue(argv, '--workspace-roots') ?? env.BOTMUX_AGENT_WORKSPACE_ROOTS ?? '';
  const workspaceRoots = rootsRaw.split(':').map((s) => s.trim()).filter((s) => s !== '');
  return { socketPath, runsRoot, workspaceRoots };
}

export async function mainAgentSidecar(argv: string[] = []): Promise<void> {
  const config = parseSidecarConfig(argv);

  const bots = loadBotConfigs();
  // Secrets never enter the ledger/snapshot: the pool re-resolves live by
  // larkAppId at spawn (v3 contract; no env fallback).
  const secretById = new Map(bots.map((b) => [b.larkAppId, b.larkAppSecret]));
  const resolveLarkAppSecret = (larkAppId: string): string | undefined => secretById.get(larkAppId);
  const { runNode } = createEphemeralPool({ resolveLarkAppSecret });

  const validateManifest: ValidateManifest = async (manifestPath, outputDir) => {
    try {
      return { ok: true, manifest: await readAndValidateManifest(manifestPath, outputDir) };
    } catch (e) {
      return { ok: false, problems: e instanceof ManifestValidationError ? e.problems : [String(e)] };
    }
  };

  const resolveProfile = (profileRef: string): BotSnapshot | undefined => {
    try {
      return botToSnapshot(resolveBotConfig(profileRef, bots));
    } catch {
      return undefined;
    }
  };

  const store = createRunStore(config.runsRoot);
  const driver = createRunDriver({
    store,
    resolveProfile,
    allowedWorkspaceRoots: config.workspaceRoots,
    runNode,
    validateManifest,
    collectUsage: createClaudeUsageCollector(),
    // v1 fail-closed, NO override: the botmux sandbox still bind-mounts the
    // daemon-mediated relay outbox (`botmux send` egress via host watcher) and
    // real auth paths rw, so sandboxNetwork=false does NOT make a run
    // side-effect-free.  Lifting this requires a sidecar sandbox policy
    // (no relay outbox/shim, auth read-only) plus Linux runtime negative
    // tests — a code change by design, not a config flag.
    realRunsDisabledReason:
      'v1 sidecar is contract-proof only: real worker runs are disabled until the ' +
      'sandbox discovery policy closes relay-outbox/auth-write egress (see README section 9)',
    log: (msg) => console.error(msg),
  });

  const server = createSidecarServer({ store, driver });
  await listenOnSocket(server, config.socketPath);
  console.log(`agent-sidecar listening on ${config.socketPath}`);
  console.log(`  runs root:       ${config.runsRoot}`);
  console.log(`  workspace roots: ${config.workspaceRoots.join(':') || '(none — all cwd rejected)'}`);

  const shutdown = (): void => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(0), 2000).unref();
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  await new Promise<void>((resolve) => server.on('close', resolve));
}

// Direct-entry boot (`node dist/agent-sidecar/main.js`); the cli.ts case calls
// mainAgentSidecar explicitly, so this never double-boots.
const isDirectRun = ((): boolean => {
  try {
    return import.meta.url === pathToFileURL(realpathSync(process.argv[1] ?? '')).href;
  } catch {
    return false;
  }
})();
if (isDirectRun) {
  mainAgentSidecar(process.argv.slice(2)).catch((err) => {
    console.error(`agent-sidecar failed: ${err instanceof Error ? err.message : String(err)}`);
    process.exit(1);
  });
}
