/**
 * Resolve Botmux's configuration directory (`~/.botmux`) and the bots.json
 * registry inside it, from ONE canonical precedence rule that matches the
 * registry loader: `BOTS_CONFIG` (exact file) > `$HOME/.botmux/bots.json`.
 *
 * ── The bug this closes ──────────────────────────────────────────────────────
 * `HOME=~/alt botmux start` makes the daemon load `~/alt/.botmux/bots.json`, but
 * the daemon injects only `cwd` and the `BOTMUX_*` family into CLI children —
 * never `HOME`. The child therefore resolves `homedir()/.botmux/bots.json` (the
 * *default* home), does not find the bot it is running as, and every
 * `botmux send` / `botmux history` from inside that session fails with
 * `Bot not registered: <appId>`.
 *
 * ── Why the child is pinned to a FILE, not a DIRECTORY ───────────────────────
 * The registry's real precedence is `BOTS_CONFIG` > `<config dir>/bots.json`,
 * and `BOTS_CONFIG` may name an ARBITRARY file (`/srv/fleet-a.json`). A
 * directory-shaped hint therefore cannot express what the daemon actually
 * loaded: `dirname` + a hardcoded `bots.json` guesses wrong for every custom
 * filename, and it sits BELOW `BOTS_CONFIG` in precedence, so a stale ambient
 * `BOTS_CONFIG` in a shared tmux server's global env would silently outrank it
 * and hand the child a foreign registry (verified: the child loaded the stale
 * file while the daemon had the correct one).
 *
 * So the daemon pins the EXACT path it loaded — `getLoadedConfigPath()`, already
 * frozen into `loadedBotsConfigPath` for the sandbox fs-policy — into the
 * child's `BOTS_CONFIG`. That is one authority, at the TOP of the precedence
 * chain, and file-shaped so any filename survives. `BOTS_CONFIG` is also
 * reserved from per-bot `env` and scrubbed off the pane/tmux paths, because a
 * bot must not be able to redirect the registry that defines it.
 *
 * Injecting `HOME` into children was considered and rejected: `HOME` also
 * anchors the CLI's own config discovery (Claude Code falls back to
 * `$HOME/.claude` when `CLAUDE_CONFIG_DIR` is unset), so overriding it to point
 * at the fleet home silently relocates skills/settings for the spawned agent.
 *
 * ── Scope (deliberately narrow) ──────────────────────────────────────────────
 * This is an INTERNAL daemon→child propagation fix, not a new public
 * "relocate botmux" knob. `os.homedir()` already follows `$HOME`, so
 * `HOME=~/alt botmux start` ALREADY relocates cli.ts's `CONFIG_DIR` / `DATA_DIR`
 * / `PM2_HOME` / `BOTS_JSON_FILE`, the dashboard's write path and setup —
 * verified: `HOME=<fleet> botmux setup list` reads the fleet registry with no
 * code change. The daemon-spawned CLI child was the ONLY process that diverged,
 * precisely because it is the only one that does not inherit `HOME`. Introducing
 * a second, dir-shaped public variable would have created a rival source of
 * truth that governs the registry but not the data dir, pm2 home or dashboard
 * writes — half-relocated deployments — so no public variable is added.
 */

import { homedir } from 'node:os';
import { isAbsolute, join, resolve } from 'node:path';

export interface ResolveBotmuxConfigDirOptions {
  env?: NodeJS.ProcessEnv;
  /** Test seam; defaults to HOME/USERPROFILE from env, then os.homedir(). */
  homeDir?: string;
}

/**
 * The env var naming the EXACT bots.json to load. Top of the registry's
 * precedence chain, and the channel the daemon uses to pin its own loaded path
 * onto a spawned CLI child.
 *
 * Reserved from per-bot `env` (see core/per-bot-env.ts) and stripped from the
 * tmux client env + pane wrapper (see utils/child-env.ts): a bot must not be
 * able to redirect the registry that defines it, and a stale value in a shared
 * tmux server's global env must not reach a pane.
 */
export const BOTS_CONFIG_ENV = 'BOTS_CONFIG';

/**
 * Priority: `$HOME/.botmux` (`HOME` → `USERPROFILE` → `os.homedir()`).
 *
 * Note this resolves the DIRECTORY only, and is therefore NOT the whole story
 * for the registry: `BOTS_CONFIG` may point at an arbitrary file outside this
 * dir. Use {@link resolveBotsConfigFile} whenever you need the registry path.
 */
export function resolveBotmuxConfigDir(
  options: ResolveBotmuxConfigDirOptions = {},
): string {
  const env = options.env ?? process.env;
  const home = options.homeDir ?? env.HOME ?? env.USERPROFILE ?? homedir();
  return join(home, '.botmux');
}

/**
 * The bots.json path implied by the environment: `BOTS_CONFIG` (absolute-ized
 * against cwd, exactly as the loader does) else `<config dir>/bots.json`.
 *
 * Existence is NOT checked here — callers differ on what an absent file means
 * (the loader throws for an explicit `BOTS_CONFIG`, degrades for the default).
 */
export function resolveBotsConfigFile(
  options: ResolveBotmuxConfigDirOptions = {},
): string {
  const env = options.env ?? process.env;
  const explicit = env[BOTS_CONFIG_ENV]?.trim();
  if (explicit) return resolve(explicit);
  return join(resolveBotmuxConfigDir(options), 'bots.json');
}

/**
 * Decide the `BOTS_CONFIG` value to pin onto a spawned CLI child.
 *
 * `loadedConfigPath` is the daemon's frozen `getLoadedConfigPath()` — the file
 * it actually parsed. When present it wins unconditionally (absolute-ized for
 * the same cwd-independence reason a relative path is never trusted).
 *
 * Returns null when the daemon has no usable loaded path. Callers must then
 * DELETE `BOTS_CONFIG` from the child env rather than leave an inherited value
 * in place: an ambient stale value outranks the on-disk default and would hand
 * the child a foreign registry.
 *
 * `exists` (injected so this module stays fs-free and unit-testable) guards the
 * one case where pinning would be strictly worse than not pinning: core-only
 * synthesis pins `loadedConfigPath` to the DEFAULT `~/.botmux/bots.json` even
 * though it never read that file, and it may not exist. `BOTS_CONFIG` naming a
 * missing file is a hard `throw` in the loader, whereas the unpinned default
 * path degrades gracefully — so an absent target falls back to null. Note a
 * read-ISOLATED child still pins correctly: Seatbelt denies the content read but
 * allows the metadata read, so the file "exists" here and the loader's
 * EPERM+underReadIsolation branch handles it.
 */
export function resolveChildBotsConfig(
  loadedConfigPath: string | undefined,
  opts: { exists?: (path: string) => boolean } = {},
): string | null {
  const trimmed = loadedConfigPath?.trim();
  if (!trimmed) return null;
  const absolute = isAbsolute(trimmed) ? trimmed : resolve(trimmed);
  if (opts.exists && !opts.exists(absolute)) return null;
  return absolute;
}
