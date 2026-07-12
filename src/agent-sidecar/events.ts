/**
 * Wire event derivation (spec §5): journal.ndjson → seq-stamped frames.
 *
 * seq = 1-based index of the successfully parsed journal line (append-only +
 * torn-final-line tolerance in readJournal make this stable across reads, so
 * `?since=N` replay is prefix-consistent and gapless by construction).  Every
 * parsed line maps to exactly one frame; the sidecar never mutates the journal.
 *
 * Terminal gating (terminal-before-ack): the terminal frame is emitted ONLY
 * when terminal.json already exists — a journal-terminal line without a
 * persisted record ends the replay just before it.  A cancel that aborted the
 * engine before any journal-terminal line gets a SYNTHESIZED terminal frame at
 * seq = parsedLines + 1 (matching terminalSeqFor / terminal.json.lastSeq).
 *
 * Session frames strip webPort (the journal already never carries the write
 * token); log frames are structured phase summaries, never PTY bytes.
 */

import type { StoredEvent } from '../workflows/v3/journal.js';
import type { SidecarEventFrame, SidecarTerminalRecord } from './contract.js';

/** The seq the terminal frame occupies for this journal: the journal-terminal
 *  line's own seq, or parsedLines+1 when the terminal is synthesized (cancel
 *  without a journal-terminal line).  finalizeRun persists this as lastSeq. */
export function terminalSeqFor(events: StoredEvent[]): number {
  for (let i = events.length - 1; i >= 0; i--) {
    const t = events[i]!.type;
    if (t === 'runSucceeded' || t === 'runFailed' || t === 'runBlocked') return i + 1;
  }
  return events.length + 1;
}

function logText(e: StoredEvent): string {
  switch (e.type) {
    case 'nodeDispatched':
      return `node dispatched (${e.attemptId})`;
    case 'nodeRetryRequested':
      return `node retry requested (${e.previousAttemptId} -> ${e.nextAttemptId})`;
    case 'nodeSucceeded':
      return `node succeeded (${e.attemptId})`;
    case 'nodeFailed':
      return `node failed [${e.errorCode ?? e.errorClass}]${e.message ? ` ${e.message}` : ''}`;
    case 'nodeBlocked':
      return `node blocked [${e.errorCode ?? e.errorClass}]${e.message ? ` ${e.message}` : ''}`;
    default:
      return `event ${e.type}`;
  }
}

/**
 * Derive the full wire frame list for a journal snapshot.  Pass the persisted
 * terminal record (or undefined) — it gates and shapes the terminal frame
 * (cancel folding: frame state comes from the RECORD, not the journal event).
 */
export function deriveWireFrames(
  events: StoredEvent[],
  terminal: SidecarTerminalRecord | undefined,
): SidecarEventFrame[] {
  const frames: SidecarEventFrame[] = [];
  for (let i = 0; i < events.length; i++) {
    const e = events[i]!;
    const seq = i + 1;
    if (e.type === 'runSucceeded' || e.type === 'runFailed' || e.type === 'runBlocked') {
      if (!terminal) return frames; // terminal-before-ack: not persisted yet
      frames.push({ seq, ts: e.ts, event: { type: 'terminal', state: terminal.state } });
      return frames; // nothing legitimately follows a run-terminal line
    }
    if (e.type === 'runStarted') {
      frames.push({ seq, ts: e.ts, event: { type: 'run.accepted' } });
    } else if (e.type === 'nodeSessionReady') {
      frames.push({ seq, ts: e.ts, event: { type: 'session', sessionId: e.sessionInfo.sessionId } });
    } else {
      frames.push({ seq, ts: e.ts, event: { type: 'log', text: logText(e) } });
    }
  }
  if (terminal) {
    // Cancelled before any journal-terminal line — synthesize the terminal
    // frame right after the last journal frame (seq matches terminal.lastSeq).
    frames.push({
      seq: events.length + 1,
      ts: terminal.finishedAt,
      event: { type: 'terminal', state: terminal.state },
    });
  }
  return frames;
}
