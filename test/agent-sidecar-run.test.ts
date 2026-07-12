/**
 * agent-sidecar-run.test.ts — ledger 崩溃窗口与 run 语义：
 * A4 kill-window（journal 终态而 terminal.json 缺失 → 幂等 finalize，无双终态）、
 * dead-lease 非终态 re-drive（§4 不变量4）、A8 manifest 逃逸/绝对路径/sha 不符
 * （真实 validator）、A11 usage 采集失败诚实置 costComplete=false、
 * A13 ASK_HUMAN → blocked + ask 证据、A15 secrets 不落 ledger/不上 wire。
 */

import { describe, it, expect } from 'vitest';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { appendEvent } from '../src/workflows/v3/journal.js';
import type { RunNode } from '../src/workflows/v3/contract.js';
import { createClaudeUsageCollector, frozenSessionIdFor } from '../src/agent-sidecar/driver.js';
import {
  askRunNode,
  awaitTerminal,
  badManifestRunNode,
  buildRunBody,
  makeStack,
  okRunNode,
  parseFrames,
  wire,
} from './agent-sidecar-harness.js';

/** 一个必然已死的 pid（spawn 一个立即退出的子进程并回收）。 */
function deadPid(): number {
  const child = spawnSync(process.execPath, ['-e', '0']);
  return child.pid ?? 999_999;
}

function walkFiles(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    const p = join(dir, name);
    const st = statSync(p);
    if (st.isDirectory()) out.push(...walkFiles(p));
    else if (st.isFile()) out.push(p);
  }
  return out;
}

describe('A4 — kill window: journal 终态、terminal.json 缺失', () => {
  it('成功 run 重建 terminal.json：同一逻辑终态，单一 terminal 帧', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    const body = buildRunBody({ runId: 'run-a4-window1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record: original } = await awaitTerminal(stack.socketPath, body.runId);

      // 模拟「journal 终态后、terminal.json 写盘前」崩溃：删掉 terminal.json，
      // 在同一 runsRoot 上起全新 sidecar。
      rmSync(join(stack.runsRoot, body.runId, 'terminal.json'));
      const stack2 = await makeStack({
        runNode: okRunNode(),
        reuseRunsRoot: stack.runsRoot,
        extraWorkspaceRoots: [stack.wsRoot],
      });
      try {
        const { record: rebuilt } = await awaitTerminal(stack2.socketPath, body.runId);
        // 同一逻辑终态（ts 类字段除外）
        expect(rebuilt.state).toBe(original.state);
        expect(rebuilt.error).toEqual(original.error);
        expect(rebuilt.summary).toBe(original.summary);
        expect(rebuilt.artifacts).toEqual(original.artifacts);
        expect(rebuilt.lastSeq).toBe(original.lastSeq);
        expect(rebuilt.startedAt).toBe(original.startedAt); // journal ts，非重建时刻

        // /result 幂等（重建后字节级稳定）
        const again = await wire(stack2.socketPath, 'GET', `/v1/runs/${body.runId}/result`);
        expect(JSON.parse(again.text)).toEqual(rebuilt);

        // 无双终态：events 里 terminal 帧唯一
        const frames = parseFrames((await wire(stack2.socketPath, 'GET', `/v1/runs/${body.runId}/events?follow=0`)).text);
        expect(frames.filter((f) => f.event.type === 'terminal')).toHaveLength(1);
        expect(frames.at(-1)!.seq).toBe(rebuilt.lastSeq);
        expect(stack2.runNodeCalls()).toBe(0); // 重建不重跑
      } finally {
        await stack2.close();
      }
    } finally {
      await stack.close();
    }
  });

  it('blocked(ASK_HUMAN) run 重建：error code 与 ask 证据不漂移', async () => {
    const stack = await makeStack({ runNode: askRunNode('Deploy to prod?', ['yes', 'no']) });
    const body = buildRunBody({ runId: 'run-a4-window2', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record: original } = await awaitTerminal(stack.socketPath, body.runId);
      expect(original.state).toBe('blocked');

      rmSync(join(stack.runsRoot, body.runId, 'terminal.json'));
      const stack2 = await makeStack({
        runNode: askRunNode('Deploy to prod?', ['yes', 'no']),
        reuseRunsRoot: stack.runsRoot,
        extraWorkspaceRoots: [stack.wsRoot],
      });
      try {
        const { record: rebuilt } = await awaitTerminal(stack2.socketPath, body.runId);
        expect(rebuilt.state).toBe('blocked');
        expect(rebuilt.error.code).toBe('ASK_HUMAN');
        expect(rebuilt.ask).toEqual(original.ask);
        expect(rebuilt.lastSeq).toBe(original.lastSeq);
      } finally {
        await stack2.close();
      }
    } finally {
      await stack.close();
    }
  });
});

describe('§4 不变量4 — dead lease + journal 非终态', () => {
  it('attach 触发 re-drive：dangling attempt 走 retry，新 attempt 收敛成功', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    const runId = 'run-redrive1';
    const body = buildRunBody({ runId, goal: 'g', cwd: stack.cwd });
    try {
      // 手工铸「驱动进程死在 run 中途」的账本：request/session/dead-lease +
      // journal 停在 nodeDispatched（无 settle、无终态）。
      stack.store.writeRequest(body);
      stack.store.writeSession(runId, {
        cliId: 'claude-code', sessionId: frozenSessionIdFor(runId), cwd: stack.cwd,
      });
      stack.store.writeLease(runId, { pid: deadPid(), startedAt: Date.now() });
      const journalPath = stack.store.journalPath(runId);
      appendEvent(journalPath, { type: 'runStarted', runId });
      appendEvent(journalPath, {
        type: 'nodeDispatched', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/001',
      });

      const res = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(res.status).toBe(200);
      expect(JSON.parse(res.text)).toMatchObject({ created: false, state: 'running' });

      const { record } = await awaitTerminal(stack.socketPath, runId);
      expect(record.state).toBe('succeeded');
      expect(stack.runNodeCalls()).toBe(1); // 重试恰好执行一次

      // journal 证据：retry 保留在同 instance，新 attempt 002
      const events = stack.store.readJournalEvents(runId);
      const retry = events.find((e) => e.type === 'nodeRetryRequested') as any;
      expect(retry).toBeDefined();
      expect(retry.previousAttemptId).toBe('goal#001/attempts/001');
      expect(retry.nextAttemptId).toBe('goal#001/attempts/002');
      const dispatches = events.filter((e) => e.type === 'nodeDispatched');
      expect((dispatches.at(-1) as any).attemptId).toBe('goal#001/attempts/002');
    } finally {
      await stack.close();
    }
  });

  it('dead lease + cancel：无人驱动时 cancel 立即折算 cancelled', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    const runId = 'run-redrive2';
    const body = buildRunBody({ runId, goal: 'g', cwd: stack.cwd });
    try {
      stack.store.writeRequest(body);
      stack.store.writeSession(runId, {
        cliId: 'claude-code', sessionId: frozenSessionIdFor(runId), cwd: stack.cwd,
      });
      stack.store.writeLease(runId, { pid: deadPid(), startedAt: Date.now() });
      appendEvent(stack.store.journalPath(runId), { type: 'runStarted', runId });

      const res = await wire(stack.socketPath, 'POST', `/v1/runs/${runId}/cancel`);
      expect(res.status).toBe(202);
      const { record } = await awaitTerminal(stack.socketPath, runId);
      expect(record.state).toBe('cancelled');
      expect(record.error.code).toBe('CANCELLED');
      // journal 无终态行 → terminal 帧是合成的，seq = 已解析行数 + 1
      const frames = parseFrames((await wire(stack.socketPath, 'GET', `/v1/runs/${runId}/events?follow=0`)).text);
      expect(frames.at(-1)!.event).toEqual({ type: 'terminal', state: 'cancelled' });
      expect(frames.at(-1)!.seq).toBe(record.lastSeq);
      expect(stack.runNodeCalls()).toBe(0);
    } finally {
      await stack.close();
    }
  });
});

describe('A8 — manifest 不可信输出（真实 validator）', () => {
  it('路径逃逸 / 绝对路径 / sha256 不符 → blocked 且 artifacts=[]', async () => {
    const byGoal: Record<string, RunNode> = {
      escape: badManifestRunNode('escape'),
      absolute: badManifestRunNode('absolute'),
      sha: badManifestRunNode('sha'),
    };
    const stack = await makeStack({ runNode: (req) => byGoal[req.node.goal]!(req) });
    try {
      for (const goal of ['escape', 'absolute', 'sha'] as const) {
        const body = buildRunBody({ runId: `run-a8-${goal}xx`, goal, cwd: stack.cwd });
        await wire(stack.socketPath, 'POST', '/v1/runs', body);
        const { record } = await awaitTerminal(stack.socketPath, body.runId);
        expect(record.state, goal).toBe('blocked'); // manifestInvalid = 可重试的合同失败
        expect(record.error.code, goal).toBe('MANIFEST_INVALID');
        expect(record.artifacts, goal).toEqual([]); // 未过校验绝不发布产物
      }
    } finally {
      await stack.close();
    }
  });
});

describe('A13 — ASK_HUMAN', () => {
  it('blocked + 结构化 ask 证据（无回答通道，纯证据回传）', async () => {
    const stack = await makeStack({ runNode: askRunNode('Which region?', ['us-east', 'eu-west']) });
    const body = buildRunBody({ runId: 'run-a13-askhh', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.state).toBe('blocked');
      expect(record.error).toMatchObject({ code: 'ASK_HUMAN', retryable: true });
      expect(record.ask).toEqual({ question: 'Which region?', options: ['us-east', 'eu-west'] });
      expect(record.artifacts).toEqual([]);
    } finally {
      await stack.close();
    }
  });
});

describe('A11 — usage 采集诚实性', () => {
  it('transcript 缺失 → costComplete=false、usage 缺省（不编造 0）', async () => {
    const emptyDataDir = mkdtempSync(join(tmpdir(), 'bmx-as-claude-'));
    const stack = await makeStack({
      runNode: okRunNode(),
      collectUsage: createClaudeUsageCollector({ claudeDataDir: emptyDataDir }),
    });
    const body = buildRunBody({ runId: 'run-a11-miss1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.costComplete).toBe(false);
      expect('usage' in record).toBe(false); // 缺省，而非 0 值假账
    } finally {
      await stack.close();
      rmSync(emptyDataDir, { recursive: true, force: true });
    }
  });

  it('transcript 命中 → 4-bucket usage 上账，但 v1 无 usd → costComplete 仍 false', async () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bmx-as-claude2-'));
    const stack = await makeStack({
      runNode: okRunNode(),
      collectUsage: createClaudeUsageCollector({ claudeDataDir: dataDir }),
    });
    const runId = 'run-a11-hit12';
    const body = buildRunBody({ runId, goal: 'g', cwd: stack.cwd });
    try {
      // 转录路径在 spawn 前即可预计算：realpath(cwd) 打键 + 冻结的 sessionId。
      const projectKey = realpathSync(stack.cwd).replace(/[^A-Za-z0-9-]/g, '-');
      const transcriptDir = join(dataDir, 'projects', projectKey);
      mkdirSync(transcriptDir, { recursive: true });
      writeFileSync(join(transcriptDir, `${frozenSessionIdFor(runId)}.jsonl`), [
        JSON.stringify({ type: 'assistant', message: { id: 'msg_1', model: 'claude-sonnet-4-5', usage: { input_tokens: 11, output_tokens: 7, cache_read_input_tokens: 3, cache_creation_input_tokens: 2 } } }),
        JSON.stringify({ type: 'assistant', message: { id: 'msg_2', model: 'claude-sonnet-4-5', usage: { input_tokens: 5, output_tokens: 4, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } }),
        '',
      ].join('\n'));

      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record } = await awaitTerminal(stack.socketPath, runId);
      expect(record.usage).toMatchObject({
        model: 'claude-sonnet-4-5',
        inputTokens: 16,
        outputTokens: 11,
        cacheReadTokens: 3,
        cacheCreationTokens: 2,
        turns: 2,
      });
      expect(record.usage.usd).toBeUndefined();
      expect(record.costComplete).toBe(false); // 真实装配恒 false：无定价表
    } finally {
      await stack.close();
      rmSync(dataDir, { recursive: true, force: true });
    }
  });

  it('非 claude 家族 CLI → costComplete=false', () => {
    const collect = createClaudeUsageCollector({ claudeDataDir: '/nonexistent' });
    expect(collect({ cliId: 'codex', sessionId: 's', cwd: '/tmp' })).toEqual({ costComplete: false });
  });

  it('fake collector 才能走 costComplete=true 分支（测试注入）', async () => {
    const stack = await makeStack({
      runNode: okRunNode(),
      collectUsage: () => ({
        usage: { model: 'fake-model', inputTokens: 1, outputTokens: 2, cacheReadTokens: 0, cacheCreationTokens: 0, turns: 1, usd: 0.42 },
        costComplete: true,
      }),
    });
    const body = buildRunBody({ runId: 'run-a11-fake1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.costComplete).toBe(true);
      expect(record.usage.usd).toBe(0.42);
    } finally {
      await stack.close();
    }
  });
});

describe('A15 — secrets 扫描', () => {
  it('marker secret 不出现在 runDir 账本任何文件与任何 wire 响应中', async () => {
    const MARKER = 'sk-MARKER-c4f1e9d2b7a83e55';
    // 模拟 profile secret resolver：runNode（=pool 的替身）在 spawn 时消费，
    // 但绝不落盘 —— BotSnapshot 本身无 secret 字段（v3 契约）。
    const resolveSecretFake = (_larkAppId: string): string => MARKER;
    const inner = okRunNode();
    const stack = await makeStack({
      runNode: async (req) => {
        const secret = resolveSecretFake(req.botSnapshot.larkAppId);
        expect(secret).toBe(MARKER); // 消费得到，仅存在于内存
        // ledger-before-spawn：runNode 被调用时 request/session 必须已落盘
        expect(stack.store.exists(req.runId)).toBe(true);
        expect(stack.store.readSession(req.runId)).toBeDefined();
        return inner(req);
      },
    });
    const body = buildRunBody({ runId: 'run-a15-secret', goal: 'do the thing', cwd: stack.cwd });
    try {
      const responses: string[] = [];
      responses.push((await wire(stack.socketPath, 'POST', '/v1/runs', body)).text);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.state).toBe('succeeded');
      responses.push((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/result`)).text);
      responses.push((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events?follow=0`)).text);

      for (const text of responses) expect(text).not.toContain(MARKER);
      for (const file of walkFiles(join(stack.runsRoot, body.runId))) {
        expect(readFileSync(file, 'utf-8'), file).not.toContain(MARKER);
      }
      // goal 走文件不走 argv；request.json 里也不允许出现凭证字段
      const requestJson = JSON.parse(readFileSync(join(stack.runsRoot, body.runId, 'request.json'), 'utf-8'));
      expect(Object.keys(requestJson).sort()).toEqual(
        ['cwd', 'goal', 'profileRef', 'protocol', 'requestHash', 'runId', 'timeoutMs'],
      );
    } finally {
      await stack.close();
    }
  });
});
