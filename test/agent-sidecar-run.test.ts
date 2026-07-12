/**
 * agent-sidecar-run.test.ts — ledger 崩溃窗口与 run 语义：
 * A4 kill-window（journal 终态而 terminal.json 缺失 → 幂等 finalize，无双终态）、
 * dead-lease 非终态 re-drive（§4 不变量4）、A8 manifest 逃逸/绝对路径/sha 不符
 * （真实 validator）、A11 usage 采集失败诚实置 costComplete=false、
 * A13 ASK_HUMAN → blocked + ask 证据、A15 secrets 不落 ledger/不上 wire。
 */

import { describe, it, expect } from 'vitest';
import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, realpathSync, rmSync, statSync, writeFileSync } from 'node:fs';
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
    expect(collect([{ cliId: 'codex', sessionId: 's', cwd: '/tmp' }])).toEqual({ costComplete: false });
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

  it('collector 谎报 costComplete=true 但无 usd → 记录级 gate 钉回 false', async () => {
    const stack = await makeStack({
      runNode: okRunNode(),
      collectUsage: () => ({
        usage: { model: 'liar', inputTokens: 1, outputTokens: 2, cacheReadTokens: 0, cacheCreationTokens: 0, turns: 1 },
        costComplete: true, // 违约声明：wire 契约要求 true 必须带归一化 usd
      }),
    });
    const body = buildRunBody({ runId: 'run-a11-liar1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.costComplete).toBe(false); // canonical owner 不产出自相矛盾记录
      expect(record.usage).toMatchObject({ model: 'liar' }); // usage 本身仍如实保留
    } finally {
      await stack.close();
    }
  });

  it('turns NaN/负数、model 空串 → 同样钉回 false（NaN 持久化会变 null 毒化消费方）', async () => {
    const cases = [
      { label: 'turns-nan', usage: { model: 'm', inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheCreationTokens: 0, turns: Number.NaN, usd: 0.1 } },
      { label: 'turns-neg', usage: { model: 'm', inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheCreationTokens: 0, turns: -1, usd: 0.1 } },
      { label: 'model-empty', usage: { model: '', inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheCreationTokens: 0, turns: 1, usd: 0.1 } },
    ];
    for (const [i, c] of cases.entries()) {
      const stack = await makeStack({
        runNode: okRunNode(),
        collectUsage: () => ({ usage: c.usage as any, costComplete: true }),
      });
      const body = buildRunBody({ runId: `run-a11-inv${i}xx`, goal: 'g', cwd: stack.cwd });
      try {
        await wire(stack.socketPath, 'POST', '/v1/runs', body);
        const { record } = await awaitTerminal(stack.socketPath, body.runId);
        expect(record.costComplete, c.label).toBe(false);
      } finally {
        await stack.close();
      }
    }
  });

  it('usd 非有限/负数同样钉回 false', async () => {
    for (const usd of [Number.NaN, -0.01]) {
      const stack = await makeStack({
        runNode: okRunNode(),
        collectUsage: () => ({
          usage: { model: 'm', inputTokens: 1, outputTokens: 1, cacheReadTokens: 0, cacheCreationTokens: 0, turns: 1, usd },
          costComplete: true,
        }),
      });
      const body = buildRunBody({ runId: `run-a11-bad${usd < 0 ? 'neg' : 'nan'}`, goal: 'g', cwd: stack.cwd });
      try {
        await wire(stack.socketPath, 'POST', '/v1/runs', body);
        const { record } = await awaitTerminal(stack.socketPath, body.runId);
        expect(record.costComplete, String(usd)).toBe(false);
      } finally {
        await stack.close();
      }
    }
  });

  it('usage 采集范围 = 全部 attempt session（retry 的花费也是本 run 的花费）', async () => {
    // 多 attempt 的账本：journal 里两条 nodeSessionReady（s1 重试后 s2），
    // finalize 必须把两个 session 都交给 collector 聚合。
    const seen: string[][] = [];
    const stack = await makeStack({
      runNode: okRunNode(),
      collectUsage: (sessions) => {
        seen.push(sessions.map((s) => s.sessionId));
        return { costComplete: false };
      },
    });
    const runId = 'run-a11-multi1';
    const body = buildRunBody({ runId, goal: 'g', cwd: stack.cwd });
    try {
      stack.store.writeRequest(body);
      stack.store.writeSession(runId, { cliId: 'claude-code', sessionId: 'sess-frozen', cwd: stack.cwd });
      const journalPath = stack.store.journalPath(runId);
      appendEvent(journalPath, { type: 'runStarted', runId });
      appendEvent(journalPath, { type: 'nodeDispatched', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/001' });
      appendEvent(journalPath, { type: 'nodeSessionReady', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/001', sessionInfo: { sessionId: 'sess-attempt-1' } });
      appendEvent(journalPath, { type: 'nodeFailed', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/001', errorClass: 'workerError' });
      appendEvent(journalPath, { type: 'nodeRetryRequested', nodeId: 'goal', instanceId: 'goal#001', previousAttemptId: 'goal#001/attempts/001', nextAttemptId: 'goal#001/attempts/002', reason: 'blockedRetry' });
      appendEvent(journalPath, { type: 'nodeSessionReady', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/002', sessionInfo: { sessionId: 'sess-attempt-2' } });
      appendEvent(journalPath, { type: 'nodeFailed', nodeId: 'goal', instanceId: 'goal#001', attemptId: 'goal#001/attempts/002', errorClass: 'workerError' });
      appendEvent(journalPath, { type: 'runFailed', failedNodeId: 'goal' });

      const { record } = await awaitTerminal(stack.socketPath, runId);
      expect(record.state).toBe('failed');
      expect(seen).toEqual([['sess-attempt-1', 'sess-attempt-2']]); // journal 真相，全量按序
      expect(record.sessionId).toBe('sess-attempt-2'); // 对外 session = 最后一个 attempt
    } finally {
      await stack.close();
    }
  });

  it('createClaudeUsageCollector 跨 attempt 聚合；缺一个 transcript 仍如实报部分 usage', () => {
    const dataDir = mkdtempSync(join(tmpdir(), 'bmx-as-claude3-'));
    try {
      const cwd = mkdtempSync(join(tmpdir(), 'bmx-as-cwd3-'));
      const projectKey = realpathSync(cwd).replace(/[^A-Za-z0-9-]/g, '-');
      const transcriptDir = join(dataDir, 'projects', projectKey);
      mkdirSync(transcriptDir, { recursive: true });
      const line = (id: string, inTok: number, outTok: number): string =>
        JSON.stringify({ type: 'assistant', message: { id, model: 'claude-sonnet-4-5', usage: { input_tokens: inTok, output_tokens: outTok, cache_read_input_tokens: 0, cache_creation_input_tokens: 0 } } });
      writeFileSync(join(transcriptDir, 'sess-a.jsonl'), line('m1', 10, 5) + '\n');
      writeFileSync(join(transcriptDir, 'sess-b.jsonl'), line('m2', 7, 3) + '\n');

      const collect = createClaudeUsageCollector({ claudeDataDir: dataDir });
      const both = collect([
        { cliId: 'claude-code', sessionId: 'sess-a', cwd },
        { cliId: 'claude-code', sessionId: 'sess-b', cwd },
      ]) as { usage?: any; costComplete: boolean };
      expect(both.usage).toMatchObject({ inputTokens: 17, outputTokens: 8, turns: 2 });
      expect(both.costComplete).toBe(false);

      const partial = collect([
        { cliId: 'claude-code', sessionId: 'sess-a', cwd },
        { cliId: 'claude-code', sessionId: 'sess-missing', cwd },
      ]) as { usage?: any; costComplete: boolean };
      expect(partial.usage).toMatchObject({ inputTokens: 10, outputTokens: 5, turns: 1 }); // 部分如实
      expect(partial.costComplete).toBe(false);
    } finally {
      rmSync(dataDir, { recursive: true, force: true });
    }
  });
});

describe('REAL_RUNS_DISABLED — v1 真实组合根 fail-closed', () => {
  it('realRunsDisabledReason 置位 → create/attach 一律 403，无任何账本写', async () => {
    const stack = await makeStack({
      runNode: okRunNode(),
      realRunsDisabledReason: 'v1 sidecar is contract-proof only (test)',
    });
    const body = buildRunBody({ runId: 'run-disabled01', goal: 'g', cwd: stack.cwd });
    try {
      for (let i = 0; i < 2; i++) { // create 与重试 attach 同样被拒
        const res = await wire(stack.socketPath, 'POST', '/v1/runs', body);
        expect(res.status).toBe(403);
        expect(JSON.parse(res.text).error.code).toBe('REAL_RUNS_DISABLED');
      }
      expect(existsSync(join(stack.runsRoot, body.runId))).toBe(false);
      expect(stack.runNodeCalls()).toBe(0);
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
        ['cwd', 'goal', 'mode', 'profileRef', 'protocol', 'requestHash', 'runId', 'timeoutMs'],
      );
    } finally {
      await stack.close();
    }
  });
});
