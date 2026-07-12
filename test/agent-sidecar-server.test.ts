/**
 * agent-sidecar-server.test.ts — UDS wire 面：create-or-attach 幂等（A1/A2/A3）、
 * events seq 重放（A5）、result 永久重放（A6）、cancel 幂等收敛（A7）、
 * cwd 越界/symlink 逃逸（A9）、非 sandbox profile（A10）、未知 runId（A14）、
 * socket 权限与 stale unlink。全部 in-process http.request({socketPath})。
 */

import { describe, it, expect } from 'vitest';
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { createServer, request as httpRequest } from 'node:http';
import { spawnSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { createSidecarServer, listenOnSocket } from '../src/agent-sidecar/server.js';
import type { SidecarEventFrame } from '../src/agent-sidecar/contract.js';
import {
  awaitTerminal,
  buildRunBody,
  gatedRunNode,
  hangUntilAbortRunNode,
  makeStack,
  okRunNode,
  parseFrames,
  wire,
} from './agent-sidecar-harness.js';

function expectGapless(frames: SidecarEventFrame[], since = 0): void {
  let prev = since;
  for (const f of frames) {
    expect(f.seq).toBe(prev + 1);
    prev = f.seq;
  }
}

describe('health', () => {
  it('声明 protocol 与 {input:false, human:false}', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      const res = await wire(stack.socketPath, 'GET', '/v1/health');
      expect(res.status).toBe(200);
      const body = JSON.parse(res.text);
      expect(body.protocol).toBe('botmux-goal-v1');
      expect(body.capabilities).toEqual({ input: false, human: false });
      expect(typeof body.pid).toBe('number');
    } finally {
      await stack.close();
    }
  });
});

describe('POST /v1/runs — create-or-attach', () => {
  it('A1: 同 runId+同 hash 三态 attach（live / 终态 / 重启后），只执行一次', async () => {
    const gate = gatedRunNode(okRunNode());
    const stack = await makeStack({ runNode: gate.runNode });
    const body = buildRunBody({ runId: 'run-a1-attach', goal: 'do it', cwd: stack.cwd });
    try {
      const created = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(created.status).toBe(201);
      expect(JSON.parse(created.text)).toMatchObject({ created: true, state: 'running', capabilities: { input: false, human: false } });

      // live attach
      const live = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(live.status).toBe(200);
      expect(JSON.parse(live.text)).toMatchObject({ created: false, state: 'running' });

      gate.release();
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.state).toBe('succeeded');

      // terminal attach
      const after = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(after.status).toBe(200);
      expect(JSON.parse(after.text)).toMatchObject({ created: false, state: 'succeeded' });

      // restart attach：同 runsRoot 上起全新 server/store/driver
      const stack2 = await makeStack({
        runNode: okRunNode(),
        reuseRunsRoot: stack.runsRoot,
        extraWorkspaceRoots: [stack.wsRoot],
      });
      try {
        const restarted = await wire(stack2.socketPath, 'POST', '/v1/runs', body);
        expect(restarted.status).toBe(200);
        expect(JSON.parse(restarted.text)).toMatchObject({ created: false, state: 'succeeded' });
        const { record: record2 } = await awaitTerminal(stack2.socketPath, body.runId);
        expect(record2).toEqual(record); // 同一逻辑终态，重启不漂移
        expect(stack2.runNodeCalls()).toBe(0); // 重启后 attach 不重跑
      } finally {
        await stack2.close();
      }
      expect(stack.runNodeCalls()).toBe(1); // 全程只执行一次
    } finally {
      await stack.close();
    }
  });

  it('A2: 同 runId+异 hash → 409 IDEMPOTENCY_CONFLICT，不触发执行', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    const body = buildRunBody({ runId: 'run-a2-conflict', goal: 'original goal', cwd: stack.cwd });
    try {
      expect((await wire(stack.socketPath, 'POST', '/v1/runs', body)).status).toBe(201);
      await awaitTerminal(stack.socketPath, body.runId);
      const callsAfterFirst = stack.runNodeCalls();

      const other = buildRunBody({ runId: body.runId, goal: 'DIFFERENT goal', cwd: stack.cwd });
      const res = await wire(stack.socketPath, 'POST', '/v1/runs', other);
      expect(res.status).toBe(409);
      const err = JSON.parse(res.text).error;
      expect(err.code).toBe('IDEMPOTENCY_CONFLICT');
      expect(err.expectedHash).toBe(body.requestHash);
      expect(err.receivedHash).toBe(other.requestHash);
      expect(stack.runNodeCalls()).toBe(callsAfterFirst);
    } finally {
      await stack.close();
    }
  });

  it('A3: 客户端 hash 与 payload 不符 → 400 HASH_MISMATCH，不落 ledger', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      const body = { ...buildRunBody({ runId: 'run-a3-hashbad', goal: 'g', cwd: stack.cwd }), requestHash: 'f'.repeat(64) };
      const res = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(res.status).toBe(400);
      const err = JSON.parse(res.text).error;
      expect(err.code).toBe('HASH_MISMATCH');
      expect(err.receivedHash).toBe('f'.repeat(64));
      expect(existsSync(join(stack.runsRoot, 'run-a3-hashbad'))).toBe(false);
      expect(stack.runNodeCalls()).toBe(0);
    } finally {
      await stack.close();
    }
  });

  it('malformed body → 400 MALFORMED_REQUEST（协议/runId 形状/timeoutMs）', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      for (const bad of [
        'not json at all',
        { ...buildRunBody({ runId: 'run-shape-ok1', goal: 'g', cwd: stack.cwd }), protocol: 'nope' },
        { ...buildRunBody({ runId: 'run-shape-ok2', goal: 'g', cwd: stack.cwd }), runId: '../etc' },
        { ...buildRunBody({ runId: 'run-shape-ok3', goal: 'g', cwd: stack.cwd }), timeoutMs: -5 },
      ]) {
        const res = await wire(stack.socketPath, 'POST', '/v1/runs', bad);
        expect(res.status).toBe(400);
        expect(JSON.parse(res.text).error.code).toBe('MALFORMED_REQUEST');
      }
    } finally {
      await stack.close();
    }
  });

  it('A9: cwd 越界 / symlink 逃逸 → 403 CWD_NOT_ALLOWED，不落 ledger', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      // 直接越界
      const outside = mkdtempSync(join(tmpdir(), 'bmx-as-out-'));
      const r1 = await wire(stack.socketPath, 'POST', '/v1/runs',
        buildRunBody({ runId: 'run-a9-outside1', goal: 'g', cwd: outside }));
      expect(r1.status).toBe(403);
      expect(JSON.parse(r1.text).error.code).toBe('CWD_NOT_ALLOWED');

      // symlink 在 root 内、指向 root 外 → realpath 判逃逸
      const link = join(stack.wsRoot, 'sneaky');
      symlinkSync(outside, link);
      const r2 = await wire(stack.socketPath, 'POST', '/v1/runs',
        buildRunBody({ runId: 'run-a9-symlink1', goal: 'g', cwd: link }));
      expect(r2.status).toBe(403);
      expect(JSON.parse(r2.text).error.code).toBe('CWD_NOT_ALLOWED');

      // 不存在的 cwd 同样拒绝
      const r3 = await wire(stack.socketPath, 'POST', '/v1/runs',
        buildRunBody({ runId: 'run-a9-missing1', goal: 'g', cwd: join(stack.wsRoot, 'no-such-dir') }));
      expect(r3.status).toBe(403);

      for (const id of ['run-a9-outside1', 'run-a9-symlink1', 'run-a9-missing1']) {
        expect(existsSync(join(stack.runsRoot, id))).toBe(false);
      }
      expect(stack.runNodeCalls()).toBe(0);
      rmSync(outside, { recursive: true, force: true });
    } finally {
      await stack.close();
    }
  });

  it('A10: 非 sandbox profile → 403 PROFILE_NOT_SANDBOXED；未知 profile → 400', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      const r1 = await wire(stack.socketPath, 'POST', '/v1/runs',
        buildRunBody({ runId: 'run-a10-nosbx1', goal: 'g', cwd: stack.cwd, profileRef: 'no-sandbox' }));
      expect(r1.status).toBe(403);
      expect(JSON.parse(r1.text).error.code).toBe('PROFILE_NOT_SANDBOXED');

      const r2 = await wire(stack.socketPath, 'POST', '/v1/runs',
        buildRunBody({ runId: 'run-a10-ghost1', goal: 'g', cwd: stack.cwd, profileRef: 'ghost' }));
      expect(r2.status).toBe(400);
      expect(JSON.parse(r2.text).error.code).toBe('UNKNOWN_PROFILE');

      expect(existsSync(join(stack.runsRoot, 'run-a10-nosbx1'))).toBe(false);
      expect(stack.runNodeCalls()).toBe(0);
    } finally {
      await stack.close();
    }
  });

  it('discovery-safe 门：有沙箱但网开 / bypass 未武装 → 403 PROFILE_NOT_DISCOVERY_SAFE', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      for (const [runId, profileRef] of [
        ['run-dsafe-neton1', 'sandbox-net-on'],
        ['run-dsafe-bypass', 'sandbox-bypassable'],
      ] as const) {
        const r = await wire(stack.socketPath, 'POST', '/v1/runs',
          buildRunBody({ runId, goal: 'g', cwd: stack.cwd, profileRef }));
        expect(r.status, profileRef).toBe(403);
        expect(JSON.parse(r.text).error.code, profileRef).toBe('PROFILE_NOT_DISCOVERY_SAFE');
        expect(existsSync(join(stack.runsRoot, runId)), profileRef).toBe(false); // 门先于账本
      }
      expect(stack.runNodeCalls()).toBe(0);
    } finally {
      await stack.close();
    }
  });

  it('mode 缺失或非 discovery → 400 MALFORMED_REQUEST（v1 只收 discovery）', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      const good = buildRunBody({ runId: 'run-mode-miss1', goal: 'g', cwd: stack.cwd });
      const { mode: _m, ...withoutMode } = good as any;
      const r1 = await wire(stack.socketPath, 'POST', '/v1/runs', withoutMode);
      expect(r1.status).toBe(400);
      expect(JSON.parse(r1.text).error.code).toBe('MALFORMED_REQUEST');

      const r2 = await wire(stack.socketPath, 'POST', '/v1/runs', { ...good, mode: 'execute' });
      expect(r2.status).toBe(400);
      expect(JSON.parse(r2.text).error.code).toBe('MALFORMED_REQUEST');
      expect(stack.runNodeCalls()).toBe(0);
    } finally {
      await stack.close();
    }
  });
});

describe('GET /v1/runs/:id/events', () => {
  it('A5: since 重放前缀一致、seq 无缝、terminal 帧唯一；session 帧剥 webPort/token', async () => {
    const stack = await makeStack({ runNode: okRunNode({ sessionId: 'sess-1234' }) });
    const body = buildRunBody({ runId: 'run-a5-events1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      await awaitTerminal(stack.socketPath, body.runId);

      const full = parseFrames((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events?follow=0`)).text);
      expectGapless(full);
      expect(full[0]!.event).toEqual({ type: 'run.accepted' });
      expect(full.filter((f) => f.event.type === 'terminal')).toHaveLength(1);
      expect(full.at(-1)!.event).toEqual({ type: 'terminal', state: 'succeeded' });

      const session = full.find((f) => f.event.type === 'session');
      expect(session).toBeDefined();
      expect(session!.event).toEqual({ type: 'session', sessionId: 'sess-1234' }); // 无 webPort/token
      expect(JSON.stringify(full)).not.toContain('tok-must-never-leak');
      expect(JSON.stringify(full)).not.toContain('40123');

      // since=N 重放：等于全量的后缀（前缀一致 + append-only）
      const since = 2;
      const tail = parseFrames((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events?since=${since}&follow=0`)).text);
      expect(tail).toEqual(full.filter((f) => f.seq > since));
      expectGapless(tail, since);

      // follow 模式对已终态 run：完整重放后正常关闭
      const followed = parseFrames((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events`)).text);
      expect(followed).toEqual(full);
    } finally {
      await stack.close();
    }
  });

  it('follow 模式跟随 live run 直到 terminal 帧', async () => {
    const gate = gatedRunNode(okRunNode());
    const stack = await makeStack({ runNode: gate.runNode });
    const body = buildRunBody({ runId: 'run-follow-live', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const streaming = wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events`);
      setTimeout(() => gate.release(), 50);
      const frames = parseFrames((await streaming).text);
      expectGapless(frames);
      expect(frames.at(-1)!.event).toEqual({ type: 'terminal', state: 'succeeded' });
      expect(frames.filter((f) => f.event.type === 'terminal')).toHaveLength(1);
    } finally {
      await stack.close();
    }
  });
});

describe('GET /v1/runs/:id/result', () => {
  it('运行中 202；A6: 终态后永久可重放，字节级同一记录', async () => {
    const gate = gatedRunNode(okRunNode());
    const stack = await makeStack({ runNode: gate.runNode });
    const body = buildRunBody({ runId: 'run-a6-result1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);
      const pending = await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/result`);
      expect(pending.status).toBe(202);

      gate.release();
      await awaitTerminal(stack.socketPath, body.runId);
      const first = await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/result`);
      const second = await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/result`);
      expect(first.status).toBe(200);
      expect(second.text).toBe(first.text); // 字节级一致

      const record = JSON.parse(first.text);
      expect(record).toMatchObject({
        protocol: 'botmux-goal-v1',
        runId: body.runId,
        state: 'succeeded',
        costComplete: false,
      });
      expect(record.artifacts).toHaveLength(1);
      expect(record.artifacts[0]).toMatchObject({ name: 'out.md', path: 'out.md', kind: 'markdown' });
      expect(record.lastSeq).toBeGreaterThan(0);
    } finally {
      await stack.close();
    }
  });
});

describe('POST /v1/runs/:id/cancel — A7', () => {
  it('运行中连击两次幂等；终态后 cancel 返回 alreadyTerminal；全部收敛 cancelled', async () => {
    const stack = await makeStack({ runNode: hangUntilAbortRunNode() });
    const body = buildRunBody({ runId: 'run-a7-cancel1', goal: 'g', cwd: stack.cwd });
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body);

      const c1 = await wire(stack.socketPath, 'POST', `/v1/runs/${body.runId}/cancel`);
      expect(c1.status).toBe(202);
      expect(JSON.parse(c1.text)).toMatchObject({ cancelRequested: true, alreadyTerminal: false });

      const c2 = await wire(stack.socketPath, 'POST', `/v1/runs/${body.runId}/cancel`);
      expect([200, 202]).toContain(c2.status); // 竞态窗口内与当时状态一致
      const c2body = JSON.parse(c2.text);
      if (c2.status === 202) expect(c2body).toMatchObject({ cancelRequested: true, alreadyTerminal: false });
      else expect(c2body).toMatchObject({ state: 'cancelled', alreadyTerminal: true });

      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.state).toBe('cancelled');
      expect(record.error.code).toBe('CANCELLED');

      // 终态后再 cancel：幂等同构结果
      const c3 = await wire(stack.socketPath, 'POST', `/v1/runs/${body.runId}/cancel`);
      expect(c3.status).toBe(200);
      expect(JSON.parse(c3.text)).toEqual({
        runId: body.runId, state: 'cancelled', cancelRequested: false, alreadyTerminal: true,
      });

      // attach / events 也收敛同一终态
      const attach = await wire(stack.socketPath, 'POST', '/v1/runs', body);
      expect(JSON.parse(attach.text)).toMatchObject({ created: false, state: 'cancelled' });
      const frames = parseFrames((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/events?follow=0`)).text);
      expect(frames.filter((f) => f.event.type === 'terminal')).toEqual([
        expect.objectContaining({ event: { type: 'terminal', state: 'cancelled' } }),
      ]);
      expect(frames.at(-1)!.seq).toBe(JSON.parse((await wire(stack.socketPath, 'GET', `/v1/runs/${body.runId}/result`)).text).lastSeq);
    } finally {
      await stack.close();
    }
  });
});

describe('未知 runId — A14', () => {
  it('events/result/cancel 一律 404 UNKNOWN_RUN（含遍历形状）', async () => {
    const stack = await makeStack({ runNode: okRunNode() });
    try {
      for (const [method, path] of [
        ['GET', '/v1/runs/run-never-seen/events'],
        ['GET', '/v1/runs/run-never-seen/result'],
        ['POST', '/v1/runs/run-never-seen/cancel'],
        ['GET', `/v1/runs/${encodeURIComponent('../../etc')}/result`],
      ] as const) {
        const res = await wire(stack.socketPath, method, path);
        expect(res.status, path).toBe(404);
        expect(JSON.parse(res.text).error.code).toBe('UNKNOWN_RUN');
      }
    } finally {
      await stack.close();
    }
  });
});

/** 铸真 stale socket：子进程 listen 成功后 SIGKILL 自杀，socket 文件残留。 */
function makeCrashedSocket(socketPath: string): void {
  const r = spawnSync(process.execPath, ['-e',
    `const net=require('net');const s=net.createServer();s.listen(process.argv[1],()=>{process.kill(process.pid,'SIGKILL')});`,
    socketPath]);
  if (!existsSync(socketPath)) {
    throw new Error(`makeCrashedSocket: no socket file left behind (status=${r.status} signal=${r.signal})`);
  }
}

describe('UDS socket 行为', () => {
  it('父目录 0700、socket 0600、真 stale socket（崩溃残留）启动时回收', async () => {
    const base = mkdtempSync(join(tmpdir(), 'bmx-as-uds-'));
    const dir = join(base, 'holder');
    const socketPath = join(dir, 's.sock');
    mkdirSync(dir, { recursive: true, mode: 0o700 });

    // 铸一个真正的 stale socket：子进程绑定后 SIGKILL 自杀（close 会 unlink，
    // 崩溃不会）——socket 文件残留 + lock 里留一个必死 pid。
    makeCrashedSocket(socketPath);
    writeFileSync(`${socketPath}.lock`, JSON.stringify({ pid: 99999999, startedAt: 0 }));

    const server = createServer(() => {});
    try {
      await listenOnSocket(server, socketPath); // 死 pid lock 回收 + stale socket 清理
      expect(statSync(dir).mode & 0o777).toBe(0o700);
      expect(statSync(socketPath).mode & 0o777).toBe(0o600);
      expect(statSync(socketPath).isSocket()).toBe(true);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('普通文件占位 → 硬失败且绝不删除（非 socket 不是可证 stale）', async () => {
    const base = mkdtempSync(join(tmpdir(), 'bmx-as-uds3-'));
    const socketPath = join(base, 'plain.sock');
    writeFileSync(socketPath, 'i am not a socket');
    const server = createServer(() => {});
    try {
      await expect(listenOnSocket(server, socketPath)).rejects.toThrow(/not a socket/);
      expect(readFileSync(socketPath, 'utf-8')).toBe('i am not a socket'); // 未被动过
      expect(existsSync(`${socketPath}.lock`)).toBe(false); // 失败路径释放 lock
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('并发 stale 启动：ownership lock 保证恰好一个赢家，赢家地址不被 unlink', async () => {
    const base = mkdtempSync(join(tmpdir(), 'bmx-as-uds4-'));
    const socketPath = join(base, 'race.sock');
    // 真 stale socket + 死 pid lock（两个竞争者都面对同一残骸）
    makeCrashedSocket(socketPath);
    writeFileSync(`${socketPath}.lock`, JSON.stringify({ pid: 99999999, startedAt: 0 }));

    const a = createServer((_req, res) => { res.writeHead(200); res.end('a'); });
    const b = createServer((_req, res) => { res.writeHead(200); res.end('b'); });
    try {
      const results = await Promise.allSettled([
        listenOnSocket(a, socketPath),
        listenOnSocket(b, socketPath),
      ]);
      const wins = results.filter((r) => r.status === 'fulfilled').length;
      expect(wins).toBe(1); // 恰好一个成功
      // 赢家的地址仍然可达（没有被输家 unlink/夺址）
      const probe = await new Promise<number>((resolve, reject) => {
        const req = httpRequest({ socketPath, path: '/', method: 'GET' }, (res) => {
          res.resume();
          resolve(res.statusCode ?? 0);
        });
        req.on('error', reject);
        req.end();
      });
      expect(probe).toBe(200);
    } finally {
      await new Promise<void>((r) => a.close(() => r()));
      await new Promise<void>((r) => b.close(() => r()));
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('目录不存在时以 0700 创建', async () => {
    const base = mkdtempSync(join(tmpdir(), 'bmx-as-uds2-'));
    const dir = join(base, 'fresh');
    const server = createServer(() => {});
    try {
      await listenOnSocket(server, join(dir, 's.sock'));
      expect(statSync(dir).mode & 0o777).toBe(0o700);
    } finally {
      await new Promise<void>((r) => server.close(() => r()));
      rmSync(base, { recursive: true, force: true });
    }
  });

  it('单一属主：活 socket 上的第二次启动硬失败，不夺址；旧 server 照常服务与 cancel', async () => {
    const stack = await makeStack({ runNode: hangUntilAbortRunNode() });
    const body = buildRunBody({ runId: 'run-owner-0001', goal: 'g', cwd: stack.cwd });
    const usurper = createServer(() => {});
    try {
      await wire(stack.socketPath, 'POST', '/v1/runs', body); // 旧进程驱动一个活 run

      // 第二个 server 试图绑同一 socket → 拒绝启动（绝不 unlink 活地址）
      await expect(listenOnSocket(usurper, stack.socketPath)).rejects.toThrow(/refusing to st/); // lock 或活 listener 任一道门

      // 旧 server 未被打扰：health 正常，活 run 仍可被 cancel 并收敛 cancelled
      const health = await wire(stack.socketPath, 'GET', '/v1/health');
      expect(health.status).toBe(200);
      const c = await wire(stack.socketPath, 'POST', `/v1/runs/${body.runId}/cancel`);
      expect([200, 202]).toContain(c.status);
      const { record } = await awaitTerminal(stack.socketPath, body.runId);
      expect(record.state).toBe('cancelled');
    } finally {
      await new Promise<void>((r) => usurper.close(() => r()));
      await stack.close();
    }
  });
});
