/**
 * agent-sidecar-contract.test.ts — 协议纯函数：canonical JSON / 请求 hash
 * 黄金向量（矩阵 A16，双仓必须钉死同一字面值）/ runId 正则。
 */

import { describe, it, expect } from 'vitest';

import {
  BOTMUX_GOAL_PROTOCOL,
  SIDECAR_RUN_ID_RE,
  canonicalJson,
  canonicalRequestHash,
  isValidSidecarRunId,
} from '../src/agent-sidecar/contract.js';

const FIXTURE1 = {
  protocol: 'botmux-goal-v1',
  runId: 'run-0001-e2e',
  profileRef: 'sandbox-claude',
  goal: 'Write a haiku about idempotency into out.md',
  cwd: '/tmp/ws/demo',
  timeoutMs: 600000,
  mode: 'discovery',
};

describe('canonicalJson', () => {
  it('键按 UTF-16 code unit 升序、无空白、undefined 整体省略', () => {
    expect(canonicalJson(FIXTURE1)).toBe(
      '{"cwd":"/tmp/ws/demo","goal":"Write a haiku about idempotency into out.md","mode":"discovery","profileRef":"sandbox-claude","protocol":"botmux-goal-v1","runId":"run-0001-e2e","timeoutMs":600000}',
    );
    expect(canonicalJson({ b: 1, a: undefined, c: 'x' })).toBe('{"b":1,"c":"x"}');
  });

  it('数组保序、嵌套递归、null 保留', () => {
    expect(canonicalJson({ z: [3, 1, { b: 2, a: 1 }], n: null })).toBe('{"n":null,"z":[3,1,{"a":1,"b":2}]}');
  });

  it('非有限数字抛错', () => {
    expect(() => canonicalJson({ x: Number.NaN })).toThrow(/non-finite/);
    expect(() => canonicalJson({ x: Infinity })).toThrow(/non-finite/);
  });
});

describe('canonicalRequestHash — 黄金向量 (A16)', () => {
  it('fixture1', () => {
    expect(canonicalRequestHash(FIXTURE1)).toBe(
      'a591a05407c2113911a7c647949342c6e9c4baf3e1d3133800a71815c6c25490',
    );
  });

  it('fixture2 = fixture1 + goal 变更', () => {
    expect(canonicalRequestHash({ ...FIXTURE1, goal: FIXTURE1.goal + ' (changed)' })).toBe(
      '2147c1a7eb21c74e410610f26cfd1c250142d40688e746625c3908a43a6ab746',
    );
  });

  it('fixture3 = fixture1 + 身份字段（taskId/threadId 参与 hash）', () => {
    expect(canonicalRequestHash({ ...FIXTURE1, taskId: 'task-42', threadId: 'thread-7' })).toBe(
      '50264ec35037e92c0dd5d4b4887e3b6d41696906ff2bf15053735180023e1eb5',
    );
  });

  it('requestHash 字段本身不参与 hash', () => {
    expect(canonicalRequestHash({ ...FIXTURE1, requestHash: 'deadbeef' })).toBe(
      canonicalRequestHash(FIXTURE1),
    );
  });
});

describe('runId 正则（先校验后拼路径）', () => {
  it('接受合法形状', () => {
    for (const ok of ['run-0001-e2e', 'A1234567', 'a.b_c-d1234', 'x'.repeat(64)]) {
      expect(isValidSidecarRunId(ok), ok).toBe(true);
    }
  });

  it('拒绝遍历/超长/超短/非法字符', () => {
    for (const bad of ['', 'short', '../../../etc', 'has space 123', '-leadingdash', '.leadingdot', 'x'.repeat(65), 'run/../..x', 42, undefined]) {
      expect(isValidSidecarRunId(bad as any), String(bad)).toBe(false);
    }
    expect(SIDECAR_RUN_ID_RE.source).toBe('^[A-Za-z0-9][A-Za-z0-9._-]{7,63}$');
  });
});

describe('协议常量', () => {
  it('protocol 字面值', () => {
    expect(BOTMUX_GOAL_PROTOCOL).toBe('botmux-goal-v1');
  });
});
