import { describe, test, expect, beforeAll, afterAll, beforeEach } from 'bun:test';
import { tmpdir } from 'node:os';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { handleCommandIpcRequest } from '../src/mcp/server.ts';
import { CommandIpcError } from '../src/core/context/import-ipc.ts';
import type { ToolResult } from '../src/mcp/dispatch.ts';
import { resetPgliteState } from './helpers/reset-pglite.ts';

let engine: PGLiteEngine;

beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});

afterAll(async () => {
  await engine.disconnect();
});

beforeEach(async () => {
  await resetPgliteState(engine);
  await engine.setConfig('version', '123');
});

describe('command IPC dispatch trust boundary', () => {
  test('mcp-proxy caller uses untrusted MCP dispatch', async () => {
    const out = await handleCommandIpcRequest(engine, {
      callerKind: 'mcp-proxy',
      op: 'submit_job',
      params: {
        name: 'shell',
        data: {
          cmd: 'echo hello',
          cwd: tmpdir(),
        },
      },
    }, { sourceId: 'default' });

    const result = out.result as ToolResult;
    expect(result.isError).toBe(true);
    const body = JSON.parse(result.content[0]?.text || '{}') as { error?: string; message?: string };
    expect(body.error).toBe('permission_denied');
    expect(body.message).toContain('cannot be submitted over MCP');
  });

  test('trusted CLI caller stays on the local trusted path', async () => {
    const out = await handleCommandIpcRequest(engine, {
      callerKind: 'trusted-cli',
      op: 'submit_job',
      params: {
        name: 'shell',
        data: {
          cmd: 'echo hello',
          cwd: tmpdir(),
        },
      },
    }, { sourceId: 'default' });

    const result = out.result as { id: number; name: string; status: string };
    expect(result.name).toBe('shell');
    expect(result.status).toBe('waiting');
    expect(result.id).toBeGreaterThan(0);
  });

  test('missing or unknown caller kinds fail closed', async () => {
    await expect(handleCommandIpcRequest(engine, {
      op: 'search',
      params: { query: 'example' },
    }, { sourceId: 'default' } as any)).rejects.toMatchObject({
      name: 'CommandIpcError',
      reason: 'protocol',
      code: 'invalid_caller_kind',
    } satisfies Partial<CommandIpcError>);

    await expect(handleCommandIpcRequest(engine, {
      callerKind: 'weird-caller' as any,
      op: 'search',
      params: { query: 'example' },
    }, { sourceId: 'default' })).rejects.toMatchObject({
      name: 'CommandIpcError',
      reason: 'protocol',
      code: 'invalid_caller_kind',
    } satisfies Partial<CommandIpcError>);
  });
});
