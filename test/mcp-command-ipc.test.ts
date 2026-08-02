import { afterAll, beforeAll, beforeEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { PGLiteEngine } from '../src/core/pglite-engine.ts';
import { handleCommandIpcRequest, startIpcProxyMcpServer } from '../src/mcp/server.ts';
import type { ToolResult } from '../src/mcp/dispatch.ts';
import { LOCAL_CLI_IMPORT_OP } from '../src/core/local-command-ipc.ts';
import { resetPgliteState } from './helpers/reset-pglite.ts';

let engine: PGLiteEngine;
beforeAll(async () => {
  engine = new PGLiteEngine();
  await engine.connect({});
  await engine.initSchema();
});
afterAll(async () => { await engine.disconnect(); });
beforeEach(async () => {
  await resetPgliteState(engine);
  await engine.setConfig('version', '123');
});

describe('command IPC trust boundary', () => {
  test('MCP proxy endpoint remains remote and protected operations fail', async () => {
    const output = await handleCommandIpcRequest(engine, {
      op: 'submit_job',
      params: { name: 'shell', data: { cmd: 'echo blocked', cwd: tmpdir() } },
    }, 'mcp-proxy');
    const result = output.result as ToolResult;
    expect(result.isError).toBe(true);
    expect(JSON.parse(result.content[0]!.text)).toMatchObject({ error: 'permission_denied' });
  });

  test('trusted CLI endpoint retains remote:false local execution', async () => {
    const output = await handleCommandIpcRequest(engine, {
      op: 'submit_job',
      params: { name: 'shell', data: { cmd: 'echo allowed', cwd: tmpdir() } },
      cwd: process.cwd(),
    }, 'trusted-cli');
    expect(output.result).toMatchObject({ name: 'shell', status: 'waiting' });
  });

  test('trusted CLI endpoint delegates import without a second PGLite owner', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'gbrain-ipc-import-'));
    try {
      writeFileSync(join(dir, 'page.md'), '---\ntitle: IPC import\n---\nImported through the daemon.\n');
      const output = await handleCommandIpcRequest(engine, {
        op: LOCAL_CLI_IMPORT_OP,
        params: { args: [dir, '--no-embed'] },
        cwd: process.cwd(),
      }, 'trusted-cli');
      expect(output.result).toMatchObject({ imported: 1, errors: 0 });
      expect(await engine.getPage('page')).toMatchObject({ title: 'IPC import' });
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  test('MCP proxy cannot invoke the private CLI import operation', async () => {
    const output = await handleCommandIpcRequest(engine, {
      op: LOCAL_CLI_IMPORT_OP,
      params: { args: ['/tmp', '--no-embed'] },
    }, 'mcp-proxy');
    const result = output.result as ToolResult;
    expect(result.isError).toBe(true);
  });

  test('an IPC proxy transport close requests shutdown exactly once', async () => {
    let transportClose: (() => void) | undefined;
    let shutdowns = 0;
    const transport = {
      async start() {},
      async send() {},
      async close() { transportClose?.(); },
      set onclose(fn: (() => void) | undefined) { transportClose = fn; },
      get onclose() { return transportClose; },
    };

    await startIpcProxyMcpServer(
      'unused.sock',
      () => { shutdowns++; },
      () => transport as unknown as import('@modelcontextprotocol/sdk/server/stdio.js').StdioServerTransport,
    );
    await transport.close();
    await transport.close();
    expect(shutdowns).toBe(1);
  });
});
