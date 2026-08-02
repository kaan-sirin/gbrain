import { afterAll, beforeAll, describe, expect, test } from 'bun:test';
import { execFileSync, spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import { commandSocketPath, mcpProxySocketPath, probeCommandIpc } from '../../src/core/local-command-ipc.ts';

function testEnv(home: string): Record<string, string> {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key, value]) =>
    value !== undefined && key !== 'DATABASE_URL' && key !== 'GBRAIN_DATABASE_URL',
  )) as Record<string, string>;
  return { ...env, GBRAIN_HOME: home, NODE_ENV: 'test' };
}

async function waitForSocket(path: string): Promise<void> {
  let last: unknown;
  for (let attempt = 0; attempt < 40; attempt++) {
    try { await probeCommandIpc(path); return; }
    catch (error) { last = error; await new Promise(resolve => setTimeout(resolve, 125)); }
  }
  throw last instanceof Error ? last : new Error(`IPC endpoint was not ready: ${path}`);
}

function waitForStderr(child: ChildProcess, needle: string, timeoutMs = 5_000): Promise<void> {
  return new Promise((resolve, reject) => {
    let output = '';
    const timeout = setTimeout(() => reject(new Error(`Timed out waiting for ${needle}: ${output}`)), timeoutMs);
    const done = (error?: Error) => {
      clearTimeout(timeout);
      child.stderr?.off('data', onData);
      child.off('exit', onExit);
      if (error) reject(error); else resolve();
    };
    const onData = (chunk: Buffer) => {
      output += chunk.toString();
      if (output.includes(needle)) done();
    };
    const onExit = (code: number | null) => done(new Error(`Proxy exited before ${needle} (code ${code}): ${output}`));
    child.stderr?.on('data', onData);
    child.once('exit', onExit);
  });
}

function waitForExit(child: ChildProcess, timeoutMs = 5_000): Promise<number | null> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error('Timed out waiting for proxy exit.')), timeoutMs);
    child.once('exit', (code) => {
      clearTimeout(timeout);
      resolve(code);
    });
  });
}

describe('serve --local-daemon with DB-free stdio proxies', () => {
  let home: string;
  let env: Record<string, string>;
  let daemon: ChildProcess;
  let commandSocket: string;
  const clients: Client[] = [];
  const transports: StdioClientTransport[] = [];
  const proxies: ChildProcess[] = [];

  beforeAll(async () => {
    home = mkdtempSync(join(tmpdir(), 'gbrain-ipc-proxy-e2e-'));
    env = testEnv(home);
    execFileSync('bun', ['run', 'src/cli.ts', 'init', '--pglite', '--no-embedding', '--non-interactive'], {
      cwd: process.cwd(), env, stdio: 'ignore',
    });
    const config = JSON.parse(readFileSync(join(home, '.gbrain', 'config.json'), 'utf8')) as { database_path: string };
    commandSocket = commandSocketPath(config.database_path);
    daemon = spawn('bun', ['run', 'src/cli.ts', 'serve', '--local-daemon'], {
      cwd: process.cwd(), env, stdio: ['ignore', 'ignore', 'pipe'],
    });
    await waitForSocket(mcpProxySocketPath(config.database_path));
  }, 60_000);

  afterAll(async () => {
    for (const client of clients) { try { await client.close(); } catch { /* best effort */ } }
    for (const transport of transports) { try { await transport.close(); } catch { /* best effort */ } }
    for (const proxy of proxies) {
      if (proxy.exitCode === null) proxy.kill('SIGTERM');
    }
    if (daemon && !daemon.killed) daemon.kill('SIGTERM');
    if (home) rmSync(home, { recursive: true, force: true });
  });

  test('two stdio clients concurrently use the one daemon-owned PGLite engine', async () => {
    const connect = async (name: string) => {
      const transport = new StdioClientTransport({
        command: 'bun', args: ['run', 'src/cli.ts', 'serve', '--ipc-proxy'], cwd: process.cwd(), env,
      });
      const client = new Client({ name, version: '1.0.0' }, { capabilities: {} });
      transports.push(transport);
      clients.push(client);
      await client.connect(transport);
      return client;
    };
    const [one, two] = await Promise.all([connect('proxy-one'), connect('proxy-two')]);
    const [toolsOne, toolsTwo, identityOne, identityTwo] = await Promise.all([
      one.listTools(),
      two.listTools(),
      one.callTool({ name: 'get_brain_identity', arguments: {} }),
      two.callTool({ name: 'get_brain_identity', arguments: {} }),
    ]);
    expect(toolsOne.tools.some(tool => tool.name === 'get_brain_identity')).toBe(true);
    expect(toolsTwo.tools.some(tool => tool.name === 'get_brain_identity')).toBe(true);
    expect(identityOne.isError).not.toBe(true);
    expect(identityTwo.isError).not.toBe(true);
  }, 60_000);

  test('CLI import delegates to the daemon instead of colliding with its PGLite lock', () => {
    const source = mkdtempSync(join(tmpdir(), 'gbrain-ipc-import-e2e-'));
    try {
      writeFileSync(join(source, 'delegated.md'), '---\ntitle: Delegated\n---\nImported while the daemon owns PGLite.\n');
      const raw = execFileSync('bun', ['run', 'src/cli.ts', 'import', source, '--no-embed', '--json'], {
        cwd: process.cwd(), env, encoding: 'utf8',
      });
      expect(JSON.parse(raw)).toMatchObject({ status: 'success', imported: 1, errors: 0 });
    } finally {
      rmSync(source, { recursive: true, force: true });
    }
  }, 60_000);

  test('a rejected delegated import does not terminate the database-owning daemon', async () => {
    const result = spawnSync('bun', ['run', 'src/cli.ts', 'import', join(home, 'missing'), '--no-embed'], {
      cwd: process.cwd(), env, encoding: 'utf8',
    });
    expect(result.status).toBe(1);
    expect(result.stderr).toContain('Error [import_failed]');
    await expect(probeCommandIpc(commandSocket)).resolves.toBeUndefined();
  }, 60_000);

  test('MCP_STDIO proxy survives closed stdin until SIGTERM', async () => {
    const proxy = spawn('bun', ['run', 'src/cli.ts', 'serve', '--ipc-proxy'], {
      cwd: process.cwd(), env: { ...env, MCP_STDIO: '1' }, stdio: ['pipe', 'ignore', 'pipe'],
    });
    proxies.push(proxy);
    await waitForStderr(proxy, 'Starting GBrain MCP IPC proxy (stdio)...');

    proxy.stdin?.end();
    await new Promise(resolve => setTimeout(resolve, 250));
    expect(proxy.exitCode).toBeNull();

    const exited = waitForExit(proxy);
    proxy.kill('SIGTERM');
    // The CLI's process-cleanup signal path owns the production exit status.
    // It exits 143 before the graceful lifecycle callback can overwrite it.
    expect(await exited).toBe(143);
  }, 15_000);
});
