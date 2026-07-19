import { describe, test, expect, beforeAll, afterAll } from 'bun:test';
import { mkdtempSync, mkdirSync, writeFileSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js';
import {
  commandSocketPath,
  probeCommandIpc,
  startCommandIpcServer,
  type CommandIpcRequest,
} from '../../src/core/context/import-ipc.ts';

const PROXY_MARKER = 'ipc-proxy-forward-marker-42';
const CONCURRENCY_MARKER = 'ipc-proxy-concurrency-marker-19';

function buildEnv(home: string): Record<string, string> {
  const env: Record<string, string> = {};
  for (const [k, v] of Object.entries(process.env)) {
    if (v !== undefined && k !== 'DATABASE_URL' && k !== 'GBRAIN_DATABASE_URL') {
      env[k] = v;
    }
  }
  env.GBRAIN_HOME = home;
  return env;
}

function writePgliteConfig(home: string, dataDir: string): void {
  const cfgDir = join(home, '.gbrain');
  mkdirSync(cfgDir, { recursive: true });
  mkdirSync(dataDir, { recursive: true });
  writeFileSync(join(cfgDir, 'config.json'), JSON.stringify({
    engine: 'pglite',
    database_path: dataDir,
  }, null, 2));
}

function textOf(result: unknown): string {
  const content = (result as { content?: Array<{ type?: string; text?: string }> })?.content;
  if (!Array.isArray(content)) return '';
  return content.map((c) => (typeof c?.text === 'string' ? c.text : '')).join('\n');
}

function readPgliteConfig(home: string): { database_path: string } {
  return JSON.parse(readFileSync(join(home, '.gbrain', 'config.json'), 'utf8')) as { database_path: string };
}

async function waitForCommandSocket(socketPath: string, attempts = 30): Promise<void> {
  let lastErr: unknown;
  for (let i = 0; i < attempts; i++) {
    try {
      await probeCommandIpc(socketPath);
      return;
    } catch (err) {
      lastErr = err;
      await new Promise((resolve) => setTimeout(resolve, 250));
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(`Command IPC socket never became ready: ${socketPath}`);
}

describe('serve --ipc-proxy (DB-free stdio MCP proxy)', () => {
  let home: string;
  let dataDir: string;
  let transport: StdioClientTransport | null = null;
  let client: Client | null = null;
  let socketServer: import('node:net').Server | null = null;
  const seen: CommandIpcRequest[] = [];

  beforeAll(async () => {
    home = mkdtempSync(join(tmpdir(), 'gbrain-ipc-proxy-fake-'));
    dataDir = join(home, 'brain');
    writePgliteConfig(home, dataDir);

    socketServer = await startCommandIpcServer(commandSocketPath(dataDir), async (req) => {
      seen.push(req);
      return {
        result: {
          content: [{
            type: 'text',
            text: JSON.stringify({
              marker: PROXY_MARKER,
              op: req.op,
              callerKind: req.callerKind,
            }),
          }],
        },
      };
    });
    if (!socketServer) throw new Error('Failed to start fake command IPC server');

    transport = new StdioClientTransport({
      command: 'bun',
      args: ['run', 'src/cli.ts', 'serve', '--ipc-proxy'],
      cwd: process.cwd(),
      env: buildEnv(home),
    });
    client = new Client({ name: 'gbrain-ipc-proxy-fake', version: '1.0.0' }, { capabilities: {} });
    await client.connect(transport);
  }, 60_000);

  afterAll(async () => {
    if (client) { try { await client.close(); } catch { /* best effort */ } }
    if (transport) { try { await transport.close(); } catch { /* best effort */ } }
    if (socketServer) { try { socketServer.close(); } catch { /* best effort */ } }
    if (home) { try { rmSync(home, { recursive: true, force: true }); } catch { /* best effort */ } }
  });

  test('forwards tool calls over the local command socket without using a local DB owner', async () => {
    const { tools } = await client!.listTools();
    expect(tools.some((tool) => tool.name === 'search')).toBe(true);

    const res = await client!.callTool({ name: 'search', arguments: { query: 'anything', limit: 5 } });
    const body = JSON.parse(textOf(res)) as { marker: string; op: string; callerKind: string };
    expect(body.marker).toBe(PROXY_MARKER);
    expect(body.op).toBe('search');
    expect(body.callerKind).toBe('mcp-proxy');
    expect(seen).toEqual([{
      callerKind: 'mcp-proxy',
      op: 'search',
      params: { query: 'anything', limit: 5 },
    }]);
  });

  test('fails fast with an actionable error when the daemon socket is missing', async () => {
    const missingHome = mkdtempSync(join(tmpdir(), 'gbrain-ipc-proxy-missing-'));
    const missingDir = join(missingHome, 'brain');
    writePgliteConfig(missingHome, missingDir);
    try {
      const res = spawnSync('bun', ['run', 'src/cli.ts', 'serve', '--ipc-proxy'], {
        cwd: process.cwd(),
        env: buildEnv(missingHome),
        encoding: 'utf8',
      });
      expect(res.status).toBe(1);
      expect(res.stderr).toContain('gbrain serve --local-daemon');
      expect(res.stderr).toContain(commandSocketPath(missingDir));
    } finally {
      rmSync(missingHome, { recursive: true, force: true });
    }
  });
});

describe('serve --ipc-proxy with a real local daemon owner', () => {
  let home: string;
  let env: Record<string, string>;
  let daemon: ReturnType<typeof spawn> | null = null;
  let client1: Client | null = null;
  let client2: Client | null = null;
  let transport1: StdioClientTransport | null = null;
  let transport2: StdioClientTransport | null = null;

  beforeAll(async () => {
    home = mkdtempSync(join(tmpdir(), 'gbrain-ipc-proxy-real-'));
    env = buildEnv(home);

    execFileSync('bun', ['run', 'src/cli.ts', 'init', '--pglite', '--non-interactive'], {
      cwd: process.cwd(),
      env,
      stdio: 'ignore',
    });

    const notes = join(home, 'notes');
    mkdirSync(notes, { recursive: true });
    writeFileSync(
      join(notes, 'marker.md'),
      `---\ntitle: ${CONCURRENCY_MARKER}\n---\n\n# ${CONCURRENCY_MARKER}\n`,
    );
    execFileSync('bun', ['run', 'src/cli.ts', 'import', notes, '--no-embed'], {
      cwd: process.cwd(),
      env,
      stdio: 'ignore',
    });

    daemon = spawn('bun', ['run', 'src/cli.ts', 'serve', '--local-daemon'], {
      cwd: process.cwd(),
      env,
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const cfg = readPgliteConfig(home);
    await waitForCommandSocket(commandSocketPath(cfg.database_path));
  }, 60_000);

  afterAll(async () => {
    if (client1) { try { await client1.close(); } catch { /* best effort */ } }
    if (client2) { try { await client2.close(); } catch { /* best effort */ } }
    if (transport1) { try { await transport1.close(); } catch { /* best effort */ } }
    if (transport2) { try { await transport2.close(); } catch { /* best effort */ } }
    if (daemon) {
      daemon.kill('SIGTERM');
      await new Promise((resolve) => setTimeout(resolve, 1000));
      if (!daemon.killed) daemon.kill('SIGKILL');
    }
    if (home) { try { rmSync(home, { recursive: true, force: true }); } catch { /* best effort */ } }
  });

  test('two simultaneous stdio proxy clients can list tools and call search while the daemon owns PGLite', async () => {
    const cfg = readPgliteConfig(home);

    transport1 = new StdioClientTransport({
      command: 'bun',
      args: ['run', 'src/cli.ts', 'serve', '--ipc-proxy'],
      cwd: process.cwd(),
      env,
    });
    transport2 = new StdioClientTransport({
      command: 'bun',
      args: ['run', 'src/cli.ts', 'serve', '--ipc-proxy'],
      cwd: process.cwd(),
      env,
    });
    client1 = new Client({ name: 'gbrain-ipc-proxy-1', version: '1.0.0' }, { capabilities: {} });
    client2 = new Client({ name: 'gbrain-ipc-proxy-2', version: '1.0.0' }, { capabilities: {} });
    await Promise.all([client1.connect(transport1), client2.connect(transport2)]);

    const [tools1, tools2, search1, search2] = await Promise.all([
      client1.listTools(),
      client2.listTools(),
      client1.callTool({ name: 'search', arguments: { query: CONCURRENCY_MARKER, limit: 5 } }),
      client2.callTool({ name: 'search', arguments: { query: CONCURRENCY_MARKER, limit: 5 } }),
    ]);

    expect(tools1.tools.some((tool) => tool.name === 'search')).toBe(true);
    expect(tools2.tools.some((tool) => tool.name === 'search')).toBe(true);
    expect(textOf(search1)).toContain(CONCURRENCY_MARKER);
    expect(textOf(search2)).toContain(CONCURRENCY_MARKER);
  }, 60_000);
});
