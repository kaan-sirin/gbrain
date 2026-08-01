import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, readFileSync, rmSync, symlinkSync, unlinkSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import net from 'node:net';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  COMMAND_IPC_MAX_BYTES,
  CommandIpcError,
  cleanupStaleCommandSocket,
  commandSocketPath,
  commandViaIpc,
  encodeCommandIpcFrame,
  mcpProxySocketPath,
  startCommandIpcServer,
} from '../src/core/local-command-ipc.ts';

const dirs: string[] = [];
const servers: net.Server[] = [];
afterEach(async () => {
  for (const server of servers.splice(0)) await new Promise<void>(resolve => server.close(() => resolve()));
  for (const dir of dirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function dir(): string {
  const value = mkdtempSync(join(tmpdir(), 'gbrain-command-ipc-'));
  dirs.push(value);
  return value;
}

async function server(path: string, kind: 'trusted-cli' | 'mcp-proxy', fn: Parameters<typeof startCommandIpcServer>[2]) {
  const value = await startCommandIpcServer(path, kind, fn);
  expect(value).not.toBeNull();
  servers.push(value!);
  return value!;
}

function decodeFrame(frame: Buffer): unknown {
  const length = frame.readUInt32BE(0);
  expect(frame.length).toBe(4 + length);
  return JSON.parse(frame.subarray(4).toString('utf8'));
}

function raw(path: string, pieces: Buffer[], gapMs = 0): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    const socket = net.createConnection({ path, allowHalfOpen: true });
    socket.on('connect', () => {
      if (gapMs === 0) {
        for (const piece of pieces) socket.write(piece);
        return;
      }
      let index = 0;
      const writeNext = () => {
        if (index === pieces.length) return;
        socket.write(pieces[index++]!);
        setTimeout(writeNext, gapMs);
      };
      writeNext();
    });
    socket.on('data', chunk => { chunks.push(Buffer.from(chunk)); });
    socket.on('end', () => resolve(Buffer.concat(chunks)));
    socket.on('error', reject);
  });
}

describe('local command IPC', () => {
  test('endpoint-derived caller kind survives concurrent fragmented clients', async () => {
    const path = commandSocketPath(dir());
    const seen: Array<{ op: string; kind: string }> = [];
    await server(path, 'trusted-cli', async (request, kind) => {
      seen.push({ op: request.op, kind });
      return { result: { op: request.op, kind } };
    });
    const [a, b, c] = await Promise.all([
      commandViaIpc(path, { op: 'search', params: {}, cwd: process.cwd() }),
      commandViaIpc(path, { op: 'query', params: {} }),
      raw(path, (() => {
        const frame = encodeCommandIpcFrame({ op: 'get_page', params: {} });
        return [frame.subarray(0, 2), frame.subarray(2, 9), frame.subarray(9)];
      })()),
    ]);
    expect(a.result).toEqual({ op: 'search', kind: 'trusted-cli' });
    expect(b.result).toEqual({ op: 'query', kind: 'trusted-cli' });
    expect(decodeFrame(c)).toEqual({ ok: true, result: { op: 'get_page', kind: 'trusted-cli' } });
    expect(seen).toHaveLength(3);
  });

  test('a forged callerKind frame is rejected and cannot elevate a proxy endpoint', async () => {
    const path = mcpProxySocketPath(dir());
    let called = false;
    await server(path, 'mcp-proxy', async () => { called = true; return { result: 'bad' }; });
    const body = await raw(path, [encodeCommandIpcFrame({ callerKind: 'trusted-cli', op: 'submit_job', params: {} })]);
    expect(decodeFrame(body)).toMatchObject({ ok: false, kind: 'protocol', code: 'unknown_field' });
    expect(called).toBe(false);
  });

  test('operation errors, malformed replies, early close, and missing sockets stay distinguishable', async () => {
    const base = dir();
    const operationPath = commandSocketPath(base);
    await server(operationPath, 'trusted-cli', async () => { throw new Error('operation failed'); });
    await expect(commandViaIpc(operationPath, { op: 'search', params: {} })).rejects.toMatchObject({ reason: 'operation' } satisfies Partial<CommandIpcError>);
    await expect(commandViaIpc(join(base, 'missing.sock'), { op: 'search', params: {} })).rejects.toMatchObject({ reason: 'socket_missing' } satisfies Partial<CommandIpcError>);

    const earlyPath = join(base, 'early.sock');
    const early = net.createServer(conn => conn.end());
    await new Promise<void>(resolve => early.listen(earlyPath, resolve));
    servers.push(early);
    await expect(commandViaIpc(earlyPath, { op: 'search', params: {} })).rejects.toMatchObject({ reason: 'socket_unavailable', code: 'closed_early' } satisfies Partial<CommandIpcError>);
  });

  test('allows exact-size prefixed request and response frames, rejects +1 byte', async () => {
    const path = commandSocketPath(dir());
    const responseOverhead = 4 + Buffer.byteLength(JSON.stringify({ ok: true, result: { body: '' } }), 'utf8');
    const responseExact = 'r'.repeat(COMMAND_IPC_MAX_BYTES - responseOverhead);
    await server(path, 'trusted-cli', async request => {
      if (request.op === 'response-exact') return { result: { body: responseExact } };
      if (request.op === 'response-over') return { result: { body: responseExact + 'r' } };
      return { result: { accepted: true } };
    });
    const overhead = 4 + Buffer.byteLength(JSON.stringify({ op: 'search', params: { body: '' } }), 'utf8');
    const exact = 'x'.repeat(COMMAND_IPC_MAX_BYTES - overhead);
    await expect(commandViaIpc(path, { op: 'search', params: { body: exact } })).resolves.toEqual({ result: { accepted: true } });
    await expect(commandViaIpc(path, { op: 'search', params: { body: exact + 'x' } })).rejects.toMatchObject({ reason: 'payload_too_large' } satisfies Partial<CommandIpcError>);
    await expect(commandViaIpc(path, { op: 'response-exact', params: {} })).resolves.toMatchObject({ result: { body: responseExact } });
    await expect(commandViaIpc(path, { op: 'response-over', params: {} })).rejects.toMatchObject({ reason: 'payload_too_large' } satisfies Partial<CommandIpcError>);
  }, 30_000);

  test('handles fragmented headers and bodies, but rejects same-chunk second frames before dispatch', async () => {
    const path = commandSocketPath(dir());
    let calls = 0;
    await server(path, 'trusted-cli', async request => { calls++; return { result: request.op }; });
    const valid = encodeCommandIpcFrame({ op: 'search', params: {} });
    const fragmented = await raw(path, [valid.subarray(0, 1), valid.subarray(1, 4), valid.subarray(4, 8), valid.subarray(8)]);
    expect(decodeFrame(fragmented)).toEqual({ ok: true, result: 'search' });
    const sameChunk = await raw(path, [Buffer.concat([valid, valid])]);
    expect(decodeFrame(sameChunk)).toMatchObject({ ok: false, kind: 'protocol', code: 'trailing_data' });
    expect(calls).toBe(1);
  });

  test('rejects invalid and oversized declared lengths before buffering a body', async () => {
    const path = commandSocketPath(dir());
    let calls = 0;
    await server(path, 'trusted-cli', async () => { calls++; return { result: 'unexpected' }; });
    const zero = Buffer.alloc(4);
    expect(decodeFrame(await raw(path, [zero]))).toMatchObject({ ok: false, kind: 'protocol', code: 'invalid_frame_length' });
    const oversized = Buffer.alloc(4);
    oversized.writeUInt32BE(COMMAND_IPC_MAX_BYTES - 3, 0);
    expect(decodeFrame(await raw(path, [oversized]))).toMatchObject({ ok: false, kind: 'transport', code: 'payload_too_large' });
    expect(calls).toBe(0);
  });

  test('late second frame never dispatches a second operation', async () => {
    const path = commandSocketPath(dir());
    let calls = 0;
    await server(path, 'trusted-cli', async () => {
      calls++;
      await new Promise(resolve => setTimeout(resolve, 50));
      return { result: 'first' };
    });
    const frame = encodeCommandIpcFrame({ op: 'search', params: {} });
    const response = await raw(path, [frame, frame], 10);
    expect(decodeFrame(response)).toMatchObject({ ok: false, kind: 'protocol', code: 'trailing_data' });
    expect(calls).toBe(1);
  });

  test('rejects a response with trailing bytes and uses absolute, not idle, deadlines', async () => {
    const base = dir();
    const trailing = join(base, 'trailing.sock');
    const trailingServer = net.createServer({ allowHalfOpen: true }, conn => {
      conn.on('data', () => conn.end(Buffer.concat([
        encodeCommandIpcFrame({ ok: true, result: 'first' }),
        encodeCommandIpcFrame({ ok: true, result: 'second' }),
      ])));
    });
    await new Promise<void>(resolve => trailingServer.listen(trailing, resolve));
    servers.push(trailingServer);
    await expect(commandViaIpc(trailing, { op: 'search', params: {} })).rejects.toMatchObject({ reason: 'protocol', code: 'trailing_data' } satisfies Partial<CommandIpcError>);

    const drip = join(base, 'drip.sock');
    const dripServer = net.createServer({ allowHalfOpen: true }, conn => {
      const interval = setInterval(() => conn.write(Buffer.from([0])), 10);
      conn.once('close', () => clearInterval(interval));
    });
    await new Promise<void>(resolve => dripServer.listen(drip, resolve));
    servers.push(dripServer);
    await expect(commandViaIpc(drip, { op: 'search', params: {} }, 35)).rejects.toMatchObject({ reason: 'timeout' } satisfies Partial<CommandIpcError>);
  });

  test('server request deadline is absolute and request_timeout maps to timeout for clients', async () => {
    const path = commandSocketPath(dir());
    const value = await startCommandIpcServer(path, 'trusted-cli', async () => {
      await new Promise(resolve => setTimeout(resolve, 100));
      return { result: 'late' };
    }, { requestTimeoutMs: 25 });
    expect(value).not.toBeNull();
    servers.push(value!);
    await expect(commandViaIpc(path, { op: 'search', params: {} }, 500)).rejects.toMatchObject({
      reason: 'timeout', code: 'request_timeout',
    } satisfies Partial<CommandIpcError>);
  });

  test('never removes live sockets, regular files, FIFOs, or symlinks; chmod failure closes startup', async () => {
    const base = dir();
    const live = commandSocketPath(base);
    await server(live, 'trusted-cli', async () => ({ result: 'live' }));
    expect(await cleanupStaleCommandSocket(live)).toBe(false);
    expect(await startCommandIpcServer(live, 'trusted-cli', async () => ({ result: 'second' }))).toBeNull();
    await expect(commandViaIpc(live, { op: 'search', params: {} })).resolves.toEqual({ result: 'live' });

    const file = join(base, 'file.sock');
    writeFileSync(file, 'keep');
    expect(await cleanupStaleCommandSocket(file)).toBe(false);
    const link = join(base, 'link.sock');
    symlinkSync(file, link);
    expect(await cleanupStaleCommandSocket(link)).toBe(false);
    const fifo = join(base, 'fifo.sock');
    const madeFifo = spawnSync('mkfifo', [fifo]);
    expect(madeFifo.status).toBe(0);
    expect(await cleanupStaleCommandSocket(fifo)).toBe(false);

    const stale = join(base, 'stale.sock');
    const staleServer = net.createServer();
    await new Promise<void>(resolve => staleServer.listen(stale, resolve));
    servers.push(staleServer);
    expect(await cleanupStaleCommandSocket(stale, {
      probe: async () => {
        unlinkSync(stale);
        writeFileSync(stale, 'replacement');
        return 'stale';
      },
    })).toBe(false);
    expect(readFileSync(stale, 'utf8')).toBe('replacement');

    const failed = await startCommandIpcServer(join(base, 'chmod.sock'), 'trusted-cli', async () => ({ result: 'x' }), {
      chmod: () => { throw new Error('denied'); },
    });
    expect(failed).toBeNull();
  });
});
