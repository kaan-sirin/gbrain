import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  CommandIpcError,
  commandSocketPath,
  commandViaIpc,
  importSocketPath,
  importViaIpc,
  IMPORT_IPC_UNAVAILABLE,
  startCommandIpcServer,
  startImportIpcServer,
} from '../../src/core/context/import-ipc.ts';

const dirs: string[] = [];

function tempDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'gbrain-import-ipc-'));
  dirs.push(dir);
  return dir;
}

afterEach(() => {
  while (dirs.length) {
    rmSync(dirs.pop()!, { recursive: true, force: true });
  }
});

describe('import IPC', () => {
  test('absent socket is unavailable', async () => {
    const dir = tempDir();
    const got = await importViaIpc(importSocketPath(dir), { args: ['wiki'] });
    expect(got).toBe(IMPORT_IPC_UNAVAILABLE);
  });

  test('delegates import args to the server owner', async () => {
    const dir = tempDir();
    const sock = importSocketPath(dir);
    const seen: string[][] = [];
    const server = await startImportIpcServer(sock, async (req) => {
      seen.push(req.args);
      return {
        result: {
          imported: 2,
          skipped: 1,
          errors: 0,
          chunksCreated: 4,
          failures: [],
        },
      };
    });
    expect(server).toBeTruthy();
    try {
      const got = await importViaIpc(sock, { args: ['wiki', '--no-embed'] });
      expect(got).not.toBe(IMPORT_IPC_UNAVAILABLE);
      if (got !== IMPORT_IPC_UNAVAILABLE) {
        expect(got.result.imported).toBe(2);
        expect(got.result.chunksCreated).toBe(4);
      }
      expect(seen).toEqual([['wiki', '--no-embed']]);
    } finally {
      server?.close();
    }
  });

  test('delegates command params to the server owner', async () => {
    const dir = tempDir();
    const sock = commandSocketPath(dir);
    const seen: Array<{ callerKind?: string; op: string; params: Record<string, unknown> }> = [];
    const server = await startCommandIpcServer(sock, async (req) => {
      seen.push({ callerKind: req.callerKind, op: req.op, params: req.params });
      return {
        result: [
          { slug: 'concepts/example', score: 0.9, chunk_text: 'example result' },
        ],
      };
    });
    expect(server).toBeTruthy();
    try {
      const got = await commandViaIpc(sock, {
        callerKind: 'trusted-cli',
        op: 'search',
        params: { query: 'example' },
      });
      expect(got.result).toEqual([
        { slug: 'concepts/example', score: 0.9, chunk_text: 'example result' },
      ]);
      expect(seen).toEqual([{
        callerKind: 'trusted-cli',
        op: 'search',
        params: { query: 'example' },
      }]);
    } finally {
      server?.close();
    }
  });

  test('missing command socket is a typed socket_missing error', async () => {
    const dir = tempDir();
    await expect(commandViaIpc(commandSocketPath(dir), {
      callerKind: 'trusted-cli',
      op: 'search',
      params: { query: 'example' },
    })).rejects.toMatchObject({
      name: 'CommandIpcError',
      reason: 'socket_missing',
    } satisfies Partial<CommandIpcError>);
  });

  test('daemon operation errors stay operation errors', async () => {
    const dir = tempDir();
    const sock = commandSocketPath(dir);
    const server = await startCommandIpcServer(sock, async () => {
      throw new Error('search exploded');
    });
    expect(server).toBeTruthy();
    try {
      await expect(commandViaIpc(sock, {
        callerKind: 'trusted-cli',
        op: 'search',
        params: { query: 'example' },
      })).rejects.toMatchObject({
        name: 'CommandIpcError',
        reason: 'operation',
        message: 'search exploded',
      } satisfies Partial<CommandIpcError>);
    } finally {
      server?.close();
    }
  });

  test('command IPC accepts payloads above 1 MiB up to the new cap', async () => {
    const dir = tempDir();
    const sock = commandSocketPath(dir);
    const big = 'x'.repeat(2 * 1024 * 1024);
    const server = await startCommandIpcServer(sock, async () => ({
      result: { blob: big },
    }));
    expect(server).toBeTruthy();
    try {
      const got = await commandViaIpc(sock, {
        callerKind: 'trusted-cli',
        op: 'search',
        params: { query: 'big' },
      });
      expect(got.result).toEqual({ blob: big });
    } finally {
      server?.close();
    }
  });

  test('command IPC rejects payloads over the capped size safely', async () => {
    const dir = tempDir();
    const sock = commandSocketPath(dir);
    const tooBig = 'x'.repeat(9 * 1024 * 1024);
    const server = await startCommandIpcServer(sock, async () => ({
      result: { blob: tooBig },
    }));
    expect(server).toBeTruthy();
    try {
      await expect(commandViaIpc(sock, {
        callerKind: 'trusted-cli',
        op: 'search',
        params: { query: 'too-big' },
      })).rejects.toMatchObject({
        name: 'CommandIpcError',
        reason: 'payload_too_large',
      } satisfies Partial<CommandIpcError>);
    } finally {
      server?.close();
    }
  });
});
