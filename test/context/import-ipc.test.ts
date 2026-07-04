import { afterEach, describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  commandSocketPath,
  commandViaIpc,
  IMPORT_IPC_UNAVAILABLE,
  importSocketPath,
  importViaIpc,
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
    const seen: Array<{ op: string; params: Record<string, unknown> }> = [];
    const server = await startCommandIpcServer(sock, async (req) => {
      seen.push({ op: req.op, params: req.params });
      return {
        result: [
          { slug: 'concepts/example', score: 0.9, chunk_text: 'example result' },
        ],
      };
    });
    expect(server).toBeTruthy();
    try {
      const got = await commandViaIpc(sock, { op: 'search', params: { query: 'example' } });
      expect(got).not.toBe(IMPORT_IPC_UNAVAILABLE);
      if (got !== IMPORT_IPC_UNAVAILABLE) {
        expect(got.result).toEqual([
          { slug: 'concepts/example', score: 0.9, chunk_text: 'example result' },
        ]);
      }
      expect(seen).toEqual([{ op: 'search', params: { query: 'example' } }]);
    } finally {
      server?.close();
    }
  });
});
