/**
 * Local import IPC.
 *
 * PGLite has one owning process. When `gbrain serve` is alive it owns that
 * connection, so a separate `gbrain import` process must delegate the write to
 * serve instead of opening the database itself.
 */

import net from 'node:net';
import { existsSync, unlinkSync, statSync, chmodSync } from 'node:fs';
import { join } from 'node:path';
import type { RunImportResult } from '../../commands/import.ts';

const SOCK_NAME = '.gbrain-import.sock';
const CLIENT_TIMEOUT_MS = Number(process.env.GBRAIN_IMPORT_IPC_TIMEOUT_MS || 10 * 60 * 1000);
const MAX_MSG_BYTES = 1024 * 1024;

export const IMPORT_IPC_UNAVAILABLE = Symbol('import-ipc-unavailable');

export interface ImportIpcRequest {
  args: string[];
}

export interface ImportIpcResponse {
  result: RunImportResult;
}

export interface CommandIpcRequest {
  op: string;
  params: Record<string, unknown>;
}

export interface CommandIpcResponse {
  result: unknown;
}

export type ImportIpcHandler = (req: ImportIpcRequest) => Promise<ImportIpcResponse>;
export type CommandIpcHandler = (req: CommandIpcRequest) => Promise<CommandIpcResponse>;

export function importSocketPath(dataDir: string): string {
  return join(dataDir, SOCK_NAME);
}

export function commandSocketPath(dataDir: string): string {
  return join(dataDir, '.gbrain-command.sock');
}

export async function importViaIpc(
  socketPath: string,
  req: ImportIpcRequest,
): Promise<ImportIpcResponse | typeof IMPORT_IPC_UNAVAILABLE> {
  if (!existsSync(socketPath)) return IMPORT_IPC_UNAVAILABLE;
  return new Promise((resolve) => {
    let settled = false;
    let buf = '';
    const finish = (v: ImportIpcResponse | typeof IMPORT_IPC_UNAVAILABLE) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch { /* noop */ }
      resolve(v);
    };
    const sock = net.createConnection(socketPath);
    sock.setTimeout(CLIENT_TIMEOUT_MS);
    sock.on('connect', () => {
      sock.write(JSON.stringify(req) + '\n');
    });
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      if (buf.length > MAX_MSG_BYTES) return finish(IMPORT_IPC_UNAVAILABLE);
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        const resp = JSON.parse(buf.slice(0, nl));
        if (resp && resp.ok && resp.result) return finish({ result: resp.result });
        return finish(IMPORT_IPC_UNAVAILABLE);
      } catch {
        return finish(IMPORT_IPC_UNAVAILABLE);
      }
    });
    sock.on('timeout', () => finish(IMPORT_IPC_UNAVAILABLE));
    sock.on('error', () => finish(IMPORT_IPC_UNAVAILABLE));
    sock.on('close', () => finish(IMPORT_IPC_UNAVAILABLE));
  });
}

export async function commandViaIpc(
  socketPath: string,
  req: CommandIpcRequest,
): Promise<CommandIpcResponse | typeof IMPORT_IPC_UNAVAILABLE> {
  if (!existsSync(socketPath)) return IMPORT_IPC_UNAVAILABLE;
  return new Promise((resolve) => {
    let settled = false;
    let buf = '';
    const finish = (v: CommandIpcResponse | typeof IMPORT_IPC_UNAVAILABLE) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch { /* noop */ }
      resolve(v);
    };
    const sock = net.createConnection(socketPath);
    sock.setTimeout(CLIENT_TIMEOUT_MS);
    sock.on('connect', () => {
      sock.write(JSON.stringify(req) + '\n');
    });
    sock.on('data', (chunk) => {
      buf += chunk.toString('utf8');
      if (buf.length > MAX_MSG_BYTES) return finish(IMPORT_IPC_UNAVAILABLE);
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        const resp = JSON.parse(buf.slice(0, nl));
        if (resp && resp.ok) return finish({ result: resp.result });
        return finish(IMPORT_IPC_UNAVAILABLE);
      } catch {
        return finish(IMPORT_IPC_UNAVAILABLE);
      }
    });
    sock.on('timeout', () => finish(IMPORT_IPC_UNAVAILABLE));
    sock.on('error', () => finish(IMPORT_IPC_UNAVAILABLE));
    sock.on('close', () => finish(IMPORT_IPC_UNAVAILABLE));
  });
}

export async function startImportIpcServer(
  socketPath: string,
  handler: ImportIpcHandler,
): Promise<net.Server | null> {
  cleanupStaleImportSocket(socketPath);

  return new Promise((resolve) => {
    const server = net.createServer((conn) => {
      let buf = '';
      conn.setEncoding('utf8');
      conn.on('data', async (chunk: string) => {
        buf += chunk;
        if (buf.length > MAX_MSG_BYTES) { conn.destroy(); return; }
        const nl = buf.indexOf('\n');
        if (nl < 0) return;
        const line = buf.slice(0, nl);
        let resp: string;
        try {
          const req = JSON.parse(line) as ImportIpcRequest;
          const result = await handler(req);
          resp = JSON.stringify({ ok: true, result: result.result });
        } catch (e) {
          resp = JSON.stringify({ ok: false, error: e instanceof Error ? e.message : String(e) });
        }
        try { conn.write(resp + '\n'); } catch { /* client gone */ }
        conn.end();
      });
      conn.on('error', () => { try { conn.destroy(); } catch { /* noop */ } });
    });
    server.on('error', () => resolve(null));
    server.listen(socketPath, () => {
      try { chmodSync(socketPath, 0o600); } catch { /* best effort */ }
      resolve(server);
    });
  });
}

export async function startCommandIpcServer(
  socketPath: string,
  handler: CommandIpcHandler,
): Promise<net.Server | null> {
  cleanupStaleImportSocket(socketPath);

  return new Promise((resolve) => {
    const server = net.createServer((conn) => {
      let buf = '';
      conn.setEncoding('utf8');
      conn.on('data', async (chunk: string) => {
        buf += chunk;
        if (buf.length > MAX_MSG_BYTES) { conn.destroy(); return; }
        const nl = buf.indexOf('\n');
        if (nl < 0) return;
        const line = buf.slice(0, nl);
        let resp: string;
        try {
          const req = JSON.parse(line) as CommandIpcRequest;
          const result = await handler(req);
          resp = JSON.stringify({ ok: true, result: result.result });
        } catch (e) {
          resp = JSON.stringify({ ok: false, error: e instanceof Error ? e.message : String(e) });
        }
        try { conn.write(resp + '\n'); } catch { /* client gone */ }
        conn.end();
      });
      conn.on('error', () => { try { conn.destroy(); } catch { /* noop */ } });
    });
    server.on('error', () => resolve(null));
    server.listen(socketPath, () => {
      try { chmodSync(socketPath, 0o600); } catch { /* best effort */ }
      resolve(server);
    });
  });
}

export function cleanupStaleImportSocket(socketPath: string): void {
  try {
    if (existsSync(socketPath)) {
      const st = statSync(socketPath);
      if (st.isSocket() || st.isFIFO() || st.isFile()) unlinkSync(socketPath);
    }
  } catch {
    /* best effort */
  }
}
