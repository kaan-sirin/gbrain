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
const COMMAND_SOCKET_NAME = '.gbrain-command.sock';
// MCP tool-call responses can legitimately exceed 1 MiB once result JSON and
// _meta are serialized. Keep the command channel bounded, but large enough for
// real stdio MCP payloads. 8 MiB covers multi-tool search/query responses while
// still failing closed on runaway payloads.
const COMMAND_MAX_MSG_BYTES = Number(process.env.GBRAIN_COMMAND_IPC_MAX_MSG_BYTES || 8 * 1024 * 1024);
const COMMAND_PROBE_TIMEOUT_MS = Number(process.env.GBRAIN_COMMAND_IPC_PROBE_TIMEOUT_MS || 1_500);

export const IMPORT_IPC_UNAVAILABLE = Symbol('import-ipc-unavailable');

export interface ImportIpcRequest {
  args: string[];
}

export interface ImportIpcResponse {
  result: RunImportResult;
}

export type CommandIpcCallerKind = 'trusted-cli' | 'mcp-proxy';

export interface CommandIpcRequest {
  callerKind?: CommandIpcCallerKind;
  op: string;
  params: Record<string, unknown>;
}

export interface CommandIpcResponse {
  result: unknown;
}

type CommandIpcWireErrorKind = 'operation' | 'protocol' | 'transport';

interface CommandIpcSuccessEnvelope {
  ok: true;
  result: unknown;
}

interface CommandIpcFailureEnvelope {
  ok: false;
  kind: CommandIpcWireErrorKind;
  error: string;
  code?: string;
}

type CommandIpcEnvelope = CommandIpcSuccessEnvelope | CommandIpcFailureEnvelope;

export type CommandIpcErrorReason =
  | 'socket_missing'
  | 'socket_unavailable'
  | 'timeout'
  | 'payload_too_large'
  | 'operation'
  | 'protocol';

export class CommandIpcError extends Error {
  readonly reason: CommandIpcErrorReason;
  readonly code?: string;
  readonly socketPath: string;

  constructor(
    reason: CommandIpcErrorReason,
    message: string,
    opts: { socketPath: string; code?: string },
  ) {
    super(message);
    this.name = 'CommandIpcError';
    this.reason = reason;
    this.code = opts.code;
    this.socketPath = opts.socketPath;
  }
}

export type ImportIpcHandler = (req: ImportIpcRequest) => Promise<ImportIpcResponse>;
export type CommandIpcHandler = (req: CommandIpcRequest) => Promise<CommandIpcResponse>;

export function importSocketPath(dataDir: string): string {
  return join(dataDir, SOCK_NAME);
}

export function commandSocketPath(dataDir: string): string {
  return join(dataDir, COMMAND_SOCKET_NAME);
}

function byteLen(text: string): number {
  return Buffer.byteLength(text, 'utf8');
}

function buildCommandPayloadTooLargeEnvelope(limitBytes: number): CommandIpcFailureEnvelope {
  return {
    ok: false,
    kind: 'transport',
    code: 'payload_too_large',
    error: `Command IPC payload exceeded the ${limitBytes}-byte cap.`,
  };
}

function toCommandFailureEnvelope(err: unknown): CommandIpcFailureEnvelope {
  if (err instanceof CommandIpcError) {
    const kind: CommandIpcWireErrorKind =
      err.reason === 'operation'
        ? 'operation'
        : err.reason === 'protocol'
          ? 'protocol'
          : 'transport';
    return {
      ok: false,
      kind,
      code: err.code,
      error: err.message,
    };
  }

  return {
    ok: false,
    kind: 'operation',
    error: err instanceof Error ? err.message : String(err),
  };
}

function renderConnectError(socketPath: string, err: NodeJS.ErrnoException): CommandIpcError {
  const code = typeof err.code === 'string' ? err.code : undefined;
  const detail = code ? ` (${code})` : '';
  return new CommandIpcError(
    'socket_unavailable',
    `Command IPC socket unavailable at ${socketPath}${detail}.`,
    { socketPath, code },
  );
}

function parseCommandEnvelope(socketPath: string, raw: string): CommandIpcResponse {
  let envelope: CommandIpcEnvelope;
  try {
    envelope = JSON.parse(raw) as CommandIpcEnvelope;
  } catch {
    throw new CommandIpcError(
      'protocol',
      `Invalid JSON from command IPC socket ${socketPath}.`,
      { socketPath, code: 'invalid_json' },
    );
  }

  if (envelope && envelope.ok === true) {
    return { result: envelope.result };
  }

  if (envelope && envelope.ok === false) {
    const reason: CommandIpcErrorReason =
      envelope.kind === 'operation'
        ? 'operation'
        : envelope.code === 'payload_too_large'
          ? 'payload_too_large'
          : envelope.kind === 'protocol'
            ? 'protocol'
            : 'socket_unavailable';
    throw new CommandIpcError(
      reason,
      envelope.error,
      { socketPath, ...(envelope.code ? { code: envelope.code } : {}) },
    );
  }

  throw new CommandIpcError(
    'protocol',
    `Malformed command IPC envelope from ${socketPath}.`,
    { socketPath, code: 'invalid_envelope' },
  );
}

export function isCommandIpcUnavailableError(err: unknown): err is CommandIpcError {
  return err instanceof CommandIpcError &&
    (err.reason === 'socket_missing' || err.reason === 'socket_unavailable' || err.reason === 'timeout');
}

export async function probeCommandIpc(socketPath: string): Promise<void> {
  if (!existsSync(socketPath)) {
    throw new CommandIpcError(
      'socket_missing',
      `Command IPC socket not found at ${socketPath}.`,
      { socketPath, code: 'socket_missing' },
    );
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (err?: Error) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch { /* noop */ }
      if (err) reject(err);
      else resolve();
    };

    const sock = net.createConnection(socketPath);
    sock.setTimeout(COMMAND_PROBE_TIMEOUT_MS);
    sock.on('connect', () => finish());
    sock.on('timeout', () => finish(new CommandIpcError(
      'timeout',
      `Timed out probing command IPC socket ${socketPath}.`,
      { socketPath, code: 'timeout' },
    )));
    sock.on('error', (err: NodeJS.ErrnoException) => finish(renderConnectError(socketPath, err)));
  });
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
): Promise<CommandIpcResponse> {
  if (!existsSync(socketPath)) {
    throw new CommandIpcError(
      'socket_missing',
      `Command IPC socket not found at ${socketPath}.`,
      { socketPath, code: 'socket_missing' },
    );
  }

  const payload = JSON.stringify(req) + '\n';
  if (byteLen(payload) > COMMAND_MAX_MSG_BYTES) {
    throw new CommandIpcError(
      'payload_too_large',
      `Command IPC request exceeded the ${COMMAND_MAX_MSG_BYTES}-byte cap.`,
      { socketPath, code: 'payload_too_large' },
    );
  }

  return new Promise((resolve, reject) => {
    let settled = false;
    let buf = '';
    let bufBytes = 0;
    const finish = (err?: Error, v?: CommandIpcResponse) => {
      if (settled) return;
      settled = true;
      try { sock.destroy(); } catch { /* noop */ }
      if (err) reject(err);
      else resolve(v as CommandIpcResponse);
    };
    const sock = net.createConnection(socketPath);
    sock.setTimeout(CLIENT_TIMEOUT_MS);
    sock.on('connect', () => {
      sock.write(payload);
    });
    sock.on('data', (chunk) => {
      const text = chunk.toString('utf8');
      buf += text;
      bufBytes += byteLen(text);
      if (bufBytes > COMMAND_MAX_MSG_BYTES) {
        return finish(new CommandIpcError(
          'payload_too_large',
          `Command IPC response exceeded the ${COMMAND_MAX_MSG_BYTES}-byte cap.`,
          { socketPath, code: 'payload_too_large' },
        ));
      }
      const nl = buf.indexOf('\n');
      if (nl < 0) return;
      try {
        return finish(undefined, parseCommandEnvelope(socketPath, buf.slice(0, nl)));
      } catch (err) {
        return finish(err as Error);
      }
    });
    sock.on('timeout', () => finish(new CommandIpcError(
      'timeout',
      `Timed out waiting for command IPC response from ${socketPath}.`,
      { socketPath, code: 'timeout' },
    )));
    sock.on('error', (err: NodeJS.ErrnoException) => finish(renderConnectError(socketPath, err)));
    sock.on('close', () => {
      if (!settled) {
        finish(new CommandIpcError(
          'socket_unavailable',
          `Command IPC connection closed before a response arrived from ${socketPath}.`,
          { socketPath, code: 'closed_early' },
        ));
      }
    });
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
      let bufBytes = 0;
      conn.setEncoding('utf8');
      conn.on('data', async (chunk: string) => {
        buf += chunk;
        bufBytes += byteLen(chunk);
        if (bufBytes > COMMAND_MAX_MSG_BYTES) {
          try {
            conn.write(JSON.stringify(buildCommandPayloadTooLargeEnvelope(COMMAND_MAX_MSG_BYTES)) + '\n');
          } catch { /* client gone */ }
          conn.end();
          return;
        }
        const nl = buf.indexOf('\n');
        if (nl < 0) return;
        const line = buf.slice(0, nl);
        let resp: string;
        try {
          const req = JSON.parse(line) as CommandIpcRequest;
          const result = await handler(req);
          resp = JSON.stringify({ ok: true, result: result.result } satisfies CommandIpcSuccessEnvelope);
        } catch (e) {
          resp = JSON.stringify(toCommandFailureEnvelope(e));
        }
        if (byteLen(resp) > COMMAND_MAX_MSG_BYTES) {
          resp = JSON.stringify(buildCommandPayloadTooLargeEnvelope(COMMAND_MAX_MSG_BYTES));
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
