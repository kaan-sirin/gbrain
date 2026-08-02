/** Local, same-UID command IPC for a PGLite-owning serve process. */
import net from 'node:net';
import { chmodSync, existsSync, lstatSync, unlinkSync, type Stats } from 'node:fs';
import { join } from 'node:path';
import { OperationError } from './operations.ts';

const COMMAND_SOCKET_NAME = '.gbrain-command.sock';
const MCP_PROXY_SOCKET_NAME = '.gbrain-mcp-proxy.sock';
const CLIENT_TIMEOUT_MS = 30_000;
const PROBE_TIMEOUT_MS = 250;
const HANDLER_TIMEOUT_MS = 30_000;
export const LOCAL_CLI_IMPORT_OP = '__gbrain_cli_import';
export const LOCAL_CLI_IMPORT_TIMEOUT_MS = 10 * 60_000;
const FRAME_PREFIX_BYTES = 4;
export const COMMAND_IPC_MAX_BYTES = 8 * 1024 * 1024;
const COMMAND_IPC_MAX_PAYLOAD_BYTES = COMMAND_IPC_MAX_BYTES - FRAME_PREFIX_BYTES;

export type CommandIpcCallerKind = 'trusted-cli' | 'mcp-proxy';

/** Deliberately has no caller identity: listener selection supplies it. */
export interface CommandIpcRequest {
  op: string;
  params: Record<string, unknown>;
  cwd?: string;
}

export interface CommandIpcResponse { result: unknown; }
type FailureKind = 'operation' | 'protocol' | 'transport';
type WireEnvelope =
  | { ok: true; result: unknown }
  | { ok: false; kind: FailureKind; error: string; code?: string };

export type CommandIpcErrorReason =
  | 'socket_missing' | 'socket_unavailable' | 'timeout' | 'payload_too_large' | 'operation' | 'protocol';

export class CommandIpcError extends Error {
  constructor(
    readonly reason: CommandIpcErrorReason,
    message: string,
    readonly opts: { socketPath: string; code?: string },
  ) {
    super(message);
    this.name = 'CommandIpcError';
  }
  get code(): string | undefined { return this.opts.code; }
  get socketPath(): string { return this.opts.socketPath; }
}

export type CommandIpcHandler = (request: CommandIpcRequest, callerKind: CommandIpcCallerKind) => Promise<CommandIpcResponse>;

export function commandSocketPath(dataDir: string): string { return join(dataDir, COMMAND_SOCKET_NAME); }
export function mcpProxySocketPath(dataDir: string): string { return join(dataDir, MCP_PROXY_SOCKET_NAME); }

/** 4-byte big-endian UTF-8 payload length followed by exactly that JSON payload. */
export function encodeCommandIpcFrame(value: unknown): Buffer {
  const body = Buffer.from(JSON.stringify(value), 'utf8');
  if (body.length > COMMAND_IPC_MAX_PAYLOAD_BYTES) throw new RangeError('Command IPC frame exceeds the maximum payload size.');
  const frame = Buffer.allocUnsafe(FRAME_PREFIX_BYTES + body.length);
  frame.writeUInt32BE(body.length, 0);
  body.copy(frame, FRAME_PREFIX_BYTES);
  return frame;
}

class LengthPrefixedFrameReader {
  private header = Buffer.allocUnsafe(FRAME_PREFIX_BYTES);
  private headerBytes = 0;
  private body: Buffer | null = null;
  private bodyBytes = 0;
  private complete = false;

  constructor(private readonly socketPath: string) {}

  push(chunk: Buffer): Buffer | null {
    if (this.complete) throw protocolError(this.socketPath, 'trailing_data', 'Command IPC connection contained a second frame.');
    let offset = 0;
    if (this.headerBytes < FRAME_PREFIX_BYTES) {
      const copied = Math.min(FRAME_PREFIX_BYTES - this.headerBytes, chunk.length);
      chunk.copy(this.header, this.headerBytes, offset, offset + copied);
      this.headerBytes += copied;
      offset += copied;
      if (this.headerBytes < FRAME_PREFIX_BYTES) return null;
      const declared = this.header.readUInt32BE(0);
      if (declared === 0) throw protocolError(this.socketPath, 'invalid_frame_length', 'Command IPC frame payload length must be greater than zero.');
      if (declared > COMMAND_IPC_MAX_PAYLOAD_BYTES) throw tooLarge(this.socketPath);
      this.body = Buffer.allocUnsafe(declared);
    }
    const body = this.body!;
    const remaining = body.length - this.bodyBytes;
    const copied = Math.min(remaining, chunk.length - offset);
    if (copied > 0) {
      chunk.copy(body, this.bodyBytes, offset, offset + copied);
      this.bodyBytes += copied;
      offset += copied;
    }
    if (offset !== chunk.length) throw protocolError(this.socketPath, 'trailing_data', 'Command IPC connection contained trailing bytes after its frame.');
    if (this.bodyBytes !== body.length) return null;
    this.complete = true;
    return body;
  }
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}
function tooLarge(socketPath: string): CommandIpcError {
  return new CommandIpcError('payload_too_large', `Command IPC payload exceeded the ${COMMAND_IPC_MAX_BYTES}-byte cap.`, {
    socketPath, code: 'payload_too_large',
  });
}
function protocolError(socketPath: string, code: string, message: string): CommandIpcError {
  return new CommandIpcError('protocol', message, { socketPath, code });
}

function validateRequest(socketPath: string, value: unknown): CommandIpcRequest {
  if (!isPlainRecord(value)) throw protocolError(socketPath, 'invalid_request', 'Command IPC request must be an object.');
  for (const key of Object.keys(value)) {
    if (key !== 'op' && key !== 'params' && key !== 'cwd') {
      throw protocolError(socketPath, 'unknown_field', `Command IPC request contains unsupported field: ${key}.`);
    }
  }
  if (typeof value.op !== 'string' || value.op.length === 0) {
    throw protocolError(socketPath, 'invalid_op', 'Command IPC request op must be a non-empty string.');
  }
  if (!isPlainRecord(value.params)) {
    throw protocolError(socketPath, 'invalid_params', 'Command IPC request params must be an object.');
  }
  if (value.cwd !== undefined && typeof value.cwd !== 'string') {
    throw protocolError(socketPath, 'invalid_cwd', 'Command IPC request cwd must be a string.');
  }
  return { op: value.op, params: value.params, ...(typeof value.cwd === 'string' ? { cwd: value.cwd } : {}) };
}

function failureEnvelope(error: unknown): WireEnvelope {
  if (error instanceof CommandIpcError) {
    const kind: FailureKind = error.reason === 'operation' ? 'operation' : error.reason === 'protocol' ? 'protocol' : 'transport';
    return { ok: false, kind, error: error.message, ...(error.code ? { code: error.code } : {}) };
  }
  if (error instanceof OperationError) {
    return { ok: false, kind: 'operation', error: error.message, code: error.code };
  }
  return { ok: false, kind: 'operation', error: error instanceof Error ? error.message : String(error) };
}

function connectError(socketPath: string, error: NodeJS.ErrnoException): CommandIpcError {
  const code = typeof error.code === 'string' ? error.code : undefined;
  return new CommandIpcError('socket_unavailable', `Command IPC socket unavailable at ${socketPath}${code ? ` (${code})` : ''}.`, { socketPath, code });
}

function parseEnvelope(socketPath: string, raw: string): CommandIpcResponse {
  let envelope: WireEnvelope;
  try { envelope = JSON.parse(raw) as WireEnvelope; } catch {
    throw protocolError(socketPath, 'invalid_json', `Invalid JSON from command IPC socket ${socketPath}.`);
  }
  if (envelope?.ok === true) return { result: envelope.result };
  if (envelope?.ok === false) {
    const reason: CommandIpcErrorReason = envelope.kind === 'operation'
      ? 'operation'
      : envelope.kind === 'protocol'
        ? 'protocol'
        : envelope.code === 'payload_too_large'
          ? 'payload_too_large'
        : envelope.code === 'handler_timeout' || envelope.code === 'request_timeout'
            ? 'timeout'
            : 'socket_unavailable';
    throw new CommandIpcError(reason, envelope.error, { socketPath, ...(envelope.code ? { code: envelope.code } : {}) });
  }
  throw protocolError(socketPath, 'invalid_envelope', `Malformed command IPC envelope from ${socketPath}.`);
}

export function isCommandIpcUnavailableError(error: unknown): error is CommandIpcError {
  return error instanceof CommandIpcError &&
    (error.reason === 'socket_missing' || error.reason === 'socket_unavailable' || error.reason === 'timeout');
}

type SocketProbe = 'live' | 'stale' | 'indeterminate';

async function probeSocket(socketPath: string, timeoutMs: number): Promise<SocketProbe> {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (result: SocketProbe) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch { /* best effort */ }
      resolve(result);
    };
    const socket = net.createConnection({ path: socketPath, allowHalfOpen: true });
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => finish('live'));
    socket.once('timeout', () => finish('indeterminate'));
    socket.once('error', (error: NodeJS.ErrnoException) => {
      // ECONNREFUSED is the kernel's stale Unix-socket signal. Other errors
      // (permission, resource pressure, platform oddities) must preserve the
      // node rather than risking removal of a live listener.
      finish(error.code === 'ECONNREFUSED' || error.code === 'ENOENT' ? 'stale' : 'indeterminate');
    });
  });
}

/** Remove only a dead Unix-domain socket. Regular files, FIFOs, and symlinks are never touched. */
export interface CleanupStaleCommandSocketOpts {
  /** Test seams for the lstat → probe → unlink replacement race. */
  lstat?: (path: string) => Stats;
  probe?: (path: string, timeoutMs: number) => Promise<SocketProbe>;
  unlink?: (path: string) => void;
}

function sameSocketNode(before: Stats, after: Stats): boolean {
  return after.isSocket() && before.dev === after.dev && before.ino === after.ino && before.mode === after.mode;
}

export async function cleanupStaleCommandSocket(
  socketPath: string,
  opts: CleanupStaleCommandSocketOpts = {},
): Promise<boolean> {
  const lstat = opts.lstat ?? lstatSync;
  let stat: Stats;
  try { stat = lstat(socketPath); } catch (error: unknown) {
    return (error as NodeJS.ErrnoException).code === 'ENOENT';
  }
  if (!stat.isSocket()) return false;
  if (await (opts.probe ?? probeSocket)(socketPath, PROBE_TIMEOUT_MS) !== 'stale') return false;
  // The pathname may have been replaced while probeSocket was in flight.
  // Never unlink a node unless it is the exact stale socket we observed.
  let current: Stats;
  try { current = lstat(socketPath); } catch { return false; }
  if (!sameSocketNode(stat, current)) return false;
  try { (opts.unlink ?? unlinkSync)(socketPath); return true; } catch { return false; }
}

export async function probeCommandIpc(socketPath: string, timeoutMs = 1_500): Promise<void> {
  if (!existsSync(socketPath)) {
    throw new CommandIpcError('socket_missing', `Command IPC socket not found at ${socketPath}.`, { socketPath, code: 'socket_missing' });
  }
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const finish = (error?: Error) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch { /* best effort */ }
      if (error) reject(error); else resolve();
    };
    const socket = net.createConnection({ path: socketPath, allowHalfOpen: true });
    socket.setTimeout(timeoutMs);
    socket.once('connect', () => finish());
    socket.once('timeout', () => finish(new CommandIpcError('timeout', `Timed out probing command IPC socket ${socketPath}.`, { socketPath, code: 'timeout' })));
    socket.once('error', (error: NodeJS.ErrnoException) => finish(connectError(socketPath, error)));
  });
}

export async function commandViaIpc(
  socketPath: string,
  request: CommandIpcRequest,
  timeoutMs = CLIENT_TIMEOUT_MS,
): Promise<CommandIpcResponse> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) timeoutMs = CLIENT_TIMEOUT_MS;
  if (!existsSync(socketPath)) throw new CommandIpcError('socket_missing', `Command IPC socket not found at ${socketPath}.`, { socketPath, code: 'socket_missing' });
  let payload: Buffer;
  try { payload = encodeCommandIpcFrame(request); }
  catch { throw tooLarge(socketPath); }
  return new Promise((resolve, reject) => {
    let settled = false;
    const reader = new LengthPrefixedFrameReader(socketPath);
    let response: Buffer | null = null;
    const finish = (error?: Error, response?: CommandIpcResponse) => {
      if (settled) return;
      settled = true;
      try { socket.destroy(); } catch { /* best effort */ }
      if (error) reject(error); else resolve(response as CommandIpcResponse);
    };
    const socket = net.createConnection({ path: socketPath, allowHalfOpen: true });
    // This is an absolute response deadline, not a socket-idle timeout: a
    // peer that drips bytes forever must not keep the caller alive forever.
    const deadline = setTimeout(() => finish(new CommandIpcError('timeout', `Timed out waiting for command IPC response from ${socketPath}.`, { socketPath, code: 'timeout' })), timeoutMs);
    deadline.unref?.();
    const finishWithDeadline = (error?: Error, response?: CommandIpcResponse) => {
      clearTimeout(deadline);
      finish(error, response);
    };
    socket.once('connect', () => socket.write(payload));
    socket.on('data', (chunk) => {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      try {
        const complete = reader.push(bytes);
        if (complete) response = complete;
      } catch (error) {
        finishWithDeadline(error as Error);
      }
    });
    socket.once('end', () => {
      if (settled) return;
      if (!response) {
        return finishWithDeadline(new CommandIpcError('socket_unavailable', `Command IPC connection closed before a response arrived from ${socketPath}.`, { socketPath, code: 'closed_early' }));
      }
      try { finishWithDeadline(undefined, parseEnvelope(socketPath, response.toString('utf8'))); }
      catch (error) { finishWithDeadline(error as Error); }
    });
    socket.once('error', (error: NodeJS.ErrnoException) => finishWithDeadline(connectError(socketPath, error)));
    socket.once('close', () => {
      if (!settled) finishWithDeadline(new CommandIpcError('socket_unavailable', `Command IPC connection closed before a response arrived from ${socketPath}.`, { socketPath, code: 'closed_early' }));
    });
  });
}

function withHandlerDeadline<T>(work: Promise<T>, socketPath: string, timeoutMs: number): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new CommandIpcError(
      'timeout', `Command IPC handler exceeded its ${HANDLER_TIMEOUT_MS}ms deadline.`, { socketPath, code: 'handler_timeout' },
    )), timeoutMs);
    timeout.unref?.();
    work.then(resolve, reject).finally(() => clearTimeout(timeout));
  });
}

export async function startCommandIpcServer(
  socketPath: string,
  callerKind: CommandIpcCallerKind,
  handler: CommandIpcHandler,
  opts: { chmod?: (path: string, mode: number) => void; requestTimeoutMs?: number } = {},
): Promise<net.Server | null> {
  if (!await cleanupStaleCommandSocket(socketPath)) return null;
  return new Promise((resolve) => {
    let resolved = false;
    const finish = (server: net.Server | null) => { if (!resolved) { resolved = true; resolve(server); } };
    const server = net.createServer({ allowHalfOpen: true }, (connection) => {
      const reader = new LengthPrefixedFrameReader(socketPath);
      let finished = false;
      let dispatched = false;
      const startedAt = Date.now();
      const requestTimeoutMs = opts.requestTimeoutMs ?? HANDLER_TIMEOUT_MS;
      const send = (envelope: WireEnvelope) => {
        if (connection.destroyed) return;
        let payload: Buffer;
        try { payload = encodeCommandIpcFrame(envelope); }
        catch { payload = encodeCommandIpcFrame(failureEnvelope(tooLarge(socketPath))); }
        connection.end(payload);
      };
      const finish = (envelope: WireEnvelope) => {
        if (finished) return;
        finished = true;
        clearTimeout(deadline);
        send(envelope);
      };
      // Deliberately starts at accept, and is never reset by data events.
      const deadline = setTimeout(() => {
        finish(failureEnvelope(new CommandIpcError('timeout', 'Command IPC request exceeded its absolute deadline.', { socketPath, code: 'request_timeout' })));
      }, requestTimeoutMs);
      deadline.unref?.();
      const dispatch = async (body: Buffer) => {
        try {
          const request = validateRequest(socketPath, JSON.parse(body.toString('utf8')));
          const remainingMs = requestTimeoutMs - (Date.now() - startedAt);
          if (remainingMs <= 0) throw new CommandIpcError('timeout', 'Command IPC request exceeded its absolute deadline.', { socketPath, code: 'request_timeout' });
          const result = await withHandlerDeadline(handler(request, callerKind), socketPath, remainingMs);
          finish({ ok: true, result: result.result });
        } catch (error) {
          finish(failureEnvelope(error));
        }
      };
      connection.on('data', (chunk) => {
        if (finished) return;
        if (dispatched) {
          // A complete first frame may already be executing, but this endpoint
          // never dispatches another frame on the same connection.
          finish(failureEnvelope(protocolError(socketPath, 'trailing_data', 'Command IPC request contained trailing data after its frame.')));
          return;
        }
        const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        try {
          const body = reader.push(bytes);
          if (!body) return;
          dispatched = true;
          void dispatch(body);
        } catch (error) {
          finish(failureEnvelope(error));
        }
      });
      connection.on('error', () => { clearTimeout(deadline); try { connection.destroy(); } catch { /* best effort */ } });
    });
    server.once('error', () => finish(null));
    server.listen(socketPath, () => {
      try {
        (opts.chmod ?? chmodSync)(socketPath, 0o600);
      } catch {
        try { server.close(); } catch { /* best effort */ }
        try { unlinkSync(socketPath); } catch { /* this process created it */ }
        finish(null);
        return;
      }
      finish(server);
    });
  });
}
