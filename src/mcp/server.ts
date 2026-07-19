import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { ListToolsRequestSchema, CallToolRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import type { BrainEngine } from '../core/engine.ts';
import { operations } from '../core/operations.ts';
import { VERSION } from '../version.ts';
import { buildToolDefs } from './tool-defs.ts';
import { dispatchToolCall, validateParams, buildOperationContext } from './dispatch.ts';
import { getBrainHotMemoryMeta } from '../core/facts/meta-hook.ts';
import { loadConfig } from '../core/config.ts';
import {
  resolveSocketPath,
  startResolveIpcServer,
  cleanupStaleSocket,
} from '../core/context/resolve-ipc.ts';
import {
  commandSocketPath,
  commandViaIpc,
  CommandIpcError,
  type CommandIpcRequest,
  type CommandIpcResponse,
  importSocketPath,
  startCommandIpcServer,
  startImportIpcServer,
  cleanupStaleImportSocket,
} from '../core/context/import-ipc.ts';
import { resolveEntitiesToPointers, logDeliveredReflexPointers } from '../core/context/retrieval-reflex.ts';

export interface LocalIpcServers {
  close(): void;
}

function buildRemoteMcpDispatchOpts(sourceId: string) {
  return {
    remote: true as const,
    takesHoldersAllowList: ['world'],
    sourceId,
    metaHook: getBrainHotMemoryMeta,
  };
}

function formatProxyCommandIpcError(socketPath: string, err: unknown): string {
  if (err instanceof CommandIpcError) {
    switch (err.reason) {
      case 'socket_missing':
        return `GBrain IPC proxy could not find the local daemon socket at ${socketPath}. Start the daemon with: gbrain serve --local-daemon`;
      case 'socket_unavailable':
      case 'timeout':
        return `GBrain IPC proxy could not reach the local daemon socket at ${socketPath}. Restart the daemon with: gbrain serve --local-daemon`;
      case 'payload_too_large':
        return err.message;
      case 'operation':
      case 'protocol':
        return err.message;
      default:
        return err.message;
    }
  }
  return err instanceof Error ? err.message : String(err);
}

export async function handleCommandIpcRequest(
  engine: BrainEngine,
  req: CommandIpcRequest,
  opts: { sourceId: string },
): Promise<CommandIpcResponse> {
  const params = req.params || {};
  if (req.callerKind === 'trusted-cli') {
    const result = await handleToolCall(engine, req.op, params, { sourceId: opts.sourceId });
    return { result };
  }
  if (req.callerKind === 'mcp-proxy') {
    const result = await dispatchToolCall(
      engine,
      req.op,
      params,
      buildRemoteMcpDispatchOpts(opts.sourceId),
    );
    return { result };
  }
  throw new CommandIpcError(
    'protocol',
    'Command IPC callerKind must be one of: trusted-cli, mcp-proxy.',
    { socketPath: 'command-ipc', code: 'invalid_caller_kind' },
  );
}

export async function startLocalIpcServers(engine: BrainEngine): Promise<LocalIpcServers> {
  let resolveServer: import('node:net').Server | null = null;
  let resolveSocket: string | null = null;
  let importServer: import('node:net').Server | null = null;
  let importSocket: string | null = null;
  let commandServer: import('node:net').Server | null = null;
  let commandSocket: string | null = null;

  const cfg = loadConfig();
  if (cfg?.engine === 'pglite' && cfg.database_path) {
    resolveSocket = resolveSocketPath(cfg.database_path);
    importSocket = importSocketPath(cfg.database_path);
    commandSocket = commandSocketPath(cfg.database_path);
    const defaultSource = process.env.GBRAIN_SOURCE || 'default';
    resolveServer = await startResolveIpcServer(
      resolveSocket,
      (req) =>
        resolveEntitiesToPointers(
          engine,
          req.sourceId || defaultSource,
          req.candidates ?? [],
          {
            priorContextText: req.priorContextText,
            maxPointers: req.maxPointers,
            suppression: req.suppression,
          },
        ),
      // The IPC resolve path IS the ambient reflex channel. Logging happens
      // at DELIVERY (post-write), not inside the resolver — a block the
      // client's 250ms budget abandoned was never injected, and counting it
      // would corrupt the volunteered-vs-used precision stats (red-team).
      (block) => logDeliveredReflexPointers(engine, block.pointers),
    );
    importServer = await startImportIpcServer(
      importSocket,
      async (req) => {
        const { runImport } = await import('../commands/import.ts');
        const result = await runImport(engine, req.args || []);
        return { result };
      },
    );
    commandServer = await startCommandIpcServer(
      commandSocket,
      (req) => handleCommandIpcRequest(engine, req, { sourceId: defaultSource }),
    );
  }

  return {
    close() {
      try { resolveServer?.close(); } catch { /* noop */ }
      try { importServer?.close(); } catch { /* noop */ }
      try { commandServer?.close(); } catch { /* noop */ }
      if (resolveSocket) cleanupStaleSocket(resolveSocket);
      if (importSocket) cleanupStaleImportSocket(importSocket);
      if (commandSocket) cleanupStaleImportSocket(commandSocket);
    },
  };
}

export async function startMcpServer(engine: BrainEngine) {
  const server = new Server(
    { name: 'gbrain', version: VERSION },
    { capabilities: { tools: {} } },
  );

  // Generate tool definitions from operations. Extracted to buildToolDefs so
  // the subagent tool registry (v0.15+) can call the same mapper against a
  // filtered OPERATIONS subset instead of duplicating this shape.
  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: buildToolDefs(operations),
  }));

  // Dispatch tool calls via shared dispatch.ts (parity with HTTP transport).
  // MCP stdio callers are remote/untrusted; dispatch defaults remote=true.
  // The MCP SDK's response type widened in 1.29 to allow a managed-task wrapper;
  // gbrain ops are synchronous, so we return the legacy `{ content, isError? }`
  // shape and cast through `any` (the SDK accepts it via the ServerResult union).
  server.setRequestHandler(CallToolRequestSchema, async (request: any): Promise<any> => {
    const { name, arguments: params } = request.params;
    // v0.28: stdio MCP has no per-token auth (local pipe). Default the
    // takes-holder allow-list to ['world'] so agent-facing callers don't
    // see private hunches via takes_list / takes_search / query. Operators
    // who want stdio to see everything should call ops directly via
    // `gbrain call <op>` (sets remote=false in src/cli.ts).
    return dispatchToolCall(
      engine,
      name,
      params,
      buildRemoteMcpDispatchOpts(process.env.GBRAIN_SOURCE || 'default'),
    );
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);

  // Retrieval Reflex (#1981, D9=C): on a PGLite brain, serve owns the single
  // connection, so the context engine resolves salient entities THROUGH us over
  // a local unix socket rather than opening a second (impossible) connection.
  // Best-effort; failure to bind never blocks the MCP server.
  let localIpc: LocalIpcServers | null = null;
  try {
    localIpc = await startLocalIpcServers(engine);
  } catch {
    /* local IPC is best-effort; never block MCP serve */
  }

  // Exit cleanly when MCP client disconnects (stdin EOF) or on signals.
  // Without this, orphaned serve processes accumulate and contend for the
  // PGLite write lock, causing ingest jobs (email-sync) to time out.
  let shuttingDown = false;
  const shutdown = (reason: string, code = 0) => {
    if (shuttingDown) return;
    shuttingDown = true;
    process.stderr.write(`[gbrain-serve] shutdown: ${reason}\n`);
    localIpc?.close();
    Promise.resolve(engine.disconnect?.())
      .catch(() => {})
      .finally(() => process.exit(code));
  };
  // v0.34.1 (#870): when MCP_STDIO=1, the wrapping gateway (OpenClaw's
  // bundle-mcp layer, others) often pipes the JSON-RPC handshake then
  // closes its stdin half. Treating that as a permanent disconnect kills
  // the server before the first tool call arrives. Signal handlers and
  // transport.onclose still cover the legitimate shutdown paths.
  if (process.env.MCP_STDIO !== '1') {
    process.stdin.on('end', () => shutdown('stdin end'));
    process.stdin.on('close', () => shutdown('stdin close'));
  }
  // @ts-ignore — SDK exposes onclose on transport
  transport.onclose = () => shutdown('transport close');
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGHUP', () => shutdown('SIGHUP'));
}

export async function startIpcProxyMcpServer(socketPath: string) {
  const server = new Server(
    { name: 'gbrain', version: VERSION },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: buildToolDefs(operations),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request: any): Promise<any> => {
    const { name, arguments: params } = request.params;
    try {
      const { result } = await commandViaIpc(socketPath, {
        callerKind: 'mcp-proxy',
        op: name,
        params: params || {},
      });
      return result as any;
    } catch (err) {
      throw new Error(formatProxyCommandIpcError(socketPath, err));
    }
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);

  let shuttingDown = false;
  const shutdown = (reason: string, code = 0) => {
    if (shuttingDown) return;
    shuttingDown = true;
    process.stderr.write(`[gbrain-serve] shutdown: ${reason}\n`);
    process.exit(code);
  };
  if (process.env.MCP_STDIO !== '1') {
    process.stdin.on('end', () => shutdown('stdin end'));
    process.stdin.on('close', () => shutdown('stdin close'));
  }
  // @ts-ignore — SDK exposes onclose on transport
  transport.onclose = () => shutdown('transport close');
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGHUP', () => shutdown('SIGHUP'));
}

// Backward compat: used by `gbrain call` command (trusted local path).
// v0.31.8 (D22): accept opts.sourceId so `gbrain call --source X <op> <json>`
// can scope the op handler to that source. resolveSourceId() in call.ts is
// the upstream resolver; this layer just passes the resolved id through.
export async function handleToolCall(
  engine: BrainEngine,
  tool: string,
  params: Record<string, unknown>,
  opts?: { sourceId?: string },
): Promise<unknown> {
  const op = operations.find(o => o.name === tool);
  if (!op) throw new Error(`Unknown tool: ${tool}`);

  const validationError = validateParams(op, params);
  if (validationError) throw new Error(validationError);

  const ctx = buildOperationContext(engine, params, {
    remote: false,
    logger: { info: console.log, warn: console.warn, error: console.error },
    ...(opts?.sourceId ? { sourceId: opts.sourceId } : {}),
  });

  return op.handler(ctx, params);
}
