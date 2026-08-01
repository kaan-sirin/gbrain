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
import { resolveEntitiesToPointers, logDeliveredReflexPointers } from '../core/context/retrieval-reflex.ts';
import {
  commandSocketPath,
  commandViaIpc,
  mcpProxySocketPath,
  cleanupStaleCommandSocket,
  CommandIpcError,
  type CommandIpcRequest,
  type CommandIpcResponse,
  startCommandIpcServer,
} from '../core/local-command-ipc.ts';

export interface LocalCommandIpcServer {
  close(): void;
}

async function localFederatedDispatchOpts(engine: BrainEngine, sourceId: string) {
  let localFederated: string[] | undefined;
  try {
    const { localFederatedSourceIds } = await import('../core/source-resolver.ts');
    localFederated = await localFederatedSourceIds(
      engine,
      sourceId,
      process.env.GBRAIN_SOURCE ? 'env' : 'seed_default',
    );
  } catch { /* scalar scope stands */ }
  return {
    remote: true as const,
    transport: 'stdio' as const,
    takesHoldersAllowList: ['world'],
    sourceId,
    ...(localFederated ? { localFederatedSourceIds: localFederated } : {}),
    metaHook: getBrainHotMemoryMeta,
  };
}

/** Dispatches an IPC request through the caller's explicit trust posture. */
export async function handleCommandIpcRequest(
  engine: BrainEngine,
  request: CommandIpcRequest,
  callerKind: 'trusted-cli' | 'mcp-proxy',
): Promise<CommandIpcResponse> {
  if (callerKind === 'trusted-cli') {
    const result = await handleToolCall(engine, request.op, request.params || {}, { cwd: request.cwd });
    return { result };
  }
  if (callerKind === 'mcp-proxy') {
    const sourceId = process.env.GBRAIN_SOURCE || 'default';
    const result = await dispatchToolCall(
      engine,
      request.op,
      request.params || {},
      await localFederatedDispatchOpts(engine, sourceId),
    );
    return { result };
  }
  throw new CommandIpcError(
    'protocol',
    'Command IPC listener has an invalid caller kind.',
    { socketPath: 'command-ipc', code: 'invalid_caller_kind' },
  );
}

/** Start the local command endpoint owned by a PGLite serve process. */
export async function startLocalCommandIpcServer(engine: BrainEngine): Promise<LocalCommandIpcServer | null> {
  const cfg = loadConfig();
  if (cfg?.engine !== 'pglite' || !cfg.database_path) return null;
  const socketPath = commandSocketPath(cfg.database_path);
  const server = await startCommandIpcServer(
    socketPath,
    'trusted-cli',
    (request, callerKind) => handleCommandIpcRequest(engine, request, callerKind),
  );
  if (!server) return null;
  const proxySocket = mcpProxySocketPath(cfg.database_path);
  const proxyServer = await startCommandIpcServer(
    proxySocket,
    'mcp-proxy',
    (request, callerKind) => handleCommandIpcRequest(engine, request, callerKind),
  );
  if (!proxyServer) {
    server.close(() => { void cleanupStaleCommandSocket(socketPath); });
    return null;
  }
  return {
    close() {
      try { server.close(() => { void cleanupStaleCommandSocket(socketPath); }); } catch { /* best effort */ }
      try { proxyServer.close(() => { void cleanupStaleCommandSocket(proxySocket); }); } catch { /* best effort */ }
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
    // #3242: when the operator didn't pin a source via GBRAIN_SOURCE, stdio
    // reads span every `config.federated = true` source (same visibility set
    // as unqualified local CLI reads). GBRAIN_SOURCE set = explicit scope,
    // no widening. Best-effort: a resolver failure keeps the scalar scope.
    // ponytail: one tiny SELECT per tool call; cache it if it ever shows up.
    // v0.28: stdio MCP has no per-token auth (local pipe). Default the
    // takes-holder allow-list to ['world'] so agent-facing callers don't
    // see private hunches via takes_list / takes_search / query. Operators
    // who want stdio to see everything should call ops directly via
    // `gbrain call <op>` (sets remote=false in src/cli.ts).
    return dispatchToolCall(engine, name, params, await localFederatedDispatchOpts(
      engine,
      process.env.GBRAIN_SOURCE || 'default',
    ));
  });

  const transport = new StdioServerTransport();
  await server.connect(transport);

  // Retrieval Reflex (#1981, D9=C): on a PGLite brain, serve owns the single
  // connection, so the context engine resolves salient entities THROUGH us over
  // a local unix socket rather than opening a second (impossible) connection.
  // Best-effort; failure to bind never blocks the MCP server.
  let resolveServer: import('node:net').Server | null = null;
  let resolveSocket: string | null = null;
  let commandIpc: LocalCommandIpcServer | null = null;
  try {
    const cfg = loadConfig();
    if (cfg?.engine === 'pglite' && cfg.database_path) {
      resolveSocket = resolveSocketPath(cfg.database_path);
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
    }
  } catch {
    /* resolve IPC is best-effort; never block serve */
  }
  try {
    commandIpc = await startLocalCommandIpcServer(engine);
  } catch {
    /* command IPC is best-effort for direct stdio serve */
  }

  // Exit cleanly when MCP client disconnects (stdin EOF) or on signals.
  // Without this, orphaned serve processes accumulate and contend for the
  // PGLite write lock, causing ingest jobs (email-sync) to time out.
  let shuttingDown = false;
  const shutdown = (reason: string, code = 0) => {
    if (shuttingDown) return;
    shuttingDown = true;
    process.stderr.write(`[gbrain-serve] shutdown: ${reason}\n`);
    try { resolveServer?.close(); } catch { /* noop */ }
    commandIpc?.close();
    if (resolveSocket) cleanupStaleSocket(resolveSocket);
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

/**
 * DB-free stdio MCP frontend. The socket endpoint, not a caller supplied
 * frame field, selects the untrusted MCP dispatch posture in the owner.
 */
export async function startIpcProxyMcpServer(
  socketPath: string,
  onTransportClose: () => void = () => {},
  createTransport: () => StdioServerTransport = () => new StdioServerTransport(),
): Promise<void> {
  const server = new Server(
    { name: 'gbrain', version: VERSION },
    { capabilities: { tools: {} } },
  );
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: buildToolDefs(operations) }));
  server.setRequestHandler(CallToolRequestSchema, async (request: any): Promise<any> => {
    const { name, arguments: params } = request.params;
    try {
      // A successful operation, including its ToolResult isError envelope,
      // crosses unchanged. Only socket/protocol failures become proxy errors.
      return (await commandViaIpc(socketPath, { op: name, params: params || {} })).result as any;
    } catch (error) {
      const detail = error instanceof CommandIpcError
        ? { error: 'ipc_transport', reason: error.reason, code: error.code, message: error.message }
        : { error: 'ipc_transport', message: error instanceof Error ? error.message : String(error) };
      return { content: [{ type: 'text', text: JSON.stringify(detail) }], isError: true };
    }
  });
  const transport = createTransport();
  let transportClosed = false;
  // The lifecycle callback is idempotent, and this local guard prevents a
  // repeated SDK close notification from trying to tear down the proxy twice.
  transport.onclose = () => {
    if (transportClosed) return;
    transportClosed = true;
    onTransportClose();
  };
  await server.connect(transport);
}

// Backward compat: used by `gbrain call` command (trusted local path).
// v0.31.8 (D22): accept opts.sourceId so `gbrain call --source X <op> <json>`
// can scope the op handler to that source. resolveSourceId() in call.ts is
// the upstream resolver; this layer just passes the resolved id through.
export async function handleToolCall(
  engine: BrainEngine,
  tool: string,
  params: Record<string, unknown>,
  opts?: { sourceId?: string; cwd?: string },
): Promise<unknown> {
  const op = operations.find(o => o.name === tool);
  if (!op) throw new Error(`Unknown tool: ${tool}`);

  const validationError = validateParams(op, params);
  if (validationError) throw new Error(validationError);

  let sourceId = opts?.sourceId;
  let localFederated: string[] | undefined;
  if (!sourceId) {
    const explicit = (params.source as string | undefined) ?? null;
    const { resolveSourceWithTier, localFederatedSourceIds } = await import('../core/source-resolver.ts');
    try {
      const resolved = await resolveSourceWithTier(engine, explicit, opts?.cwd);
      sourceId = resolved.source_id;
      localFederated = await localFederatedSourceIds(engine, resolved.source_id, resolved.tier);
    } catch (error) {
      if (explicit) throw error;
      sourceId = 'default';
    }
  }
  const ctx = buildOperationContext(engine, params, {
    remote: false,
    logger: { info: console.log, warn: console.warn, error: console.error },
    sourceId,
    ...(localFederated ? { localFederatedSourceIds: localFederated } : {}),
  });

  return op.handler(ctx, params);
}
