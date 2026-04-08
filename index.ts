/**
 * A2A Gateway plugin endpoints:
 * - /.well-known/agent.json  (Agent Card discovery)
 * - /a2a/jsonrpc              (JSON-RPC transport)
 * - /a2a/rest                 (REST transport)
 * - gRPC on port+1            (gRPC transport)
 */

import type { Server } from "node:http";
import os from "node:os";
import path from "node:path";

import { AGENT_CARD_PATH } from "@a2a-js/sdk";
import { DefaultRequestHandler } from "@a2a-js/sdk/server";
import { UserBuilder, agentCardHandler, jsonRpcHandler, restHandler } from "@a2a-js/sdk/server/express";
import { grpcService, A2AService, UserBuilder as GrpcUserBuilder } from "@a2a-js/sdk/server/grpc";
import { Server as GrpcServer, ServerCredentials, status as GrpcStatus } from "@grpc/grpc-js";
import express from "express";

import { buildAgentCard } from "./src/agent-card.js";
import { A2AClient } from "./src/client.js";
import { OpenClawAgentExecutor } from "./src/executor.js";
import { QueueingAgentExecutor } from "./src/queueing-executor.js";
import { runTaskCleanup } from "./src/task-cleanup.js";
import { FileTaskStore } from "./src/task-store.js";
import { GatewayTelemetry } from "./src/telemetry.js";
import { AuditLogger } from "./src/audit.js";
import { PeerHealthManager } from "./src/peer-health.js";
import type {
  AgentCardConfig,
  GatewayConfig,
  InboundAuth,
  OpenClawPluginApi,
  PeerConfig,
} from "./src/types.js";
import {
  validateUri,
  validateMimeType,
} from "./src/file-security.js";
import { FederationGateway } from "./src/internal/federation.js";
import type { FederationConfig } from "./src/internal/federation.js";
import { GatewayRpcDispatcher } from "./src/internal/gateway-dispatcher.js";

/** Build a JSON-RPC error response. */
function jsonRpcError(id: string | number | null, code: number, message: string) {
  return { jsonrpc: "2.0" as const, id, error: { code, message } };
}

function asObject(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object") {
    return {};
  }

  return value as Record<string, unknown>;
}

function asString(value: unknown, fallback = ""): string {
  if (typeof value === "string") {
    return value;
  }

  return fallback;
}

function asNumber(value: unknown, fallback: number): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  return fallback;
}

function asBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }

  return fallback;
}

function normalizeHttpPath(value: string, fallback: string): string {
  const trimmed = value.trim() || fallback;
  return trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
}

function resolveConfiguredPath(
  value: unknown,
  fallback: string,
  resolvePath?: (nextPath: string) => string,
): string {
  const configured = asString(value, "").trim() || fallback;
  const resolved = resolvePath ? resolvePath(configured) : configured;
  return path.isAbsolute(resolved) ? resolved : path.resolve(resolved);
}

function parseAgentCard(raw: Record<string, unknown>): AgentCardConfig {
  const skills = Array.isArray(raw.skills) ? raw.skills : [];

  return {
    name: asString(raw.name, "OpenClaw A2A Gateway"),
    description: asString(raw.description, "A2A bridge for OpenClaw agents"),
    url: asString(raw.url, ""),
    skills: skills.map((entry) => {
      if (typeof entry === "string") {
        return entry;
      }
      const skill = asObject(entry);
      return {
        id: asString(skill.id, ""),
        name: asString(skill.name, "unknown"),
        description: asString(skill.description, ""),
      };
    }),
  };
}

function parsePeers(raw: unknown): PeerConfig[] {
  if (!Array.isArray(raw)) {
    return [];
  }

  const peers: PeerConfig[] = [];
  for (const entry of raw) {
    const value = asObject(entry);
    const name = asString(value.name, "");
    const agentCardUrl = asString(value.agentCardUrl, "");
    if (!name || !agentCardUrl) {
      continue;
    }

    const authRaw = asObject(value.auth);
    const authTypeRaw = asString(authRaw.type, "");
    const authType = authTypeRaw === "bearer" || authTypeRaw === "apiKey" ? authTypeRaw : "";
    const token = asString(authRaw.token, "");

    peers.push({
      name,
      agentCardUrl,
      auth: authType && token ? { type: authType, token } : undefined,
    });
  }

  return peers;
}

export function parseConfig(raw: unknown, resolvePath?: (nextPath: string) => string): GatewayConfig {
  const config = asObject(raw);
  const server = asObject(config.server);
  const storage = asObject(config.storage);
  const security = asObject(config.security);
  const routing = asObject(config.routing);
  const limits = asObject(config.limits);
  const observability = asObject(config.observability);
  const timeouts = asObject(config.timeouts);
  const federation = asObject(config.federation);
  const resilience = asObject(config.resilience);
  const healthCheck = asObject(resilience.healthCheck);
  const retry = asObject(resilience.retry);
  const circuitBreaker = asObject(resilience.circuitBreaker);

  const inboundAuth = asString(security.inboundAuth, "none") as InboundAuth;

  const defaultMimeTypes = [
    "image/*", "application/pdf", "text/plain", "text/csv",
    "application/json", "audio/*", "video/*",
  ];
  const rawAllowedMime = Array.isArray(security.allowedMimeTypes) ? security.allowedMimeTypes : [];
  const allowedMimeTypes = rawAllowedMime.length > 0
    ? rawAllowedMime.filter((v: unknown) => typeof v === "string") as string[]
    : defaultMimeTypes;
  const rawUriAllowlist = Array.isArray(security.fileUriAllowlist) ? security.fileUriAllowlist : [];
  const fileUriAllowlist = rawUriAllowlist.filter((v: unknown) => typeof v === "string") as string[];

  return {
    agentCard: parseAgentCard(asObject(config.agentCard)),
    server: {
      host: asString(server.host, "0.0.0.0"),
      port: asNumber(server.port, 18800),
    },
    storage: {
      tasksDir: resolveConfiguredPath(
        storage.tasksDir,
        path.join(os.homedir(), ".openclaw", "a2a-tasks"),
        resolvePath,
      ),
      taskTtlHours: Math.max(1, asNumber(storage.taskTtlHours, 72)),
      cleanupIntervalMinutes: Math.max(1, asNumber(storage.cleanupIntervalMinutes, 60)),
    },
    peers: parsePeers(config.peers),
    security: (() => {
      const singleToken = asString(security.token, "");
      const tokenArray = Array.isArray(security.tokens)
        ? (security.tokens as unknown[]).filter((t): t is string => typeof t === "string" && t.length > 0)
        : [];
      const validTokens = new Set<string>(
        [singleToken, ...tokenArray].filter(t => t.length > 0),
      );
      return {
        inboundAuth: inboundAuth === "bearer" ? "bearer" : "none" as const,
        token: singleToken,
        tokens: tokenArray,
        validTokens,
        allowedMimeTypes,
        maxFileSizeBytes: asNumber(security.maxFileSizeBytes, 52_428_800),
        maxInlineFileSizeBytes: asNumber(security.maxInlineFileSizeBytes, 10_485_760),
        fileUriAllowlist,
      };
    })(),
    routing: {
      defaultAgentId: asString(routing.defaultAgentId, "default"),
    },
    limits: {
      maxConcurrentTasks: Math.max(1, Math.floor(asNumber(limits.maxConcurrentTasks, 4))),
      maxQueuedTasks: Math.max(0, Math.floor(asNumber(limits.maxQueuedTasks, 100))),
    },
    observability: {
      structuredLogs: asBoolean(observability.structuredLogs, true),
      exposeMetricsEndpoint: asBoolean(observability.exposeMetricsEndpoint, true),
      metricsPath: normalizeHttpPath(asString(observability.metricsPath, "/a2a/metrics"), "/a2a/metrics"),
      metricsAuth: (asString(observability.metricsAuth, "none") === "bearer" ? "bearer" : "none") as "none" | "bearer",
      auditLogPath: resolveConfiguredPath(
        observability.auditLogPath,
        path.join(os.homedir(), ".openclaw", "a2a-audit.jsonl"),
        resolvePath,
      ),
    },
    timeouts: {
      agentResponseTimeoutMs: asNumber(timeouts.agentResponseTimeoutMs, 300_000),
    },
    resilience: {
      healthCheck: {
        enabled: asBoolean(healthCheck.enabled, true),
        intervalMs: asNumber(healthCheck.intervalMs, 30_000),
        timeoutMs: asNumber(healthCheck.timeoutMs, 5_000),
      },
      retry: {
        maxRetries: Math.max(0, Math.floor(asNumber(retry.maxRetries, 3))),
        baseDelayMs: asNumber(retry.baseDelayMs, 1_000),
        maxDelayMs: asNumber(retry.maxDelayMs, 10_000),
      },
      circuitBreaker: {
        failureThreshold: Math.max(1, Math.floor(asNumber(circuitBreaker.failureThreshold, 5))),
        resetTimeoutMs: asNumber(circuitBreaker.resetTimeoutMs, 30_000),
      },
    },
    federation: federation ? {
      enabled: asBoolean(federation.enabled, false),
      gatewayId: asString(federation.gatewayId, ""),
      peers: Array.isArray(federation.peers)
        ? (federation.peers as unknown[]).filter((p): p is Record<string, unknown> => {
            const obj = asObject(p);
            return !!obj && typeof obj.gatewayId === "string" && typeof obj.inboxUrl === "string" && typeof obj.hmacSecret === "string";
          }).map((p) => ({
            gatewayId: asString((p as Record<string, unknown>).gatewayId, ""),
            inboxUrl: asString((p as Record<string, unknown>).inboxUrl, ""),
            hmacSecret: asString((p as Record<string, unknown>).hmacSecret, ""),
          }))
        : [],
      limits: (() => {
        const fedLimits = asObject(federation.limits);
        return {
          maxHops: asNumber(fedLimits?.maxHops, 5),
          maxPayloadBytes: asNumber(fedLimits?.maxPayloadBytes, 2_097_152),
        };
      })(),
      defaultAgentId: asString(federation.defaultAgentId, "main"),
      timestampSkewSeconds: asNumber(federation.timestampSkewSeconds, 300),
    } : undefined,
  };
}

function normalizeCardPath(): string {
  if (AGENT_CARD_PATH.startsWith("/")) {
    return AGENT_CARD_PATH;
  }

  return `/${AGENT_CARD_PATH}`;
}

const plugin = {
  id: "a2a-gateway",
  name: "A2A Gateway",
  description: "OpenClaw plugin that serves A2A v0.3.0 endpoints",

  register(api: OpenClawPluginApi) {
    const config = parseConfig(api.pluginConfig, api.resolvePath?.bind(api));
    const telemetry = new GatewayTelemetry(api.logger, {
      structuredLogs: config.observability.structuredLogs,
    });
    const auditLogger = new AuditLogger(config.observability.auditLogPath);
    const client = new A2AClient();
    const taskStore = new FileTaskStore(config.storage.tasksDir);
    const executor = new QueueingAgentExecutor(
      new OpenClawAgentExecutor(api, config),
      telemetry,
      config.limits,
    );
    const agentCard = buildAgentCard(config);

    // Peer resilience: health check + circuit breaker
    const healthManager = config.peers.length > 0
      ? new PeerHealthManager(
          config.peers,
          config.resilience.healthCheck,
          config.resilience.circuitBreaker,
          async (peer) => {
            try {
              await client.discoverAgentCard(peer, config.resilience.healthCheck.timeoutMs);
              return true;
            } catch {
              return false;
            }
          },
          (level, msg, details) => {
            if (level === "error") {
              api.logger.error(details ? `${msg}: ${JSON.stringify(details)}` : msg);
            } else if (level === "warn") {
              api.logger.warn(details ? `${msg}: ${JSON.stringify(details)}` : msg);
            } else {
              api.logger.info(details ? `${msg}: ${JSON.stringify(details)}` : msg);
            }
          },
        )
      : null;

    // Wire peer state into telemetry snapshot
    if (healthManager) {
      telemetry.setPeerStateProvider(() => healthManager.getAllStates());
    }

    // Wire audit logger for inbound task completion
    telemetry.setTaskAuditCallback((taskId, contextId, state, durationMs) => {
      auditLogger.recordInbound(taskId, contextId, state, durationMs);
    });

    // SDK expects userBuilder(req) -> Promise<User>
    // When bearer auth is configured, validate the Authorization header.
    const userBuilder = async (req: { headers?: Record<string, string | string[] | undefined> }) => {
      if (config.security.inboundAuth === "bearer" && config.security.validTokens.size > 0) {
        const authHeader = req.headers?.authorization;
        const header = Array.isArray(authHeader) ? authHeader[0] : authHeader;
        const providedToken = typeof header === "string" && header.startsWith("Bearer ") ? header.slice(7) : "";
        if (!providedToken || !config.security.validTokens.has(providedToken)) {
          telemetry.recordSecurityRejection("http", "invalid or missing bearer token");
          auditLogger.recordSecurityEvent("http", "invalid or missing bearer token");
          throw jsonRpcError(null, -32000, "Unauthorized: invalid or missing bearer token");
        }
      }
      return UserBuilder.noAuthentication();
    };

    const requestHandler = new DefaultRequestHandler(agentCard, taskStore, executor);

    const app = express();
    const createHttpMetricsMiddleware =
      (route: "jsonrpc" | "rest" | "metrics") =>
      (_req: express.Request, res: express.Response, next: express.NextFunction) => {
        const startedAt = Date.now();
        res.on("finish", () => {
          telemetry.recordInboundHttp(route, res.statusCode, Date.now() - startedAt);
        });
        next();
      };

    const cardPath = normalizeCardPath();
    const cardEndpointHandler = agentCardHandler({ agentCardProvider: requestHandler });

    app.use(cardPath, cardEndpointHandler);
    if (cardPath != "/.well-known/agent.json") {
      app.use("/.well-known/agent.json", cardEndpointHandler);
    }

    app.use(
      "/a2a/jsonrpc",
      createHttpMetricsMiddleware("jsonrpc"),
      jsonRpcHandler({
        requestHandler,
        userBuilder,
      })
    );

    // Ensure errors return JSON-RPC style responses (avoid Express HTML error pages)
    app.use("/a2a/jsonrpc", (err: unknown, _req: unknown, res: any, next: (e?: unknown) => void) => {
      if (err instanceof SyntaxError) {
        res.status(400).json(jsonRpcError(null, -32700, "Parse error"));
        return;
      }

      // Surface A2A-specific errors with proper codes
      const a2aErr = err as { code?: number; message?: string; taskId?: string } | undefined;
      if (a2aErr && typeof a2aErr.code === "number") {
        const status = a2aErr.code === -32601 ? 404 : 400;
        res.status(status).json(jsonRpcError(null, a2aErr.code, a2aErr.message || "Unknown error"));
        return;
      }

      // Generic internal error
      res.status(500).json(jsonRpcError(null, -32603, "Internal error"));
    });

    app.use(
      "/a2a/rest",
      createHttpMetricsMiddleware("rest"),
      restHandler({
        requestHandler,
        userBuilder,
      })
    );

    if (config.observability.exposeMetricsEndpoint) {
      app.get(
        config.observability.metricsPath,
        createHttpMetricsMiddleware("metrics"),
        (req, res, next) => {
          if (config.observability.metricsAuth === "bearer" && config.security.validTokens.size > 0) {
            const authHeader = req.headers.authorization;
            const header = Array.isArray(authHeader) ? authHeader[0] : authHeader;
            const token = typeof header === "string" && header.startsWith("Bearer ") ? header.slice(7) : "";
            if (!token || !config.security.validTokens.has(token)) {
              res.status(401).json({ error: "Unauthorized: invalid or missing bearer token" });
              return;
            }
          }
          next();
        },
        (_req, res) => {
          res.json(telemetry.snapshot());
        },
      );
    }

    // ------------------------------------------------------------------
    // Federation layer
    // ------------------------------------------------------------------
    let federationGateway: FederationGateway | null = null;
    if (config.federation?.enabled && config.federation.peers && config.federation.peers.length > 0) {
      const fedConfig: FederationConfig = {
        gatewayId: config.federation.gatewayId || `gateway-${config.server.port}`,
        peers: config.federation.peers,
        limits: {
          maxHops: config.federation.limits?.maxHops ?? 5,
          maxPayloadBytes: config.federation.limits?.maxPayloadBytes ?? 2_097_152,
        },
        defaultAgentId: config.federation.defaultAgentId || "main",
        timestampSkewSeconds: config.federation.timestampSkewSeconds ?? 300,
      };

      const fedDispatcher = GatewayRpcDispatcher.createFromPluginApi(api);
      federationGateway = new FederationGateway(fedConfig, fedDispatcher);

      // Mount federation inbox on express app (must be before app.listen)
      // Use express.json() for the federation inbox route
      app.use("/a2a/v1/inbox", express.json());
      federationGateway.start(app);

      api.logger.info(
        `a2a-gateway: federation enabled — gatewayId=${fedConfig.gatewayId} peers=${fedConfig.peers.length}`,
      );
    }

    let server: Server | null = null;
    let grpcServer: GrpcServer | null = null;
    let cleanupTimer: ReturnType<typeof setInterval> | null = null;
    const grpcPort = config.server.port + 1;

    api.registerGatewayMethod("a2a.metrics", ({ respond }) => {
      respond(true, {
        metrics: telemetry.snapshot(),
      });
    });

    if (federationGateway) {
      api.registerGatewayMethod("a2a.federation.send", ({ params, respond }) => {
        const payload = asObject(params);
        const destGatewayId = asString(payload.destGatewayId, "");
        const destAgentId = asString(payload.destAgentId, "") || undefined;
        const message = asString(payload.message, "");
        const correlationId = asString(payload.correlationId, "") || undefined;

        if (!destGatewayId || !message) {
          respond(false, { error: "destGatewayId and message are required" });
          return;
        }

        const result = federationGateway!.sendToGateway(destGatewayId, destAgentId, message, {
          correlationId,
        });
        respond(result.ok, result);
      });

      api.registerGatewayMethod("a2a.federation.metrics", ({ respond }) => {
        respond(true, federationGateway!.getMetrics());
      });
    }

    api.registerGatewayMethod("a2a.audit", ({ params, respond }) => {
      const payload = asObject(params);
      const count = Math.min(Math.max(1, asNumber(payload.count, 50)), 500);
      auditLogger
        .tail(count)
        .then((entries) => respond(true, { entries, count: entries.length }))
        .catch((error) => respond(false, { error: String(error?.message || error) }));
    });

    api.registerGatewayMethod("a2a.send", ({ params, respond }) => {
      const payload = asObject(params);
      const peerName = asString(payload.peer || payload.name, "");
      const message = asObject(payload.message || payload.payload);

      const peer = config.peers.find((candidate) => candidate.name === peerName);
      if (!peer) {
        respond(false, { error: `Peer not found: ${peerName}` });
        return;
      }

      const startedAt = Date.now();
      const sendOptions = {
        healthManager: healthManager ?? undefined,
        retryConfig: config.resilience.retry,
        log: (level: "info" | "warn", msg: string, details?: Record<string, unknown>) => {
          if (details?.attempt) {
            telemetry.recordPeerRetry(peer.name, details.attempt as number);
          }
          api.logger[level](details ? `${msg}: ${JSON.stringify(details)}` : msg);
        },
      };
      client
        .sendMessage(peer, message, sendOptions)
        .then((result) => {
          const outDuration = Date.now() - startedAt;
          telemetry.recordOutboundRequest(peer.name, result.ok, result.statusCode, outDuration);
          auditLogger.recordOutbound(peer.name, result.ok, result.statusCode, outDuration);
          if (result.ok) {
            respond(true, {
              statusCode: result.statusCode,
              response: result.response,
            });
            return;
          }

          respond(false, {
            statusCode: result.statusCode,
            response: result.response,
          });
        })
        .catch((error) => {
          const errDuration = Date.now() - startedAt;
          telemetry.recordOutboundRequest(peer.name, false, 500, errDuration);
          auditLogger.recordOutbound(peer.name, false, 500, errDuration);
          respond(false, { error: String(error?.message || error) });
        });
    });

    // ------------------------------------------------------------------
    // Agent tool: a2a_send_file
    // Lets the agent send a file (by URI) to a peer via A2A FilePart.
    // ------------------------------------------------------------------
    if (api.registerTool) {
      const sendFileParams = {
        type: "object" as const,
        required: ["peer", "uri"],
        properties: {
          peer: { type: "string" as const, description: "Name of the target peer (must match a configured peer name)" },
          uri: { type: "string" as const, description: "Public URL of the file to send" },
          name: { type: "string" as const, description: "Filename (e.g. report.pdf)" },
          mimeType: { type: "string" as const, description: "MIME type (e.g. application/pdf). Auto-detected from extension if omitted." },
          text: { type: "string" as const, description: "Optional text message to include alongside the file" },
          agentId: { type: "string" as const, description: "Route to a specific agentId on the peer (OpenClaw extension). Omit to use the peer's default agent." },
        },
      };

      api.registerTool({
        name: "a2a_send_file",
        description: "Send a file to a peer agent via A2A. The file is referenced by its public URL (URI). " +
          "Use this when you need to transfer a document, image, or any file to another agent.",
        label: "A2A Send File",
        parameters: sendFileParams,
        async execute(toolCallId, params) {
          const peer = config.peers.find((p) => p.name === params.peer);
          if (!peer) {
            const available = config.peers.map((p) => p.name).join(", ") || "(none)";
            return {
              content: [{ type: "text" as const, text: `Peer not found: "${params.peer}". Available peers: ${available}` }],
              details: { ok: false },
            };
          }

          // Security checks: SSRF, MIME, file size
          const uriCheck = await validateUri(params.uri, config.security);
          if (!uriCheck.ok) {
            return {
              content: [{ type: "text" as const, text: `URI rejected: ${uriCheck.reason}` }],
              details: { ok: false, reason: uriCheck.reason },
            };
          }

          if (params.mimeType && !validateMimeType(params.mimeType, config.security.allowedMimeTypes)) {
            return {
              content: [{ type: "text" as const, text: `MIME type rejected: "${params.mimeType}" is not in the allowed list` }],
              details: { ok: false },
            };
          }

          const parts: Array<Record<string, unknown>> = [];
          if (params.text) {
            parts.push({ kind: "text", text: params.text });
          }
          parts.push({
            kind: "file",
            file: {
              uri: params.uri,
              ...(params.name ? { name: params.name } : {}),
              ...(params.mimeType ? { mimeType: params.mimeType } : {}),
            },
          });

          try {
            const message: Record<string, unknown> = { parts };
            if (params.agentId) {
              message.agentId = params.agentId;
            }
            const result = await client.sendMessage(peer, message, {
              healthManager: healthManager ?? undefined,
              retryConfig: config.resilience.retry,
            });
            if (result.ok) {
              return {
                content: [{ type: "text" as const, text: `File sent to ${params.peer} via A2A.\nURI: ${params.uri}\nResponse: ${JSON.stringify(result.response)}` }],
                details: { ok: true, response: result.response },
              };
            }
            return {
              content: [{ type: "text" as const, text: `Failed to send file to ${params.peer}: ${JSON.stringify(result.response)}` }],
              details: { ok: false, response: result.response },
            };
          } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            return {
              content: [{ type: "text" as const, text: `Error sending file to ${params.peer}: ${msg}` }],
              details: { ok: false, error: msg },
            };
          }
        },
      });
    }

    // ------------------------------------------------------------------
    // Agent tool: a2a_send
    // Lets the agent send a text message (and optional JSON data) to a
    // named peer via A2A message/send — no curl required.
    // ------------------------------------------------------------------
    if (api.registerTool) {
      const sendParams = {
        type: "object" as const,
        required: ["peer", "message"],
        properties: {
          peer: {
            type: "string" as const,
            description: "Name of the target peer (must match a configured peer name)",
          },
          message: {
            type: "string" as const,
            description: "Text message to send to the peer agent",
          },
          agentId: {
            type: "string" as const,
            description:
              "Route to a specific agentId on the peer (OpenClaw extension). Omit to use the peer's default agent.",
          },
          data: {
            type: "object" as const,
            description:
              "Optional structured JSON payload to include as an A2A DataPart alongside the text message.",
            additionalProperties: true,
            properties: {},
          },
        },
      };

      api.registerTool({
        name: "a2a_send",
        description:
          "Send a text message to a peer agent via A2A (message/send). " +
          "Supports optional structured JSON data as a DataPart. " +
          "Use this to communicate with Woodhouse, Ray, or any other configured A2A peer without curl.",
        label: "A2A Send Message",
        parameters: sendParams,
        async execute(_toolCallId, params) {
          const peer = config.peers.find((p) => p.name === params.peer);
          if (!peer) {
            const available =
              config.peers.map((p) => p.name).join(", ") || "(none configured)";
            return {
              content: [
                {
                  type: "text" as const,
                  text: `Peer not found: "${params.peer}". Available peers: ${available}`,
                },
              ],
              details: { ok: false },
            };
          }

          const parts: Array<Record<string, unknown>> = [
            { kind: "text", text: params.message },
          ];

          if (params.data && typeof params.data === "object") {
            parts.push({
              kind: "data",
              data: params.data,
              mimeType: "application/json",
            });
          }

          const outboundMessage: Record<string, unknown> = { parts };
          if (params.agentId) {
            outboundMessage.agentId = params.agentId;
          }

          try {
            const startedAt = Date.now();
            const result = await client.sendMessage(peer, outboundMessage, {
              healthManager: healthManager ?? undefined,
              retryConfig: config.resilience.retry,
              log: (level, msg, details) => {
                if (details?.attempt) {
                  telemetry.recordPeerRetry(peer.name, details.attempt as number);
                }
                api.logger[level](details ? `${msg}: ${JSON.stringify(details)}` : msg);
              },
            });

            const duration = Date.now() - startedAt;
            telemetry.recordOutboundRequest(peer.name, result.ok, result.statusCode, duration);
            auditLogger.recordOutbound(peer.name, result.ok, result.statusCode, duration);

            if (result.ok) {
              return {
                content: [
                  {
                    type: "text" as const,
                    text:
                      `Message sent to ${params.peer} via A2A.\n` +
                      `Response: ${JSON.stringify(result.response)}`,
                  },
                ],
                details: { ok: true, response: result.response },
              };
            }

            return {
              content: [
                {
                  type: "text" as const,
                  text: `Failed to send message to ${params.peer}: ${JSON.stringify(result.response)}`,
                },
              ],
              details: { ok: false, response: result.response },
            };
          } catch (err: unknown) {
            const msg = err instanceof Error ? err.message : String(err);
            return {
              content: [
                {
                  type: "text" as const,
                  text: `Error sending message to ${params.peer}: ${msg}`,
                },
              ],
              details: { ok: false, error: msg },
            };
          }
        },
      });
    }

    // ------------------------------------------------------------------
    // Agent tool: a2a_peers
    // Lists configured peers with live health and circuit-breaker status.
    // ------------------------------------------------------------------
    if (api.registerTool) {
      api.registerTool({
        name: "a2a_peers",
        description:
          "List all configured A2A peers with their endpoint URLs and live health/circuit-breaker status. " +
          "Use this to see which agents are available before sending a message.",
        label: "A2A List Peers",
        parameters: {
          type: "object" as const,
          required: [],
          properties: {},
        },
        async execute(_toolCallId, _params) {
          if (config.peers.length === 0) {
            return {
              content: [{ type: "text" as const, text: "No A2A peers configured." }],
              details: { peers: [] },
            };
          }

          const peerStates: Map<string, import("./src/types.js").PeerState> =
            healthManager?.getAllStates() ?? new Map();

          const lines = config.peers.map((p) => {
            const state = peerStates.get(p.name);
            const health = state?.health ?? "unknown";
            const circuit = state?.circuit ?? "unknown";
            const authType = p.auth ? p.auth.type : "none";
            return (
              `• ${p.name}\n` +
              `  Endpoint: ${p.agentCardUrl}\n` +
              `  Auth: ${authType}\n` +
              `  Health: ${health} | Circuit: ${circuit}`
            );
          });

          const text = `A2A Peers (${config.peers.length}):\n\n${lines.join("\n\n")}`;
          return {
            content: [{ type: "text" as const, text }],
            details: { peers: config.peers.map((p) => ({ name: p.name, agentCardUrl: p.agentCardUrl })) },
          };
        },
      });
    }

    if (!api.registerService) {
      api.logger.warn("a2a-gateway: registerService is unavailable; HTTP endpoints are not started");
      return;
    }

    api.registerService({
      id: "a2a-gateway",
      async start(_ctx) {
        if (server) {
          return;
        }

        // Start peer health checks
        healthManager?.start();

        // Start HTTP server (JSON-RPC + REST)
        await new Promise<void>((resolve, reject) => {
          server = app.listen(config.server.port, config.server.host, () => {
            api.logger.info(
              `a2a-gateway: HTTP listening on ${config.server.host}:${config.server.port}`
            );
            api.logger.info(
              `a2a-gateway: durable task store at ${config.storage.tasksDir}; concurrency=${config.limits.maxConcurrentTasks}; queue=${config.limits.maxQueuedTasks}`
            );
            resolve();
          });

          server!.once("error", reject);
        });

        // Start gRPC server
        try {
          grpcServer = new GrpcServer();
          const grpcUserBuilder = async (
            call: { metadata?: { get: (key: string) => unknown[] } } | unknown,
          ) => {
            if (config.security.inboundAuth === "bearer" && config.security.validTokens.size > 0) {
              const meta = (call as any)?.metadata;
              const values = meta?.get?.("authorization") || meta?.get?.("Authorization") || [];
              const header = Array.isArray(values) && values.length > 0 ? String(values[0]) : "";
              const providedToken = header.startsWith("Bearer ") ? header.slice(7) : "";
              if (!providedToken || !config.security.validTokens.has(providedToken)) {
                telemetry.recordSecurityRejection("grpc", "invalid or missing bearer token");
                auditLogger.recordSecurityEvent("grpc", "invalid or missing bearer token");
                const err: any = new Error("Unauthorized: invalid or missing bearer token");
                err.code = GrpcStatus.UNAUTHENTICATED;
                throw err;
              }
            }
            return GrpcUserBuilder.noAuthentication();
          };

          grpcServer.addService(
            A2AService,
            grpcService({ requestHandler, userBuilder: grpcUserBuilder as any })
          );

          await new Promise<void>((resolve, reject) => {
            grpcServer!.bindAsync(
              `${config.server.host}:${grpcPort}`,
              ServerCredentials.createInsecure(),
              (error) => {
                if (error) {
                  api.logger.warn(`a2a-gateway: gRPC failed to start: ${error.message}`);
                  grpcServer = null;
                  resolve(); // Non-fatal: HTTP still works
                  return;
                }
                try {
                  grpcServer!.start();
                } catch {
                  // ignore: some grpc-js versions auto-start
                }
                api.logger.info(
                  `a2a-gateway: gRPC listening on ${config.server.host}:${grpcPort}`
                );
                resolve();
              }
            );
          });
        } catch (grpcError: unknown) {
          const msg = grpcError instanceof Error ? grpcError.message : String(grpcError);
          api.logger.warn(`a2a-gateway: gRPC init failed: ${msg}`);
          grpcServer = null;
        }

        // Start task TTL cleanup
        const ttlMs = config.storage.taskTtlHours * 3_600_000;
        const intervalMs = config.storage.cleanupIntervalMinutes * 60_000;

        const doCleanup = () => {
          void runTaskCleanup(taskStore, ttlMs, telemetry, api.logger);
        };

        // Run once at startup to clear any backlog
        doCleanup();
        cleanupTimer = setInterval(doCleanup, intervalMs);

        api.logger.info(
          `a2a-gateway: task cleanup enabled — ttl=${config.storage.taskTtlHours}h interval=${config.storage.cleanupIntervalMinutes}min`,
        );
      },
      async stop(_ctx) {
        // Stop federation
        federationGateway?.stop();

        // Stop peer health checks
        healthManager?.stop();
        auditLogger.close();

        // Stop task cleanup timer
        if (cleanupTimer) {
          clearInterval(cleanupTimer);
          cleanupTimer = null;
        }

        // Stop gRPC server
        if (grpcServer) {
          grpcServer.forceShutdown();
          grpcServer = null;
        }

        // Stop HTTP server
        if (!server) {
          return;
        }

        await new Promise<void>((resolve, reject) => {
          const activeServer = server!;
          server = null;
          activeServer.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
      },
    });
  },
};

export default plugin;
