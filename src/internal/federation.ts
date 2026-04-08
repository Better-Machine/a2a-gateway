/**
 * A2A Gateway — Federation Layer
 *
 * Wires together all internal modules (envelope, security, transport,
 * routing, outbox, idempotency, metrics) into a single FederationGateway
 * that mounts on an existing Express app.
 */

import type { Application, Request, Response } from "express";
import type { A2AEnvelope, A2AMetrics } from "./types-internal.js";
import {
  createEnvelope,
  validateEnvelope,
  createAck,
} from "./envelope.js";
import {
  signRequest,
  verifyRequest,
  checkTimestamp,
  NonceCache,
  PeerRegistry,
} from "./security.js";
import { OutboundClient } from "./transport.js";
import type { OutboundResult } from "./transport.js";
import { Router } from "./routing.js";
import { Outbox } from "./outbox.js";
import type { OutboxEntry } from "./types-internal.js";
import { IdempotencyStore, createFingerprint } from "./idempotency.js";
import { A2AMetricsCollector } from "./metrics.js";

// ---------------------------------------------------------------------------
// Public interfaces
// ---------------------------------------------------------------------------

export interface FederationAgentDispatcher {
  dispatch(agentId: string, message: string, contextId: string): Promise<string>;
}

export interface FederationPeerConfig {
  gatewayId: string;
  inboxUrl: string;
  hmacSecret: string;
}

export interface FederationConfig {
  gatewayId: string;
  peers: FederationPeerConfig[];
  limits: { maxHops: number; maxPayloadBytes: number };
  defaultAgentId: string;
  timestampSkewSeconds: number;
}

// ---------------------------------------------------------------------------
// FederationGateway
// ---------------------------------------------------------------------------

const NONCE_TTL_MS = 600_000; // 10 minutes
const OUTBOX_POLL_MS = 5_000;
const OUTBOX_MAX_RETRIES = 5;
const OUTBOX_BASE_DELAY_MS = 1_000;
const OUTBOX_MAX_DELAY_MS = 30_000;
const IDEMPOTENCY_TTL_SECONDS = 3600;

export class FederationGateway {
  private readonly config: FederationConfig;
  private readonly dispatcher: FederationAgentDispatcher;
  private readonly peerRegistry: PeerRegistry;
  private readonly nonceCache: NonceCache;
  private readonly router: Router;
  private readonly outbox: Outbox;
  private readonly idempotencyStore: IdempotencyStore;
  private readonly metrics: A2AMetricsCollector;
  private readonly outboundClient: OutboundClient;

  /** Peer inboxUrl indexed by gatewayId for outbox relay. */
  private readonly peerUrlMap: Map<string, string>;
  /** Peer HMAC secrets indexed by gatewayId for outbox relay signing. */
  private readonly peerSecretMap: Map<string, string>;

  constructor(
    config: FederationConfig,
    dispatcher: FederationAgentDispatcher,
    outboundClient?: OutboundClient,
  ) {
    this.config = config;
    this.dispatcher = dispatcher;
    this.outboundClient = outboundClient ?? new OutboundClient();

    this.peerRegistry = new PeerRegistry();
    this.peerUrlMap = new Map();
    this.peerSecretMap = new Map();
    for (const peer of config.peers) {
      this.peerRegistry.addPeer({ gatewayId: peer.gatewayId, hmacSecret: peer.hmacSecret });
      this.peerUrlMap.set(peer.gatewayId, peer.inboxUrl);
      this.peerSecretMap.set(peer.gatewayId, peer.hmacSecret);
    }

    this.nonceCache = new NonceCache(60_000);
    this.router = new Router([], config.defaultAgentId);
    this.outbox = new Outbox({
      pollIntervalMs: OUTBOX_POLL_MS,
      maxRetries: OUTBOX_MAX_RETRIES,
      baseDelayMs: OUTBOX_BASE_DELAY_MS,
      maxDelayMs: OUTBOX_MAX_DELAY_MS,
    });
    this.idempotencyStore = new IdempotencyStore({ defaultTtlSeconds: IDEMPOTENCY_TTL_SECONDS });
    this.metrics = new A2AMetricsCollector();
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /** Mount POST /a2a/v1/inbox on the express app and start outbox relay. */
  start(app: Application): void {
    // Mount with express.json() middleware for body parsing
    app.post("/a2a/v1/inbox", (req: Request, res: Response) => {
      this.handleInbound(req, res).catch(() => {
        if (!res.headersSent) {
          res.status(500).json({ error: "Internal server error" });
        }
      });
    });

    // Start outbox relay
    this.outbox.startRelay((entry: OutboxEntry) => this.relaySend(entry));

    // Start idempotency cleanup
    this.idempotencyStore.startCleanup(60_000);
  }

  /** Stop outbox relay, nonce cache cleanup, idempotency cleanup. */
  stop(): void {
    this.outbox.stopRelay();
    this.nonceCache.destroy();
    this.idempotencyStore.stopCleanup();
  }

  // -----------------------------------------------------------------------
  // Outbound: enqueue a message for delivery
  // -----------------------------------------------------------------------

  sendToGateway(
    destGatewayId: string,
    destAgentId: string | undefined,
    message: string,
    opts?: { correlationId?: string; ttlSeconds?: number },
  ): { ok: boolean; messageId?: string; error?: string } {
    if (!this.peerUrlMap.has(destGatewayId)) {
      return { ok: false, error: `Unknown peer gateway: ${destGatewayId}` };
    }

    const envelope = createEnvelope({
      source: { gateway_id: this.config.gatewayId },
      destination: {
        gateway_id: destGatewayId,
        agent_id: destAgentId,
      },
      message_type: "command",
      payload: message,
      ttl_seconds: opts?.ttlSeconds ?? 300,
      correlation_id: opts?.correlationId,
    });

    const entryId = this.outbox.enqueue({
      envelope,
      destGatewayId,
    } as OutboxEntry);

    this.metrics.recordSend();
    return { ok: true, messageId: envelope.message_id };
  }

  // -----------------------------------------------------------------------
  // Metrics
  // -----------------------------------------------------------------------

  getMetrics(): { federation: A2AMetrics; outbox: Record<string, number> } {
    return {
      federation: this.metrics.getMetrics(),
      outbox: this.outbox.getStats(),
    };
  }

  // -----------------------------------------------------------------------
  // Inbound handler
  // -----------------------------------------------------------------------

  private async handleInbound(req: Request, res: Response): Promise<void> {
    // 1. Extract headers
    const signature = this.headerString(req, "x-a2a-signature");
    const timestampStr = this.headerString(req, "x-a2a-timestamp");
    const timestamp = Number(timestampStr) || 0;
    const nonce = this.headerString(req, "x-a2a-nonce");

    // Parse body — express.json() may or may not have been applied upstream
    let envelope: A2AEnvelope;
    if (req.body && typeof req.body === "object" && req.body.protocol_version) {
      envelope = req.body as A2AEnvelope;
    } else {
      res.status(400).json({ error: "Invalid request body" });
      return;
    }

    const sourceGatewayId = envelope.source?.gateway_id ?? "";

    // 2. Peer lookup
    if (!sourceGatewayId || !this.peerRegistry.isKnown(sourceGatewayId)) {
      this.metrics.recordSecurityRejection();
      res.status(401).json({ error: "Unknown peer gateway" });
      return;
    }

    const secret = this.peerRegistry.getSecret(sourceGatewayId)!;

    // 3. Timestamp check
    const tsCheck = checkTimestamp(timestamp, this.config.timestampSkewSeconds);
    if (!tsCheck.valid) {
      this.metrics.recordSecurityRejection();
      res.status(401).json({ error: tsCheck.error });
      return;
    }

    // 4. HMAC verification — use JSON.stringify(req.body) as canonical body
    const rawBody = JSON.stringify(req.body);
    const verifyResult = verifyRequest(rawBody, signature, timestamp, nonce, secret);
    if (!verifyResult.valid) {
      this.metrics.recordSecurityRejection();
      res.status(401).json({ error: verifyResult.error });
      return;
    }

    // 5. Nonce replay check
    if (!this.nonceCache.add(nonce, NONCE_TTL_MS)) {
      res.status(409).json({ error: "Replay detected: duplicate nonce" });
      return;
    }

    // 6. Envelope validation
    const a2aConfig = {
      gatewayId: this.config.gatewayId,
      limits: this.config.limits,
    };
    const validationError = validateEnvelope(envelope, a2aConfig);
    if (validationError) {
      if (validationError.code === "LOOP_DETECTED") {
        this.metrics.recordLoopRejection();
      }
      res.status(400).json({ error: validationError.message, code: validationError.code });
      return;
    }

    // 7. Idempotency check
    const fingerprint = createFingerprint(JSON.stringify(envelope.payload));
    const idempotencyResult = this.idempotencyStore.check(envelope.idempotency_key, fingerprint);
    if (idempotencyResult.status === "duplicate") {
      this.metrics.recordIdempotentHit();
      res.status(200).json(JSON.parse(idempotencyResult.response));
      return;
    }
    if (idempotencyResult.status === "conflict") {
      res.status(409).json({ error: "Idempotency key conflict: different payload" });
      return;
    }

    this.metrics.recordReceive();

    // 8. Route: local or remote?
    const destGatewayId = envelope.destination?.gateway_id;
    const isLocal = !destGatewayId || destGatewayId === this.config.gatewayId;

    let ack;
    if (isLocal) {
      // Local dispatch
      const routeResult = this.router.route(envelope.destination);
      if (!routeResult) {
        ack = createAck(envelope, this.config.gatewayId, "rejected", "No route found");
        const ackJson = JSON.stringify(ack);
        this.idempotencyStore.store(envelope.idempotency_key, fingerprint, ackJson);
        this.metrics.recordAck();
        res.status(200).json(ack);
        return;
      }

      try {
        const contextId = envelope.correlation_id || envelope.message_id;
        const payloadStr = typeof envelope.payload === "string"
          ? envelope.payload
          : JSON.stringify(envelope.payload);
        const response = await this.dispatcher.dispatch(routeResult.agentId, payloadStr, contextId);

        ack = createAck(envelope, this.config.gatewayId, "accepted");
        (ack as Record<string, unknown>).response = response;
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : String(err);
        ack = createAck(envelope, this.config.gatewayId, "rejected", msg);
      }
    } else {
      // Remote forwarding
      if (!this.peerUrlMap.has(destGatewayId)) {
        ack = createAck(envelope, this.config.gatewayId, "rejected", `Unknown destination gateway: ${destGatewayId}`);
        const ackJson = JSON.stringify(ack);
        this.idempotencyStore.store(envelope.idempotency_key, fingerprint, ackJson);
        this.metrics.recordAck();
        res.status(200).json(ack);
        return;
      }

      // Build forwarded envelope
      const forwarded = createEnvelope({
        source: envelope.source,
        destination: envelope.destination,
        message_type: envelope.message_type,
        payload: envelope.payload,
        ttl_seconds: envelope.ttl_seconds,
        correlation_id: envelope.correlation_id,
        trace_id: envelope.trace_id,
        span_id: envelope.span_id,
        idempotency_key: envelope.idempotency_key,
        hop_count: envelope.hop_count + 1,
        route_path: [...envelope.route_path, this.config.gatewayId],
      });

      const peerUrl = this.peerUrlMap.get(destGatewayId)!;
      const peerSecret = this.peerSecretMap.get(destGatewayId)!;
      const signResult = signRequest(JSON.stringify(forwarded), peerSecret);

      const sendResult = await this.outboundClient.send(
        peerUrl,
        forwarded,
        signResult.signature,
        signResult.timestamp,
        signResult.nonce,
        this.config.gatewayId,
      );

      if (sendResult.success) {
        ack = createAck(envelope, this.config.gatewayId, "accepted");
      } else {
        // Enqueue in outbox for retry
        this.outbox.enqueue({
          envelope: forwarded,
          destGatewayId,
        } as OutboxEntry);
        ack = createAck(envelope, this.config.gatewayId, "accepted");
        (ack as Record<string, unknown>).queued = true;
      }
    }

    const ackJson = JSON.stringify(ack);
    this.idempotencyStore.store(envelope.idempotency_key, fingerprint, ackJson);
    this.metrics.recordAck();
    res.status(200).json(ack);
  }

  // -----------------------------------------------------------------------
  // Outbox relay
  // -----------------------------------------------------------------------

  private async relaySend(entry: OutboxEntry): Promise<boolean> {
    const data = entry as OutboxEntry & { envelope: A2AEnvelope; destGatewayId: string };
    const { envelope, destGatewayId } = data;

    const peerUrl = this.peerUrlMap.get(destGatewayId);
    const peerSecret = this.peerSecretMap.get(destGatewayId);
    if (!peerUrl || !peerSecret) {
      return false;
    }

    const signResult = signRequest(JSON.stringify(envelope), peerSecret);
    const result: OutboundResult = await this.outboundClient.send(
      peerUrl,
      envelope,
      signResult.signature,
      signResult.timestamp,
      signResult.nonce,
      this.config.gatewayId,
    );

    if (!result.success) {
      this.metrics.recordRetry();
    }

    return result.success;
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private headerString(req: Request, name: string): string {
    const value = req.headers[name];
    if (Array.isArray(value)) return value[0] ?? "";
    return value ?? "";
  }
}
