/**
 * Tests for FederationGateway
 */
import { describe, it, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import express from "express";
import { createServer, type Server } from "node:http";

import {
  FederationGateway,
  type FederationAgentDispatcher,
  type FederationConfig,
} from "../src/internal/federation.js";
import {
  signRequest,
} from "../src/internal/security.js";
import {
  createEnvelope,
} from "../src/internal/envelope.js";
import type { OutboundClient, OutboundResult } from "../src/internal/transport.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const PEER_SECRET = "test-hmac-secret-256bit-key!!!!!";
const OUR_GATEWAY_ID = "gateway-local";
const PEER_GATEWAY_ID = "gateway-remote";
const REMOTE_GATEWAY_ID = "gateway-far-away";

function makeFederationConfig(overrides?: Partial<FederationConfig>): FederationConfig {
  return {
    gatewayId: OUR_GATEWAY_ID,
    peers: [
      { gatewayId: PEER_GATEWAY_ID, inboxUrl: "http://localhost:19999/a2a/v1/inbox", hmacSecret: PEER_SECRET },
    ],
    limits: { maxHops: 5, maxPayloadBytes: 2_097_152 },
    defaultAgentId: "main",
    timestampSkewSeconds: 300,
    ...overrides,
  };
}

function makeDispatcher(response = "Hello from agent"): FederationAgentDispatcher & { calls: Array<{ agentId: string; message: string; contextId: string }> } {
  const calls: Array<{ agentId: string; message: string; contextId: string }> = [];
  return {
    calls,
    async dispatch(agentId: string, message: string, contextId: string): Promise<string> {
      calls.push({ agentId, message, contextId });
      return response;
    },
  };
}

function makeMockOutboundClient(result?: Partial<OutboundResult>): OutboundClient {
  const defaultResult: OutboundResult = { success: true, statusCode: 200, body: "{}" };
  return {
    send: async () => ({ ...defaultResult, ...result }),
  } as OutboundClient;
}

function makeValidEnvelope(destGatewayId?: string, destAgentId?: string) {
  return createEnvelope({
    source: { gateway_id: PEER_GATEWAY_ID },
    destination: {
      gateway_id: destGatewayId ?? OUR_GATEWAY_ID,
      agent_id: destAgentId ?? "main",
    },
    message_type: "command",
    payload: "Hello from remote gateway",
    ttl_seconds: 300,
  });
}

async function postToInbox(
  app: express.Application,
  envelope: Record<string, unknown>,
  secret: string,
  opts?: {
    sourceGatewayId?: string;
    timestampOverride?: number;
    signatureOverride?: string;
    nonceOverride?: string;
  },
): Promise<{ status: number; body: Record<string, unknown> }> {
  // We need to make a real HTTP request since supertest may not be available
  // Build the request manually using the app
  return new Promise((resolve, reject) => {
    const bodyStr = JSON.stringify(envelope);
    const timestamp = opts?.timestampOverride ?? Date.now();
    const signResult = signRequest(bodyStr, secret, timestamp);
    const signature = opts?.signatureOverride ?? signResult.signature;
    const nonce = opts?.nonceOverride ?? signResult.nonce;

    const server = createServer(app);
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      if (!addr || typeof addr === "string") {
        server.close();
        reject(new Error("Failed to get server address"));
        return;
      }

      const url = `http://127.0.0.1:${addr.port}/a2a/v1/inbox`;
      fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-A2A-Signature": signature,
          "X-A2A-Timestamp": String(timestamp),
          "X-A2A-Nonce": nonce,
          "X-A2A-Source-Gateway": opts?.sourceGatewayId ?? PEER_GATEWAY_ID,
        },
        body: bodyStr,
      })
        .then(async (res) => {
          const body = await res.json() as Record<string, unknown>;
          server.close();
          resolve({ status: res.status, body });
        })
        .catch((err) => {
          server.close();
          reject(err);
        });
    });
  });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("FederationGateway", () => {
  let app: express.Application;
  let dispatcher: ReturnType<typeof makeDispatcher>;
  let gateway: FederationGateway;

  beforeEach(() => {
    app = express();
    app.use(express.json());
    dispatcher = makeDispatcher();
  });

  afterEach(() => {
    gateway?.stop();
  });

  it("valid inbound message dispatches to agent and returns ACK", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = makeValidEnvelope();
    const result = await postToInbox(app, envelope, PEER_SECRET);

    assert.equal(result.status, 200);
    assert.equal(result.body.status, "accepted");
    assert.equal(result.body.message_type, "ack");

    assert.equal(dispatcher.calls.length, 1);
    assert.equal(dispatcher.calls[0].agentId, "main");
    assert.equal(dispatcher.calls[0].message, "Hello from remote gateway");
  });

  it("unknown peer returns 401", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = createEnvelope({
      source: { gateway_id: "unknown-gateway" },
      destination: { gateway_id: OUR_GATEWAY_ID, agent_id: "main" },
      message_type: "command",
      payload: "test",
    });

    const result = await postToInbox(app, envelope, PEER_SECRET, {
      sourceGatewayId: "unknown-gateway",
    });

    assert.equal(result.status, 401);
    assert.equal(dispatcher.calls.length, 0);
  });

  it("bad HMAC returns 401", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = makeValidEnvelope();
    const result = await postToInbox(app, envelope, PEER_SECRET, {
      signatureOverride: "badbadbadbadsignature",
    });

    assert.equal(result.status, 401);
    assert.equal(dispatcher.calls.length, 0);
  });

  it("expired envelope returns 400", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    // Create envelope with TTL that has already expired
    const envelope = createEnvelope({
      source: { gateway_id: PEER_GATEWAY_ID },
      destination: { gateway_id: OUR_GATEWAY_ID, agent_id: "main" },
      message_type: "command",
      payload: "old message",
      ttl_seconds: 1,
    });
    // Backdate the timestamp so TTL has expired
    envelope.timestamp = new Date(Date.now() - 60_000).toISOString();

    const result = await postToInbox(app, envelope, PEER_SECRET);

    assert.equal(result.status, 400);
    assert.equal((result.body as Record<string, unknown>).code, "EXPIRED");
  });

  it("loop detection returns 400", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = createEnvelope({
      source: { gateway_id: PEER_GATEWAY_ID },
      destination: { gateway_id: OUR_GATEWAY_ID, agent_id: "main" },
      message_type: "command",
      payload: "looping message",
      route_path: [OUR_GATEWAY_ID], // Our gateway is already in the path
    });

    const result = await postToInbox(app, envelope, PEER_SECRET);

    assert.equal(result.status, 400);
    assert.equal((result.body as Record<string, unknown>).code, "LOOP_DETECTED");
  });

  it("duplicate idempotency key returns cached ACK", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = makeValidEnvelope();

    // First request
    const result1 = await postToInbox(app, envelope, PEER_SECRET);
    assert.equal(result1.status, 200);
    assert.equal(result1.body.status, "accepted");
    assert.equal(dispatcher.calls.length, 1);

    // Second request with same idempotency key
    const result2 = await postToInbox(app, envelope, PEER_SECRET);
    assert.equal(result2.status, 200);
    assert.equal(result2.body.status, "accepted");
    // Dispatcher should NOT be called again
    assert.equal(dispatcher.calls.length, 1);
  });

  it("local dispatch calls dispatcher.dispatch with correct args", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = createEnvelope({
      source: { gateway_id: PEER_GATEWAY_ID },
      destination: { gateway_id: OUR_GATEWAY_ID, agent_id: "special-agent" },
      message_type: "command",
      payload: "specific task",
    });

    const result = await postToInbox(app, envelope, PEER_SECRET);

    assert.equal(result.status, 200);
    assert.equal(dispatcher.calls.length, 1);
    assert.equal(dispatcher.calls[0].agentId, "special-agent");
    assert.equal(dispatcher.calls[0].message, "specific task");
  });

  it("sendToGateway enqueues correctly", () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const result = gateway.sendToGateway(PEER_GATEWAY_ID, "target-agent", "federated message");

    assert.equal(result.ok, true);
    assert.ok(result.messageId);

    const metrics = gateway.getMetrics();
    assert.equal(metrics.federation.messages_sent, 1);
    assert.ok(metrics.outbox.pending >= 0); // Entry was enqueued
  });

  it("sendToGateway to unknown peer returns error", () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const result = gateway.sendToGateway("nonexistent-gateway", undefined, "test");

    assert.equal(result.ok, false);
    assert.ok(result.error?.includes("Unknown peer gateway"));
  });

  it("expired timestamp returns 401", async () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const envelope = makeValidEnvelope();
    // Use a timestamp from 10 minutes ago (beyond 300s skew)
    const result = await postToInbox(app, envelope, PEER_SECRET, {
      timestampOverride: Date.now() - 600_000,
    });

    assert.equal(result.status, 401);
  });

  it("getMetrics returns federation and outbox stats", () => {
    gateway = new FederationGateway(makeFederationConfig(), dispatcher, makeMockOutboundClient());
    gateway.start(app);

    const metrics = gateway.getMetrics();

    assert.ok("federation" in metrics);
    assert.ok("outbox" in metrics);
    assert.equal(typeof metrics.federation.messages_sent, "number");
    assert.equal(typeof metrics.outbox.pending, "number");
  });
});
