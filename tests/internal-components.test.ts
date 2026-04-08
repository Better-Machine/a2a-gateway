/**
 * Tests for all internal modules (envelope, security, routing, outbox,
 * idempotency, metrics).
 */
import { describe, it, beforeEach } from "node:test";
import assert from "node:assert/strict";

// ---------------------------------------------------------------------------
// Envelope
// ---------------------------------------------------------------------------

import {
  createEnvelope,
  validateEnvelope,
  createAck,
  generateId,
  type EnvelopeError,
} from "../src/internal/envelope.js";
import type { A2AConfig, A2AEnvelope } from "../src/internal/types-internal.js";

describe("envelope", () => {
  const config: A2AConfig = {
    gatewayId: "gw-local",
    limits: { maxHops: 5, maxPayloadBytes: 2_097_152 },
  };

  describe("generateId", () => {
    it("returns unique ids", () => {
      const a = generateId();
      const b = generateId();
      assert.notEqual(a, b);
      assert.ok(a.includes("-"));
    });
  });

  describe("createEnvelope", () => {
    it("builds a valid envelope with defaults", () => {
      const env = createEnvelope({
        source: { gateway_id: "gw-a" },
        destination: { gateway_id: "gw-b" },
        message_type: "command",
        payload: { text: "hello" },
      });

      assert.equal(env.protocol_version, "a2a/v1");
      assert.ok(env.message_id);
      assert.ok(env.idempotency_key);
      assert.ok(env.timestamp);
      assert.equal(env.ttl_seconds, 300);
      assert.equal(env.hop_count, 0);
      assert.deepEqual(env.route_path, []);
      assert.equal(env.message_type, "command");
    });

    it("respects explicit values", () => {
      const env = createEnvelope({
        source: { gateway_id: "gw-a" },
        destination: { gateway_id: "gw-b" },
        message_type: "event",
        payload: "test",
        ttl_seconds: 60,
        hop_count: 2,
        route_path: ["gw-x"],
        correlation_id: "corr-1",
        idempotency_key: "idem-1",
      });

      assert.equal(env.ttl_seconds, 60);
      assert.equal(env.hop_count, 2);
      assert.deepEqual(env.route_path, ["gw-x"]);
      assert.equal(env.correlation_id, "corr-1");
      assert.equal(env.idempotency_key, "idem-1");
    });
  });

  describe("validateEnvelope", () => {
    function validEnvelope(): A2AEnvelope {
      return createEnvelope({
        source: { gateway_id: "gw-remote" },
        destination: { gateway_id: "gw-local" },
        message_type: "command",
        payload: "test",
      });
    }

    it("returns null for valid envelope", () => {
      const result = validateEnvelope(validEnvelope(), config);
      assert.equal(result, null);
    });

    it("rejects wrong protocol version", () => {
      const env = validEnvelope();
      env.protocol_version = "a2a/v2";
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "INVALID_VERSION");
    });

    it("rejects missing message_id", () => {
      const env = validEnvelope();
      env.message_id = "";
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });

    it("rejects missing source.gateway_id", () => {
      const env = validEnvelope();
      env.source.gateway_id = "";
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });

    it("rejects invalid message_type", () => {
      const env = validEnvelope();
      (env as Record<string, unknown>).message_type = "invalid_type";
      const err = validateEnvelope(env as A2AEnvelope, config);
      assert.equal(err!.code, "INVALID_TYPE");
    });

    it("rejects expired TTL", () => {
      const env = validEnvelope();
      env.timestamp = new Date(Date.now() - 600_000).toISOString();
      env.ttl_seconds = 1;
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "EXPIRED");
    });

    it("detects loop (own gateway in route_path)", () => {
      const env = validEnvelope();
      env.route_path = ["gw-local"];
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "LOOP_DETECTED");
    });

    it("rejects hop limit exceeded", () => {
      const env = validEnvelope();
      env.hop_count = 5; // equals maxHops
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "HOP_LIMIT");
    });

    it("rejects oversized payload", () => {
      const smallConfig: A2AConfig = {
        gatewayId: "gw-local",
        limits: { maxHops: 5, maxPayloadBytes: 10 },
      };
      const env = validEnvelope();
      env.payload = "x".repeat(100);
      const err = validateEnvelope(env, smallConfig);
      assert.equal(err!.code, "PAYLOAD_TOO_LARGE");
    });

    it("rejects invalid ttl_seconds (non-positive)", () => {
      const env = validEnvelope();
      env.ttl_seconds = 0;
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });

    it("rejects negative hop_count", () => {
      const env = validEnvelope();
      env.hop_count = -1;
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });

    it("rejects non-array route_path", () => {
      const env = validEnvelope();
      (env as Record<string, unknown>).route_path = "not-an-array";
      const err = validateEnvelope(env as A2AEnvelope, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });

    it("rejects invalid timestamp format", () => {
      const env = validEnvelope();
      env.timestamp = "not-a-date";
      const err = validateEnvelope(env, config);
      assert.equal(err!.code, "MISSING_FIELD");
    });
  });

  describe("createAck", () => {
    it("creates accepted ACK", () => {
      const env = createEnvelope({
        source: { gateway_id: "gw-a" },
        destination: { gateway_id: "gw-b" },
        message_type: "command",
        payload: "test",
      });

      const ack = createAck(env, "gw-b", "accepted");
      assert.equal(ack.protocol_version, "a2a/v1");
      assert.equal(ack.message_type, "ack");
      assert.equal(ack.status, "accepted");
      assert.equal(ack.correlation_id, env.message_id);
      assert.equal(ack.source.gateway_id, "gw-b");
      assert.equal(ack.reason, undefined);
    });

    it("creates rejected ACK with reason", () => {
      const env = createEnvelope({
        source: { gateway_id: "gw-a" },
        destination: { gateway_id: "gw-b" },
        message_type: "command",
        payload: "test",
      });

      const ack = createAck(env, "gw-b", "rejected", "No route found");
      assert.equal(ack.status, "rejected");
      assert.equal(ack.reason, "No route found");
    });
  });
});

// ---------------------------------------------------------------------------
// Security
// ---------------------------------------------------------------------------

import {
  signRequest,
  verifyRequest,
  checkTimestamp,
  NonceCache,
  PeerRegistry,
} from "../src/internal/security.js";

describe("security", () => {
  const SECRET = "test-secret-key-for-hmac-256!!!";

  describe("signRequest + verifyRequest roundtrip", () => {
    it("sign then verify succeeds", () => {
      const body = JSON.stringify({ message: "hello" });
      const { signature, timestamp, nonce } = signRequest(body, SECRET);
      const result = verifyRequest(body, signature, timestamp, nonce, SECRET);
      assert.equal(result.valid, true);
    });

    it("verify fails with wrong secret", () => {
      const body = "test body";
      const { signature, timestamp, nonce } = signRequest(body, SECRET);
      const result = verifyRequest(body, signature, timestamp, nonce, "wrong-secret");
      assert.equal(result.valid, false);
    });

    it("verify fails with tampered body", () => {
      const body = "original body";
      const { signature, timestamp, nonce } = signRequest(body, SECRET);
      const result = verifyRequest("tampered body", signature, timestamp, nonce, SECRET);
      assert.equal(result.valid, false);
    });

    it("verify fails with tampered signature", () => {
      const body = "test";
      const { timestamp, nonce } = signRequest(body, SECRET);
      const result = verifyRequest(body, "badsignature", timestamp, nonce, SECRET);
      assert.equal(result.valid, false);
    });
  });

  describe("checkTimestamp", () => {
    it("accepts timestamp within skew", () => {
      const result = checkTimestamp(Date.now(), 300);
      assert.equal(result.valid, true);
    });

    it("rejects timestamp outside skew", () => {
      const result = checkTimestamp(Date.now() - 600_000, 300);
      assert.equal(result.valid, false);
      assert.ok(result.error?.includes("exceeds limit"));
    });

    it("accepts timestamp slightly in the future", () => {
      const result = checkTimestamp(Date.now() + 1000, 300);
      assert.equal(result.valid, true);
    });
  });

  describe("NonceCache", () => {
    it("add returns true for new nonce", () => {
      const cache = new NonceCache(60_000);
      try {
        assert.equal(cache.add("nonce-1", 60_000), true);
      } finally {
        cache.destroy();
      }
    });

    it("add returns false for duplicate nonce (replay)", () => {
      const cache = new NonceCache(60_000);
      try {
        cache.add("nonce-1", 60_000);
        assert.equal(cache.add("nonce-1", 60_000), false);
      } finally {
        cache.destroy();
      }
    });

    it("allows reuse of expired nonce", () => {
      const cache = new NonceCache(60_000);
      try {
        cache.add("nonce-1", 1); // 1ms TTL
        // Wait for expiry
        const start = Date.now();
        while (Date.now() - start < 10) { /* busy wait */ }
        assert.equal(cache.add("nonce-1", 60_000), true);
      } finally {
        cache.destroy();
      }
    });

    it("cleanup removes expired entries", () => {
      const cache = new NonceCache(60_000);
      try {
        cache.add("nonce-1", 1); // 1ms TTL
        const start = Date.now();
        while (Date.now() - start < 10) { /* busy wait */ }
        cache.cleanup();
        // After cleanup, nonce should be gone
        assert.equal(cache.add("nonce-1", 60_000), true);
      } finally {
        cache.destroy();
      }
    });

    it("has returns correct status", () => {
      const cache = new NonceCache(60_000);
      try {
        assert.equal(cache.has("nonce-1"), false);
        cache.add("nonce-1", 60_000);
        assert.equal(cache.has("nonce-1"), true);
      } finally {
        cache.destroy();
      }
    });
  });

  describe("PeerRegistry", () => {
    it("addPeer and getPeer", () => {
      const registry = new PeerRegistry();
      registry.addPeer({ gatewayId: "gw-1", hmacSecret: "secret-1" });

      const peer = registry.getPeer("gw-1");
      assert.ok(peer);
      assert.equal(peer.gatewayId, "gw-1");
      assert.equal(peer.hmacSecret, "secret-1");
    });

    it("isKnown returns correct status", () => {
      const registry = new PeerRegistry();
      assert.equal(registry.isKnown("gw-1"), false);
      registry.addPeer({ gatewayId: "gw-1", hmacSecret: "secret-1" });
      assert.equal(registry.isKnown("gw-1"), true);
    });

    it("getSecret returns secret for known peer", () => {
      const registry = new PeerRegistry();
      registry.addPeer({ gatewayId: "gw-1", hmacSecret: "my-secret" });
      assert.equal(registry.getSecret("gw-1"), "my-secret");
    });

    it("getSecret returns undefined for unknown peer", () => {
      const registry = new PeerRegistry();
      assert.equal(registry.getSecret("gw-unknown"), undefined);
    });
  });
});

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

import { Router } from "../src/internal/routing.js";

describe("routing", () => {
  describe("Router", () => {
    it("resolves by direct agent_id", () => {
      const router = new Router([]);
      const result = router.route({ agent_id: "agent-x" });
      assert.ok(result);
      assert.equal(result.agentId, "agent-x");
      assert.equal(result.matched_by, "agent_id");
    });

    it("resolves by route_key", () => {
      const router = new Router([{ routeKey: "billing", agentId: "billing-agent" }]);
      const result = router.route({ route_key: "billing" });
      assert.ok(result);
      assert.equal(result.agentId, "billing-agent");
      assert.equal(result.matched_by, "route_key");
    });

    it("resolves by default agent", () => {
      const router = new Router([], "fallback-agent");
      const result = router.route({});
      assert.ok(result);
      assert.equal(result.agentId, "fallback-agent");
      assert.equal(result.matched_by, "default");
    });

    it("returns null when no route matches", () => {
      const router = new Router([]);
      const result = router.route({ route_key: "nonexistent" });
      assert.equal(result, null);
    });

    it("agent_id takes priority over route_key", () => {
      const router = new Router([{ routeKey: "billing", agentId: "billing-agent" }]);
      const result = router.route({ agent_id: "explicit", route_key: "billing" });
      assert.ok(result);
      assert.equal(result.agentId, "explicit");
      assert.equal(result.matched_by, "agent_id");
    });

    it("addRule and removeRule", () => {
      const router = new Router([]);
      router.addRule({ routeKey: "support", agentId: "support-agent" });
      assert.equal(router.getRules().length, 1);

      const result = router.route({ route_key: "support" });
      assert.ok(result);
      assert.equal(result.agentId, "support-agent");

      router.removeRule("support");
      assert.equal(router.getRules().length, 0);

      const noResult = router.route({ route_key: "support" });
      assert.equal(noResult, null);
    });
  });
});

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

import { Outbox } from "../src/internal/outbox.js";

describe("outbox", () => {
  it("enqueue adds entry", () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
    });

    const id = outbox.enqueue({} as any);
    assert.ok(id);

    const stats = outbox.getStats();
    assert.equal(stats.pending, 1);
  });

  it("getPending returns eligible entries", () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
    });

    outbox.enqueue({} as any);
    const pending = outbox.getPending();
    assert.equal(pending.length, 1);
    assert.equal(pending[0].status, "pending");
  });

  it("markSent changes status", () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
    });

    const id = outbox.enqueue({} as any);
    outbox.markSending(id);
    outbox.markSent(id);

    const stats = outbox.getStats();
    assert.equal(stats.sent, 1);
    assert.equal(stats.pending, 0);
  });

  it("markFailed applies backoff and eventually moves to dead", () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 2,
      baseDelayMs: 10,
      maxDelayMs: 100,
    });

    const id = outbox.enqueue({} as any);

    // First failure
    outbox.markFailed(id, "err1");
    let stats = outbox.getStats();
    assert.equal(stats.pending, 1); // back to pending with delay

    // Second failure → dead
    outbox.markFailed(id, "err2");
    stats = outbox.getStats();
    assert.equal(stats.dead, 1);
    assert.equal(stats.pending, 0);
  });

  it("getDeadLetters returns dead entries", () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 1,
      baseDelayMs: 10,
      maxDelayMs: 100,
    });

    const id = outbox.enqueue({} as any);
    outbox.markFailed(id, "err");

    const deadLetters = outbox.getDeadLetters();
    assert.equal(deadLetters.length, 1);
    assert.equal(deadLetters[0].last_error, "err");
  });

  it("cleanup removes old sent entries", async () => {
    const outbox = new Outbox({
      pollIntervalMs: 1000,
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 1000,
    });

    const id = outbox.enqueue({} as any);
    outbox.markSent(id);

    // Wait a bit so entry becomes old enough for cleanup
    await new Promise((r) => setTimeout(r, 20));
    outbox.cleanup(0);
    const stats = outbox.getStats();
    assert.equal(stats.sent, 0);
  });

  it("startRelay and stopRelay lifecycle", async () => {
    const outbox = new Outbox({
      pollIntervalMs: 50,
      maxRetries: 3,
      baseDelayMs: 10,
      maxDelayMs: 100,
    });

    outbox.enqueue({} as any);

    let sendCalled = false;
    outbox.startRelay(async () => {
      sendCalled = true;
      return true;
    });

    // Wait for at least one relay cycle
    await new Promise((r) => setTimeout(r, 150));
    outbox.stopRelay();

    assert.equal(sendCalled, true);
    const stats = outbox.getStats();
    assert.equal(stats.sent, 1);
  });
});

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

import { IdempotencyStore, createFingerprint } from "../src/internal/idempotency.js";

describe("idempotency", () => {
  describe("createFingerprint", () => {
    it("produces consistent fingerprint", () => {
      const fp1 = createFingerprint("hello");
      const fp2 = createFingerprint("hello");
      assert.equal(fp1, fp2);
    });

    it("produces different fingerprints for different inputs", () => {
      const fp1 = createFingerprint("hello");
      const fp2 = createFingerprint("world");
      assert.notEqual(fp1, fp2);
    });
  });

  describe("IdempotencyStore", () => {
    it("returns new for unseen key", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 3600 });
      const result = store.check("key-1", "fp-1");
      assert.equal(result.status, "new");
    });

    it("returns duplicate with cached response", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 3600 });
      store.store("key-1", "fp-1", '{"status":"ok"}');

      const result = store.check("key-1", "fp-1");
      assert.equal(result.status, "duplicate");
      if (result.status === "duplicate") {
        assert.equal(result.response, '{"status":"ok"}');
      }
    });

    it("returns conflict for same key different fingerprint", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 3600 });
      store.store("key-1", "fp-1", '{"status":"ok"}');

      const result = store.check("key-1", "fp-different");
      assert.equal(result.status, "conflict");
    });

    it("TTL expiry resets entry", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 0 }); // instant expiry
      store.store("key-1", "fp-1", "response");

      // Wait for expiry
      const start = Date.now();
      while (Date.now() - start < 10) { /* busy wait */ }

      const result = store.check("key-1", "fp-1");
      assert.equal(result.status, "new");
    });

    it("getStats returns correct counts", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 3600 });
      store.store("key-1", "fp-1", "response");

      const stats = store.getStats();
      assert.equal(stats.total, 1);
      assert.equal(stats.expired_cleaned, 0);
    });

    it("cleanup removes expired entries", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 0 });
      store.store("key-1", "fp-1", "response");

      const start = Date.now();
      while (Date.now() - start < 10) { /* busy wait */ }

      store.cleanup();
      const stats = store.getStats();
      assert.equal(stats.total, 0);
      assert.ok(stats.expired_cleaned > 0);
    });

    it("startCleanup and stopCleanup lifecycle", () => {
      const store = new IdempotencyStore({ defaultTtlSeconds: 3600 });
      store.startCleanup(100);
      store.stopCleanup();
      // Should not throw
    });
  });
});

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

import { A2AMetricsCollector } from "../src/internal/metrics.js";

describe("metrics", () => {
  let collector: A2AMetricsCollector;

  beforeEach(() => {
    collector = new A2AMetricsCollector();
  });

  it("recordSend increments counter and sets timestamp", () => {
    collector.recordSend();
    const m = collector.getMetrics();
    assert.equal(m.messages_sent, 1);
    assert.ok(m.last_send_at);
  });

  it("recordReceive increments counter", () => {
    collector.recordReceive();
    const m = collector.getMetrics();
    assert.equal(m.messages_received, 1);
    assert.ok(m.last_receive_at);
  });

  it("recordAck increments counter", () => {
    collector.recordAck();
    assert.equal(collector.getMetrics().acks_sent, 1);
  });

  it("recordRetry increments counter", () => {
    collector.recordRetry();
    assert.equal(collector.getMetrics().retries, 1);
  });

  it("recordIdempotentHit increments counter", () => {
    collector.recordIdempotentHit();
    assert.equal(collector.getMetrics().idempotent_hits, 1);
  });

  it("recordDeadLetter increments counter", () => {
    collector.recordDeadLetter();
    assert.equal(collector.getMetrics().dead_letters, 1);
  });

  it("recordError increments counter", () => {
    collector.recordError();
    assert.equal(collector.getMetrics().errors, 1);
  });

  it("recordSecurityRejection increments counter", () => {
    collector.recordSecurityRejection();
    assert.equal(collector.getMetrics().security_rejections, 1);
  });

  it("recordLoopRejection increments counter", () => {
    collector.recordLoopRejection();
    assert.equal(collector.getMetrics().loop_rejections, 1);
  });

  it("getMetrics returns a snapshot (not reference)", () => {
    collector.recordSend();
    const m1 = collector.getMetrics();
    collector.recordSend();
    const m2 = collector.getMetrics();
    assert.equal(m1.messages_sent, 1);
    assert.equal(m2.messages_sent, 2);
  });

  it("reset clears all counters", () => {
    collector.recordSend();
    collector.recordReceive();
    collector.recordError();
    collector.reset();

    const m = collector.getMetrics();
    assert.equal(m.messages_sent, 0);
    assert.equal(m.messages_received, 0);
    assert.equal(m.errors, 0);
    assert.equal(m.last_send_at, undefined);
    assert.equal(m.last_receive_at, undefined);
  });

  it("structuredLog outputs to console", () => {
    // Just verify it doesn't throw
    const originalLog = console.log;
    let logged = false;
    console.log = () => { logged = true; };
    try {
      collector.structuredLog("test_event", { message_id: "123" });
      assert.equal(logged, true);
    } finally {
      console.log = originalLog;
    }
  });
});
