/**
 * A2A Gateway — Federation Agent Dispatcher
 *
 * Dispatches federation messages to local OpenClaw agents via
 * the Gateway RPC WebSocket protocol (reusing GatewayRpcConnection).
 */

import { v4 as uuidv4 } from "uuid";
import type { FederationAgentDispatcher } from "./federation.js";

// ---------------------------------------------------------------------------
// Types mirrored from executor.ts (not exported there)
// ---------------------------------------------------------------------------

interface GatewayRuntimeConfig {
  port: number;
  wsUrl: string;
  hooksWakeUrl: string;
  gatewayToken: string;
  gatewayPassword: string;
  hooksToken: string;
}

interface PendingGatewayRequest {
  method: string;
  expectFinal: boolean;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface WebSocketConstructor {
  new (url: string): GatewayWebSocket;
}

interface GatewayWebSocket {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(type: string, listener: (event: { data?: unknown }) => void): void;
  removeEventListener(type: string, listener: (event: { data?: unknown }) => void): void;
}

// ---------------------------------------------------------------------------
// Minimal GatewayRpcConnection (duplicated subset from executor.ts)
// ---------------------------------------------------------------------------

const GATEWAY_CONNECT_TIMEOUT_MS = 10_000;
const GATEWAY_REQUEST_TIMEOUT_MS = 300_000;

function asObject(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") return null;
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

class MinimalGatewayRpc {
  private readonly wsUrl: string;
  private readonly gatewayToken: string;
  private readonly gatewayPassword: string;
  private readonly pending = new Map<string, PendingGatewayRequest>();
  private socket: GatewayWebSocket | null = null;
  private messageListener: ((event: { data?: unknown }) => void) | null = null;
  private closeListener: ((event: unknown) => void) | null = null;

  private connectChallengeTimer: ReturnType<typeof setTimeout> | null = null;
  private connectChallengeResolver: ((nonce: string) => void) | null = null;
  private connectChallengeRejecter: ((error: Error) => void) | null = null;

  constructor(config: { wsUrl: string; gatewayToken: string; gatewayPassword: string }) {
    this.wsUrl = config.wsUrl;
    this.gatewayToken = config.gatewayToken;
    this.gatewayPassword = config.gatewayPassword;
  }

  async connect(): Promise<void> {
    const ctor = (globalThis as unknown as { WebSocket?: WebSocketConstructor }).WebSocket;
    if (!ctor) throw new Error("WebSocket runtime is unavailable");

    const socket = new ctor(this.wsUrl);
    this.socket = socket;
    this.messageListener = (event) => this.handleMessage(event);
    this.closeListener = () => {
      const error = new Error("gateway connection closed");
      this.rejectConnectChallenge(error);
      this.rejectAllPending(error);
    };
    socket.addEventListener("message", this.messageListener);
    socket.addEventListener("close", this.closeListener);

    const challengePromise = this.awaitConnectChallenge();

    try {
      await new Promise<void>((resolve, reject) => {
        let settled = false;
        const settle = (error?: Error) => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          socket.removeEventListener("open", onOpen);
          socket.removeEventListener("error", onError);
          socket.removeEventListener("close", onClose);
          if (error) { this.rejectConnectChallenge(error); reject(error); return; }
          resolve();
        };
        const onOpen = () => settle();
        const onError = () => settle(new Error("failed to open gateway websocket"));
        const onClose = () => settle(new Error("gateway websocket closed during connect"));
        const timer = setTimeout(() => settle(new Error("gateway websocket connect timed out")), GATEWAY_CONNECT_TIMEOUT_MS);
        socket.addEventListener("open", onOpen);
        socket.addEventListener("error", onError);
        socket.addEventListener("close", onClose);
      });

      await challengePromise;
    } catch (error) {
      await challengePromise.catch(() => {});
      throw error;
    }

    // Send connect request
    const auth: Record<string, string> = {};
    if (this.gatewayToken) auth.token = this.gatewayToken;
    if (this.gatewayPassword) auth.password = this.gatewayPassword;

    const connectParams: Record<string, unknown> = {
      minProtocol: 3,
      maxProtocol: 3,
      client: {
        id: "cli",
        version: "a2a-gateway-federation",
        platform: process.platform,
        mode: "cli",
        instanceId: uuidv4(),
      },
      role: "operator",
      scopes: ["operator.admin", "operator.read", "operator.write", "operator.approvals"],
    };
    if (Object.keys(auth).length > 0) connectParams.auth = auth;

    await this.request("connect", connectParams, GATEWAY_CONNECT_TIMEOUT_MS, false);
  }

  async request(
    method: string,
    params: unknown,
    timeoutMs: number,
    expectFinal: boolean,
  ): Promise<unknown> {
    const socket = this.socket;
    if (!socket || socket.readyState !== 1) {
      throw new Error("gateway websocket is not connected");
    }

    const id = uuidv4();
    const frame = { type: "req" as const, id, method, params };

    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`gateway request timed out: ${method}`));
      }, timeoutMs);

      this.pending.set(id, { method, expectFinal, timer, resolve, reject });

      try {
        socket.send(JSON.stringify(frame));
      } catch (error: unknown) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    });
  }

  close(): void {
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      if (this.messageListener) socket.removeEventListener("message", this.messageListener);
      if (this.closeListener) socket.removeEventListener("close", this.closeListener);
      this.messageListener = null;
      this.closeListener = null;
      socket.close();
    }
    this.rejectAllPending(new Error("gateway websocket connection closed"));
  }

  private awaitConnectChallenge(): Promise<void> {
    this.clearConnectChallengeWait();
    return new Promise<void>((resolve, reject) => {
      this.connectChallengeResolver = (_nonce: string) => {
        this.clearConnectChallengeWait();
        resolve();
      };
      this.connectChallengeRejecter = (error) => {
        this.clearConnectChallengeWait();
        reject(error);
      };
      this.connectChallengeTimer = setTimeout(() => {
        this.connectChallengeRejecter?.(new Error("gateway connect challenge timed out"));
      }, 2_000);
    });
  }

  private clearConnectChallengeWait(): void {
    if (this.connectChallengeTimer) clearTimeout(this.connectChallengeTimer);
    this.connectChallengeTimer = null;
    this.connectChallengeResolver = null;
    this.connectChallengeRejecter = null;
  }

  private rejectConnectChallenge(error: Error): void {
    if (this.connectChallengeRejecter) {
      this.connectChallengeRejecter(error);
      return;
    }
    this.clearConnectChallengeWait();
  }

  private handleMessage(event: { data?: unknown }): void {
    const raw = typeof event.data === "string" ? event.data : "";
    if (!raw) return;

    let parsed: unknown;
    try { parsed = JSON.parse(raw); } catch { return; }

    const frame = asObject(parsed);
    if (!frame) return;

    if (frame.type === "event") {
      if (frame.event === "connect.challenge") {
        const payload = asObject(frame.payload);
        const nonce = asString(payload?.nonce)?.trim() || "";
        if (nonce && this.connectChallengeResolver) {
          this.connectChallengeResolver(nonce);
        }
      }
      return;
    }

    if (frame.type !== "res") return;
    const id = asString(frame.id);
    if (!id) return;

    const pending = this.pending.get(id);
    if (!pending) return;

    if (pending.expectFinal) {
      const payload = asObject(frame.payload);
      if (payload?.status === "accepted") return;
    }

    this.pending.delete(id);
    clearTimeout(pending.timer);

    if (frame.ok === true) {
      pending.resolve(frame.payload);
      return;
    }

    const errorBody = asObject(frame.error);
    const message = asString(errorBody?.message) || `gateway method failed: ${pending.method}`;
    pending.reject(new Error(message));
  }

  private rejectAllPending(error: Error): void {
    for (const [, pending] of this.pending) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
  }
}

// ---------------------------------------------------------------------------
// GatewayRpcDispatcher
// ---------------------------------------------------------------------------

interface DispatcherConfig {
  port: number;
  gatewayToken: string;
  gatewayPassword: string;
}

export class GatewayRpcDispatcher implements FederationAgentDispatcher {
  private readonly config: DispatcherConfig;

  constructor(config: DispatcherConfig) {
    this.config = config;
  }

  async dispatch(agentId: string, message: string, contextId: string): Promise<string> {
    const wsUrl = `ws://localhost:${this.config.port}`;
    const gateway = new MinimalGatewayRpc({
      wsUrl,
      gatewayToken: this.config.gatewayToken,
      gatewayPassword: this.config.gatewayPassword,
    });

    await gateway.connect();

    try {
      const sessionKey = `agent:${agentId}:a2a:${contextId}`;
      const runId = uuidv4();

      const finalPayload = await gateway.request(
        "agent",
        {
          agentId,
          message,
          deliver: false,
          idempotencyKey: runId,
          sessionKey,
        },
        GATEWAY_REQUEST_TIMEOUT_MS,
        true,
      );

      const body = asObject(finalPayload);
      const status = asString(body?.status);
      if (status && status !== "ok") {
        const summary = asString(body?.summary) || "Agent run did not complete";
        throw new Error(summary);
      }

      // Extract text from agent response payloads
      const result = asObject(body?.result);
      const payloads = Array.isArray(result?.payloads) ? result.payloads : [];
      const texts: string[] = [];
      for (const entry of payloads) {
        const obj = asObject(entry);
        if (obj && typeof obj.text === "string" && obj.text.trim()) {
          texts.push(obj.text.trim());
        }
      }

      if (texts.length > 0) {
        return texts.join("\n\n");
      }

      throw new Error("No assistant response text returned by gateway");
    } finally {
      gateway.close();
    }
  }

  static createFromPluginApi(api: { config: unknown }): GatewayRpcDispatcher {
    const config = asObject(api.config) || {};
    const gateway = asObject(config.gateway) || {};
    const gatewayAuth = asObject(gateway.auth) || {};

    const port = (typeof gateway.port === "number" && Number.isFinite(gateway.port))
      ? gateway.port
      : 18_789;

    return new GatewayRpcDispatcher({
      port,
      gatewayToken: asString(gatewayAuth.token) || "",
      gatewayPassword: asString(gatewayAuth.password) || "",
    });
  }
}
