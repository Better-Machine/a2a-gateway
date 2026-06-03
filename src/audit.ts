import fs from "node:fs";
import path from "node:path";
import readline from "node:readline";

export type AuditDirection = "inbound" | "outbound";
export type AuditEventType = "task" | "security";

export interface AuditEntry {
  ts: string;
  direction: AuditDirection;
  type: AuditEventType;
  taskId?: string;
  contextId?: string;
  peer?: string;
  status: string;
  statusCode?: number;
  durationMs?: number;
  detail?: string;
}

/**
 * Append-only JSONL audit logger.
 * Writes one JSON line per A2A call event to a dedicated audit file,
 * separate from the application's structured logs.
 *
 * Includes lightweight per-peer failure tracking for self-healing alerts.
 */
export class AuditLogger {
  private readonly filePath: string;
  private dirEnsured = false;

  /** In-memory failure counter per peer. Key: peer name. */
  private peerFailures: Map<string, { consecutive: number; lastError: string; lastAt: number }> = new Map();
  /** Threshold for CRITICAL alert on consecutive failures. */
  private alertThreshold = 3;

  constructor(filePath: string, alertThreshold?: number) {
    this.filePath = filePath;
    if (alertThreshold !== undefined && alertThreshold > 0) {
      this.alertThreshold = alertThreshold;
    }
  }

  private ensureDir(): void {
    if (this.dirEnsured) return;
    this.dirEnsured = true;
    fs.mkdirSync(path.dirname(this.filePath), { recursive: true });
  }

  recordInbound(
    taskId: string,
    contextId: string,
    status: string,
    durationMs: number,
    peer?: string,
  ): void {
    this.write({
      ts: new Date().toISOString(),
      direction: "inbound",
      type: "task",
      taskId,
      contextId,
      peer,
      status,
      durationMs,
    });
    // Track per-peer failures for inbound
    if (peer && status === "failed") {
      this.trackPeerFailure(peer, `inbound task ${taskId} failed`);
    }
  }

  recordOutbound(
    peer: string,
    ok: boolean,
    statusCode: number,
    durationMs: number,
  ): void {
    this.write({
      ts: new Date().toISOString(),
      direction: "outbound",
      type: "task",
      peer,
      status: ok ? "success" : "failure",
      statusCode,
      durationMs,
    });
    if (!ok) {
      this.trackPeerFailure(peer, `outbound ${statusCode || "no status"}`);
    } else {
      this.resetPeerFailure(peer);
    }
  }

  recordSecurityEvent(surface: string, reason: string): void {
    this.write({
      ts: new Date().toISOString(),
      direction: "inbound",
      type: "security",
      status: "rejected",
      detail: `${surface}: ${reason}`,
    });
  }

  /**
   * Read the last N entries from the audit log.
   * Returns entries in reverse chronological order (newest first).
   */
  async tail(count: number = 50): Promise<AuditEntry[]> {
    if (!fs.existsSync(this.filePath)) return [];

    const entries: AuditEntry[] = [];
    const input = fs.createReadStream(this.filePath, { encoding: "utf-8" });
    const rl = readline.createInterface({ input, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line.trim()) continue;
      try {
        entries.push(JSON.parse(line));
      } catch {
        // skip malformed lines
      }
    }

    // Return last N in reverse order
    return entries.slice(-count).reverse();
  }

  close(): void {
    // No-op — appendFileSync has no persistent handles to close
  }

  /**
   * Track a failure for a peer. If consecutive failures exceed threshold,
   * emit a CRITICAL alert and return true.
   */
  private trackPeerFailure(peer: string, error: string): boolean {
    const existing = this.peerFailures.get(peer);
    if (existing) {
      existing.consecutive++;
      existing.lastError = error;
      existing.lastAt = Date.now();
    } else {
      this.peerFailures.set(peer, { consecutive: 1, lastError: error, lastAt: Date.now() });
    }
    const current = this.peerFailures.get(peer)!;
    if (current.consecutive >= this.alertThreshold) {
      this.write({
        ts: new Date().toISOString(),
        direction: "inbound",
        type: "security",
        status: "CRITICAL",
        detail: `Peer ${peer}: ${current.consecutive} consecutive failures — ${error}. Consider clearing session cache.`,
      });
      return true;
    }
    return false;
  }

  /** Reset failure counter for a peer after a successful message. */
  private resetPeerFailure(peer: string): void {
    this.peerFailures.delete(peer);
  }

  /** Get current failure state for a peer. */
  getPeerFailureState(peer: string): { consecutive: number; lastError: string; lastAt: number } | null {
    return this.peerFailures.get(peer) || null;
  }

  private write(entry: AuditEntry): void {
    try {
      this.ensureDir();
      fs.appendFileSync(this.filePath, JSON.stringify(entry) + "\n");
    } catch {
      // Swallow write errors — audit must not crash the gateway
    }
  }
}
