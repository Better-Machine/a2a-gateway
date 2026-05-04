/**
 * A2A Busy Signal v0.2.0 Implementation
 *
 * Provides capacity-aware task routing with self-reported BUSY state
 * and peer-observed UNREACHABLE state.
 *
 * @see SPEC.md for full protocol specification
 */

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type BusyState = "DORMANT" | "READY" | "BUSY" | "UNREACHABLE";

export interface BusyStateResponse {
  state: BusyState;
  reportedAt: string; // ISO-8601 timestamp
  ttlMs: number;
  capacity: {
    currentTasks: number;
    maxTasks: number;
  };
  retryAfterMs?: number; // Only present when state=BUSY
}

export interface PeerBusyState {
  state: BusyState;
  reportedAt: number; // timestamp ms
  ttlMs: number;
  capacity: {
    currentTasks: number;
    maxTasks: number;
  };
  retryAfterMs?: number;
}

export interface BusySignalConfig {
  maxTasks: number;
  ttlDefaultMs: number;
}

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_TTL_MS = 30000; // 30 seconds

// Capacity defaults per spec
const DEFAULT_CAPACITIES: Record<string, number> = {
  "Woodhouse": 3,
  "Ray": 1,
  "Liz": 5,
};

// ---------------------------------------------------------------------------
// Local Busy State Manager
// ---------------------------------------------------------------------------

/**
 * Manages this gateway's busy state.
 * Tracks current task count and transitions between READY/BUSY states.
 */
export class LocalBusyStateManager {
  private state: BusyState = "DORMANT";
  private currentTasks = 0;
  private maxTasks: number;
  private ttlDefaultMs: number;
  private lastReportedAt: number = 0;

  constructor(config: BusySignalConfig) {
    this.maxTasks = config.maxTasks;
    this.ttlDefaultMs = config.ttlDefaultMs;
  }

  /**
   * Increment task counter and update state if needed.
   */
  acquireTask(): void {
    if (this.state === "DORMANT") {
      this.state = "READY";
    }
    this.currentTasks++;
    this.lastReportedAt = Date.now();
    
    if (this.currentTasks >= this.maxTasks && this.state !== "BUSY") {
      this.state = "BUSY";
    }
  }

  /**
   * Decrement task counter and update state if needed.
   */
  releaseTask(): void {
    this.currentTasks = Math.max(0, this.currentTasks - 1);
    this.lastReportedAt = Date.now();
    
    if (this.currentTasks < this.maxTasks && this.state === "BUSY") {
      this.state = "READY";
    }
  }

  /**
   * Get current busy state response.
   */
  getState(): BusyStateResponse {
    const now = Date.now();
    this.lastReportedAt = now;
    
    const response: BusyStateResponse = {
      state: this.state,
      reportedAt: new Date(now).toISOString(),
      ttlMs: this.ttlDefaultMs,
      capacity: {
        currentTasks: this.currentTasks,
        maxTasks: this.maxTasks,
      },
    };
    
    if (this.state === "BUSY") {
      // Estimate retry time based on average task duration
      // For now, use a conservative 5 seconds
      response.retryAfterMs = 5000;
    }
    
    return response;
  }

  /**
   * Check if we can accept a new task.
   */
  canAcceptTask(): boolean {
    return this.state !== "DORMANT" && this.state !== "BUSY";
  }

  /**
   * Get the BUSY rejection response data.
   */
  getBusyRejectionData(): { state: string; retryAfterMs: number; capacity: { currentTasks: number; maxTasks: number } } {
    const stateResponse = this.getState();
    return {
      state: stateResponse.state,
      retryAfterMs: stateResponse.retryAfterMs || 5000,
      capacity: stateResponse.capacity,
    };
  }

  /**
   * Set state to READY (e.g., after restart).
   */
  setReady(): void {
    this.state = "READY";
    this.lastReportedAt = Date.now();
  }

  /**
   * Get current task count.
   */
  getCurrentTasks(): number {
    return this.currentTasks;
  }

  /**
   * Get max task capacity.
   */
  getMaxTasks(): number {
    return this.maxTasks;
  }
}

// ---------------------------------------------------------------------------
// Peer Busy State Cache
// ---------------------------------------------------------------------------

/**
 * Caches peer busy states with TTL-based expiry.
 * Peers MUST NOT trust state beyond reportedAt + ttlMs.
 */
export class PeerBusyStateCache {
  private cache = new Map<string, PeerBusyState>();

  /**
   * Update cached state for a peer.
   */
  update(peerName: string, state: PeerBusyState): void {
    this.cache.set(peerName, {
      ...state,
      reportedAt: Date.now(), // Use local timestamp for consistency
    });
  }

  /**
   * Get cached state for a peer, or null if expired/unknown.
   * Returns UNREACHABLE if TTL expired.
   */
  get(peerName: string): BusyState {
    const entry = this.cache.get(peerName);
    if (!entry) {
      return "UNREACHABLE";
    }

    const now = Date.now();
    const expiresAt = entry.reportedAt + entry.ttlMs;
    
    if (now > expiresAt) {
      // TTL expired - transition to UNREACHABLE
      return "UNREACHABLE";
    }

    return entry.state;
  }

  /**
   * Check if a peer is available (not BUSY or UNREACHABLE).
   */
  isAvailable(peerName: string): boolean {
    const state = this.get(peerName);
    return state === "READY" || state === "DORMANT";
  }

  /**
   * Get retryAfterMs for a BUSY peer.
   */
  getRetryAfterMs(peerName: string): number | undefined {
    const entry = this.cache.get(peerName);
    if (!entry || entry.state !== "BUSY") {
      return undefined;
    }

    // Check if still within TTL
    const now = Date.now();
    if (now > entry.reportedAt + entry.ttlMs) {
      return undefined;
    }

    return entry.retryAfterMs;
  }

  /**
   * Remove a peer from cache (e.g., on configuration change).
   */
  remove(peerName: string): void {
    this.cache.delete(peerName);
  }

  /**
   * Clear all cached states.
   */
  clear(): void {
    this.cache.clear();
  }
}

// ---------------------------------------------------------------------------
// Helper Functions
// ---------------------------------------------------------------------------

/**
 * Get default capacity for a peer by name.
 */
export function getDefaultCapacity(peerName: string): number {
  return DEFAULT_CAPACITIES[peerName] || 3; // Default to 3 if unknown
}

/**
 * Build agent card capability object for busy signal.
 */
export function buildBusySignalCapability(ttlDefaultMs: number = DEFAULT_TTL_MS): Record<string, unknown> {
  return {
    busySignal: {
      version: "0.2.0",
      states: ["DORMANT", "READY", "BUSY", "UNREACHABLE"],
      ttlDefaultMs,
    },
  };
}

/**
 * Error codes per spec.
 */
export const BUSY_ERROR_CODE = -32002;
export const DORMANT_ERROR_CODE = -32001;

/**
 * Build BUSY error response.
 */
export function buildBusyError(data: { state: string; retryAfterMs: number; capacity: { currentTasks: number; maxTasks: number } }): { code: number; message: string; data: typeof data } {
  return {
    code: BUSY_ERROR_CODE,
    message: "Agent busy",
    data,
  };
}

/**
 * Build DORMANT error response.
 */
export function buildDormantError(): { code: number; message: string } {
  return {
    code: DORMANT_ERROR_CODE,
    message: "Agent not initialized",
  };
}
