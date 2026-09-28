import { Inject, OnModuleDestroy, Optional } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { OnGatewayConnection, OnGatewayDisconnect, WebSocketGateway } from "@nestjs/websockets";
import type { IncomingMessage } from "node:http";
import { WebSocket } from "ws";
import { IntentsService } from "./intents.service";
import { SolversService } from "../solvers/solvers.service";
import { MetricsService } from "../metrics/metrics.service";
import { logger } from "../common/logger";
import { SUPPORTED_CHAINS, SupportedChain } from "./intents.types";
import { verifyStellarSignature, buildWsAuthMessage } from "../common/stellar-signature";
import { buildMatchPredicate, IntentCapabilityIndex, SolverMatchPredicate } from "./solver-intent-matcher";
import {
  WS_MAX_FILTER_CHAINS,
  WS_MAX_SUBSCRIPTIONS_PER_CONNECTION,
} from "../config/limits.config";
import configuration, { AppConfig } from "../config/configuration";
import { verifyHs256Jwt } from "../common/jwt";
import { Backplane, SequencedEvent, WS_BACKPLANE } from "./backplane/backplane.types";
import { MemoryBackplane } from "./backplane/memory.backplane";
import { ConnectionState, resolveClientIp } from "./ws/connection-state";

export type { SequencedEvent } from "./backplane/backplane.types";

const HEARTBEAT_INTERVAL_MS = 30_000;

/**
 * How many sequenced events to keep in the replay buffer.
 *
 * At typical broadcast volume (a few dozen events/minute in production),
 * 500 events covers many minutes of missed events — more than enough to
 * bridge a transient network blip or container restart without forcing a
 * full snapshot re-fetch. Increasing this beyond ~1 000 starts to add
 * non-trivial heap pressure for large event payloads; the current bound
 * is a deliberate memory vs. reconnect-gap tradeoff.
 */
const REPLAY_BUFFER_SIZE = 500;

/**
 * Per-subscriber filter (issue #436).
 *
 * `chains`   — explicit chain subscription set (`null` = unfiltered full feed).
 * `solver`   — capability predicate compiled from the authenticated solver's
 *              SolverRecord.  Non-null only for connections that have completed
 *              the `auth` handshake.
 * `wantAll`  — when `true` (sent via `{ type: "subscribe", all: true }`), the
 *              solver opts out of capability filtering and receives the full
 *              feed regardless of its chain/token support — useful for
 *              analytics consumers.
 */
interface SubscriberFilter {
  chains: Set<SupportedChain> | null;
  /** Compiled solver capability predicate (null = not authenticated). */
  solver: SolverMatchPredicate | null;
  /** Opt-out flag: receives all events even after authentication. */
  wantAll: boolean;
  /** Number of `subscribe` messages this connection has sent. */
  subscriptionCount: number;
}

/**
 * Fixed-size ring buffer that retains the last `capacity` events so
 * reconnecting clients can request a replay from a known sequence number.
 */
export class EventRingBuffer {
  private readonly buf: SequencedEvent[] = [];
  private readonly capacity: number;

  constructor(capacity = REPLAY_BUFFER_SIZE) {
    this.capacity = capacity;
  }

  push(event: SequencedEvent): void {
    if (this.buf.length >= this.capacity) {
      this.buf.shift();
    }
    this.buf.push(event);
  }

  /**
   * Return all buffered events whose seq is strictly greater than `fromSeq`.
   * Returns an empty array when `fromSeq` is older than the earliest buffered
   * event (the caller should request a fresh snapshot instead).
   */
  since(fromSeq: number): SequencedEvent[] {
    return this.buf.filter((e) => e.seq > fromSeq);
  }

  /** Lowest seq still in the buffer, or -1 when empty. */
  oldestSeq(): number {
    return this.buf.length === 0 ? -1 : this.buf[0].seq;
  }

  /** Highest seq in the buffer, or 0 when empty. */
  latestSeq(): number {
    return this.buf.length === 0 ? 0 : this.buf[this.buf.length - 1].seq;
  }

  size(): number {
    return this.buf.length;
  }
}

/**
 * Authentication / access-control decision (issue #49, updated #436)
 * ─────────────────────────────────────────────────────────────────────
 * The intent feed is intentionally PUBLIC and READ-ONLY for all clients.
 *
 * Solver bots that authenticate via `{ type: "auth", ... }` receive an
 * *auto-scoped* feed: only intents matching their supported chains / tokens
 * and with a non-zero bond requirement are delivered.  This reduces noise and
 * bandwidth as the solver set grows (O(solvers × intents) → O(solvers × matching-intents)).
 *
 * Opt-out: `{ type: "subscribe", all: true }` returns the full unfiltered feed
 * regardless of authentication — designed for analytics / monitoring consumers.
 *
 * Solver bots submit intents and accept/fill them through the authenticated
 * REST API. The WS gateway never accepts writes.
 */
// maxPayload is enforced by `ws` itself (close 1009). Read from the
// environment because decorator options are evaluated at import time;
// handleMessage re-checks against the validated config.
@WebSocketGateway({ path: "/ws", maxPayload: configuration().ws.maxPayloadBytes })
export class IntentsGateway
  implements OnGatewayConnection, OnGatewayDisconnect, OnModuleDestroy
{
  /**
   * Map from WebSocket client to its per-connection subscription filter.
   */
  private readonly subscribers = new Map<WebSocket, SubscriberFilter>();
  private readonly alive = new WeakMap<WebSocket, boolean>();
  private readonly authenticatedSolver = new WeakMap<WebSocket, string>();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private heartbeatTimer: any;

  /** Per-connection identity, rate-limit and outbound-queue state (issue #455). */
  private readonly connections = new Map<WebSocket, ConnectionState>();
  private readonly connectionsPerIp = new Map<string, number>();
  private readonly wsConfig: AppConfig["ws"];
  private readonly maxConnections: number;
  private readonly jwtSecret: string;

  /** Fan-out + global sequencing (issue #454): memory or Redis Streams. */
  private readonly backplane: Backplane;
  /** Serialises local delivery so events reach clients in `seq` order. */
  private deliveryChain: Promise<void> = Promise.resolve();

  /** Ring buffer storing the last REPLAY_BUFFER_SIZE broadcast events. */
  private readonly ringBuffer = new EventRingBuffer(REPLAY_BUFFER_SIZE);

  constructor(
    private readonly intentsService: IntentsService,
    private readonly solversService: SolversService,
    private readonly intentIndex: IntentCapabilityIndex,
    @Optional() private readonly metricsService?: MetricsService,
    @Optional() config?: ConfigService<AppConfig, true>,
    @Optional() @Inject(WS_BACKPLANE) backplane?: Backplane,
  ) {
    const defaults = configuration();
    this.wsConfig = config?.get("ws", { infer: true }) ?? defaults.ws;
    this.maxConnections = config?.get("wsMaxConnections", { infer: true }) ?? defaults.wsMaxConnections;
    this.jwtSecret = config?.get("authJwtSecret", { infer: true }) ?? defaults.authJwtSecret;
    this.heartbeatTimer = setInterval(() => this.heartbeat(), HEARTBEAT_INTERVAL_MS);
    this.backplane = backplane ?? new MemoryBackplane();
    // Every replica, including this one, receives its own broadcasts back
    // through the backplane, so all replicas share one sequence and order.
    void this.backplane.start((event) => this.enqueueDelivery(event)).catch((err: Error) =>
      logger.error(`ws backplane failed to start: ${err.message}`),
    );
    logger.info(`ws heartbeat started (backplane=${this.backplane.mode})`);
  }

  /** Backplane health for /health (issue #454). */
  backplaneHealth() {
    return this.backplane.health();
  }

  private static isSupportedChain(value: unknown): value is SupportedChain {
    return typeof value === "string" && (SUPPORTED_CHAINS as readonly string[]).includes(value);
  }

  /**
   * Deliver a pre-serialised event payload to every matching subscriber.
   *
   * Delivery rules (evaluated in order):
   * 1. Client is not OPEN → skip.
   * 2. Client set wantAll=true → always deliver.
   * 3. Client has a solver capability predicate:
   *    a. Event carries an inlined intent → apply predicate to that intent.
   *    b. Event is a state-transition (only intentId available) → deliver
   *       (we cannot efficiently look up the intent here; the solver would
   *       already have received the intent_created event through the filter).
   * 4. Client has a plain chain filter (`chains != null`) → apply chain match.
   * 5. No filter → full unfiltered feed (backward-compatible default).
   */
  private deliverToMatchingSubscribers(
    payload: string,
    chain: SupportedChain | null,
    event: { type: string; [key: string]: unknown },
  ) {
    for (const [client, filter] of this.subscribers) {
      if (client.readyState !== WebSocket.OPEN) continue;

      // Opt-out: solver requested full feed.
      if (filter.wantAll) {
        this.send(client, payload);
        continue;
      }

      // Authenticated solver — apply capability predicate.
      if (filter.solver !== null) {
        const solverPredicate = filter.solver;
        const inlinedIntent = (event as { intent?: unknown }).intent;

        // intent_created carries a full intent object we can test directly.
        if (event.type === "intent_created" && inlinedIntent && typeof inlinedIntent === "object") {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          const matches = solverPredicate.matches(inlinedIntent as any);
          if (matches) {
            this.send(client, payload);
            try { this.metricsService?.incWsDelivered(solverPredicate.solverAddress); } catch { /* noop */ }
          } else {
            try { this.metricsService?.incWsFiltered(solverPredicate.solverAddress); } catch { /* noop */ }
          }
          continue;
        }

        // State-transition events: the solver already filtered on intent_created,
        // so we pass them through to keep the feed self-consistent.
        this.send(client, payload);
        try { this.metricsService?.incWsDelivered(solverPredicate.solverAddress); } catch { /* noop */ }
        continue;
      }

      // No filter set → full unfiltered feed (backward-compatible default).
      if (filter.chains === null) {
        this.send(client, payload);
        continue;
      }

      // Chain couldn't be resolved → deliver to everyone (safe default).
      if (chain === null) {
        this.send(client, payload);
        continue;
      }

      // Only send if the event's chain is in this subscriber's filter.
      if (filter.chains.has(chain)) {
        this.send(client, payload);
      }
    }
  }

  /**
   * Admits a connection (issue #455): enforces WS_MAX_CONNECTIONS and the
   * per-IP limit (IP resolved through WS_TRUST_PROXY_HOPS), creates the
   * per-connection state, and accepts an optional solver JWT from
   * `?token=` or `Authorization: Bearer` (anonymous connections stay allowed).
   */
  handleConnection(client: WebSocket, request?: IncomingMessage) {
    const ip = resolveClientIp(
      request?.socket?.remoteAddress,
      request?.headers?.["x-forwarded-for"],
      this.wsConfig.trustProxyHops,
    );
    const perIp = this.connectionsPerIp.get(ip) ?? 0;
    const reject =
      this.maxConnections > 0 && this.connections.size >= this.maxConnections
        ? "max_connections"
        : this.wsConfig.maxConnectionsPerIp > 0 && perIp >= this.wsConfig.maxConnectionsPerIp
          ? "per_ip"
          : null;
    if (reject) {
      this.metricsService?.wsConnectionsRejected.inc({ reason: reject });
      client.close(1013, reject === "per_ip" ? "Too many connections from this IP" : "Server at capacity");
      return;
    }

    this.connections.set(
      client,
      new ConnectionState(
        client,
        ip,
        { perSec: this.wsConfig.rateLimitPerSec, burst: this.wsConfig.rateLimitBurst },
        {
          queueMax: this.wsConfig.outboundQueueMax,
          bufferBytes: this.wsConfig.outboundBufferBytes,
          policy: this.wsConfig.slowConsumerPolicy,
        },
      ),
    );
    this.connectionsPerIp.set(ip, perIp + 1);
    this.subscribers.set(client, { chains: null, solver: null, wantAll: false, subscriptionCount: 0 });
    this.alive.set(client, true);
    this.metricsService?.incWsConnection();

    client.on("message", (raw) => {
      void this.handleMessage(client, raw);
    });

    client.on("pong", () => {
      this.alive.set(client, true);
    });

    client.on("error", () => {
      this.removeSubscriber(client);
      logger.debug(
        `ws client error/drop — active subscribers: ${this.subscribers.size}`,
      );
    });

    const currentSeq = this.backplane.health().lastSeq;

    this.send(
      client,
      JSON.stringify({
        type: "connected",
        message: "Vortex intent stream",
        seq: currentSeq,
      }),
    );

    // Send the initial snapshot asynchronously — the client receives it
    // immediately after the "connected" message.
    Promise.resolve(this.intentsService.getByState("open"))
      .then((open) => {
        this.send(client, JSON.stringify({ type: "snapshot", intents: open.slice(0, 20), seq: currentSeq }));
      })
      .catch(() => {
        /* snapshot failure is non-fatal — client can re-fetch via REST */
      });

    const token = IntentsGateway.bearerToken(request);
    if (token) void this.authenticateJwt(client, token);

    logger.info(`ws client connected (subscribers=${this.subscribers.size})`);
  }

  /** JWT from `?token=` or `Authorization: Bearer` on the upgrade request. */
  private static bearerToken(request?: IncomingMessage): string | null {
    const auth = request?.headers?.authorization;
    if (auth?.startsWith("Bearer ")) return auth.slice(7).trim();
    try {
      return new URL(request?.url ?? "", "http://localhost").searchParams.get("token");
    } catch {
      return null;
    }
  }

  /**
   * Queues `payload` for `client` through its backpressure-aware state
   * (issue #455), counting slow-consumer drops and disconnects.
   */
  private send(client: WebSocket, payload: string): void {
    const state = this.connections.get(client);
    if (!state) {
      if (client.readyState === WebSocket.OPEN) client.send(payload);
      return;
    }
    const result = state.send(payload);
    if (result === "dropped_oldest") {
      this.metricsService?.wsOutboundDropped.inc();
    } else if (result === "disconnected") {
      this.metricsService?.wsSlowConsumerDisconnects.inc();
      logger.warn(`ws slow consumer disconnected (ip=${state.ip}, queue full)`);
      this.removeSubscriber(client);
    }
  }

  handleDisconnect(client: WebSocket) {
    this.removeSubscriber(client);
    logger.info(`ws client disconnected (subscribers=${this.subscribers.size})`);
  }

  /**
   * Drop a client from the subscriber set and keep the connection gauge honest.
   *
   * Every path that removes a client goes through here — explicit disconnect,
   * a transport-level `error`, and the heartbeat terminator — because they are
   * mutually exclusive in practice but not in the platform: a socket that
   * errors frequently never reaches `handleDisconnect`, and one that dies
   * silently is only reaped by the heartbeat. Removing a client from two
   * places with a bare `subscribers.delete` would leak
   * `vortex_ws_connections_active` upwards until the process restarts, and a
   * gauge that only ever climbs turns the WS panels into decoration.
   *
   * The gauge is decremented only when this call actually removed something, so
   * a duplicate disconnect cannot drive it negative.
   */
  private removeSubscriber(client: WebSocket): void {
    const state = this.connections.get(client);
    if (state) {
      state.close();
      this.connections.delete(client);
      const remaining = (this.connectionsPerIp.get(state.ip) ?? 1) - 1;
      if (remaining > 0) this.connectionsPerIp.set(state.ip, remaining);
      else this.connectionsPerIp.delete(state.ip);
    }
    const removed = this.subscribers.delete(client);
    this.authenticatedSolver.delete(client);
    this.alive.delete(client);
    if (removed) this.metricsService?.decWsConnection();
  }

  /**
   * Handle a single incoming WebSocket message from a client.
   *
   * Supported message types:
   * - `{ type: "subscribe", chains?: string[], all?: boolean }` — set a
   *   per-connection filter or opt out of capability filtering with `all: true`.
   * - `{ type: "replay", fromSeq: number }` — replay buffered events.
   * - `{ type: "auth", solver, timestamp, signature }` — authenticate as a
   *   registered solver; installs a capability predicate and sends an
   *   auto-scoped snapshot of currently-eligible open intents.
   *
   * Unknown types and malformed messages are silently ignored.
   */
  private async handleMessage(client: WebSocket, raw: import("ws").RawData): Promise<void> {
    // Frames already in flight when we closed the socket are ignored.
    if (client.readyState !== WebSocket.OPEN) return;
    const size = Array.isArray(raw)
      ? raw.reduce((n, b) => n + b.length, 0)
      : (raw as Buffer | ArrayBuffer).byteLength;
    if (size > this.wsConfig.maxPayloadBytes) {
      client.close(1009, "Message too big");
      return;
    }

    // Inbound token bucket (issue #455): over-limit messages are refused with
    // `rate_limited`; persistent offenders are disconnected.
    const state = this.connections.get(client);
    if (state && !state.bucket.take()) {
      state.violations += 1;
      if (state.violations >= this.wsConfig.rateLimitMaxViolations) {
        this.metricsService?.wsRateLimited.inc({ action: "disconnected" });
        client.close(1008, "Rate limit exceeded");
        return;
      }
      this.metricsService?.wsRateLimited.inc({ action: "rejected" });
      this.send(client, JSON.stringify({ type: "rate_limited", retryAfterMs: Math.ceil(1000 / this.wsConfig.rateLimitPerSec) }));
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(raw.toString());
    } catch {
      return;
    }

    if (typeof parsed !== "object" || parsed === null) return;

    const msg = parsed as Record<string, unknown>;

    switch (msg.type) {
      case "subscribe":
        this.handleSubscribe(client, msg);
        break;
      case "replay":
        this.handleReplay(client, msg);
        break;
      case "auth":
        await this.handleAuth(client, msg);
        break;
      default:
        break;
    }
  }

  /**
   * Process a `{ type: "subscribe", chains?: string[], all?: boolean }` message.
   *
   * When `all: true` is present, the connection opts out of capability filtering
   * and receives the complete unfiltered feed regardless of solver auth status.
   *
   * When `chains` is present, a per-connection chain filter is installed (this
   * clears any existing solver capability predicate on the connection).
   * Validates each chain value against `SUPPORTED_CHAINS` and stores only
   * the valid subset. A subscribe message with no valid chains is treated as
   * "subscribe to nothing" (the client will receive only chainless events).
   * An entirely missing or non-array `chains` field is rejected silently
   * without updating the existing filter.
   *
   * Issue #476: enforces two per-connection limits:
   * 1. The `chains` array may contain at most `WS_MAX_FILTER_CHAINS` values.
   * 2. A connection may send at most `WS_MAX_SUBSCRIPTIONS_PER_CONNECTION`
   *    subscribe messages in its lifetime.  Excess subscribe attempts are
   *    rejected with a `subscribe_rejected` error frame.
   */
  private handleSubscribe(client: WebSocket, msg: Record<string, unknown>): void {
    // all=true: opt out of capability filtering.
    if (msg.all === true) {
      const existing = this.subscribers.get(client) ?? { chains: null, solver: null, wantAll: false, subscriptionCount: 0 };
      this.subscribers.set(client, { ...existing, wantAll: true });
      logger.debug("ws client opted out of capability filtering (all=true)");
      if (client.readyState === WebSocket.OPEN) {
        this.send(client, JSON.stringify({ type: "subscribed", filter: { all: true } }));
      }
      return;
    }

    if (!Array.isArray(msg.chains)) {
      logger.debug("ws subscribe ignored: chains field missing or not an array");
      return;
    }

    const filter = this.subscribers.get(client);
    if (!filter) return;

    // ── Limit 1: max subscriptions per connection (issue #476) ───────────────
    const maxSubs = parseInt(
      process.env.WS_MAX_SUBSCRIPTIONS ?? String(WS_MAX_SUBSCRIPTIONS_PER_CONNECTION),
      10,
    );
    if (filter.subscriptionCount >= maxSubs) {
      logger.warn(
        `ws subscribe_rejected: connection has reached the max subscription limit (${maxSubs})`,
      );
      if (client.readyState === WebSocket.OPEN) {
        this.send(client, 
          JSON.stringify({
            type: "subscribe_rejected",
            reason: `Maximum subscription limit of ${maxSubs} reached for this connection`,
          }),
        );
      }
      return;
    }

    // ── Limit 2: max chain-filter values per subscribe message (issue #476) ──
    const maxChains = parseInt(
      process.env.WS_MAX_FILTER_CHAINS ?? String(WS_MAX_FILTER_CHAINS),
      10,
    );
    const rawChains = msg.chains as unknown[];
    if (rawChains.length > maxChains) {
      logger.warn(
        `ws subscribe_rejected: chains array length ${rawChains.length} exceeds max ${maxChains}`,
      );
      if (client.readyState === WebSocket.OPEN) {
        this.send(client, 
          JSON.stringify({
            type: "subscribe_rejected",
            reason: `chains array may contain at most ${maxChains} values`,
          }),
        );
      }
      return;
    }

    const validChains = rawChains.filter(
      (c): c is SupportedChain =>
        typeof c === "string" && (SUPPORTED_CHAINS as readonly string[]).includes(c),
    );

    // An explicit chain filter replaces any solver capability predicate.
    this.subscribers.set(client, {
      chains: new Set(validChains),
      solver: null,
      wantAll: false,
      subscriptionCount: filter.subscriptionCount + 1,
    });

    logger.debug(`ws client subscribed to chains: ${validChains.join(", ") || "(none)"}`);

    if (client.readyState === WebSocket.OPEN) {
      this.send(client, 
        JSON.stringify({
          type: "subscribed",
          filter: { chains: validChains },
        }),
      );
    }
  }

  /**
   * Process a `{ type: "replay", fromSeq: number }` message.
   */
  private handleReplay(client: WebSocket, msg: Record<string, unknown>): void {
    const fromSeq = typeof msg.fromSeq === "number" ? msg.fromSeq : null;
    if (fromSeq === null || !Number.isInteger(fromSeq) || fromSeq < 0) {
      logger.debug("ws replay ignored: fromSeq missing or invalid");
      return;
    }

    if (client.readyState !== WebSocket.OPEN) return;

    const oldest = this.ringBuffer.oldestSeq();

    if (oldest !== -1 && fromSeq < oldest - 1) {
      this.send(client, 
        JSON.stringify({
          type: "replay_too_old",
          fromSeq,
          oldestAvailableSeq: oldest,
        }),
      );
      logger.debug(`ws replay_too_old: fromSeq=${fromSeq} oldestAvailable=${oldest}`);
      return;
    }

    const events = this.ringBuffer.since(fromSeq);

    this.send(client, 
      JSON.stringify({
        type: "replay_start",
        fromSeq,
        count: events.length,
      }),
    );

    for (const event of events) {
      if (client.readyState !== WebSocket.OPEN) break;
      this.send(client, JSON.stringify(event));
    }

    if (client.readyState === WebSocket.OPEN) {
      this.send(client, 
        JSON.stringify({
          type: "replay_end",
          count: events.length,
        }),
      );
    }

    logger.debug(`ws replay complete: fromSeq=${fromSeq} count=${events.length}`);
  }

  /**
   * Authenticate a solver connection and install a capability predicate.
   *
   * On success:
   * 1. Compiles a per-solver match predicate from the solver's SolverRecord.
   * 2. Installs it on the subscriber filter so future broadcasts are scoped.
   * 3. Sends an `auth_ok` frame.
   * 4. Immediately sends a scoped `eligible_snapshot` with currently-eligible
   *    open intents from the in-memory index — so the solver doesn't need to
   *    separately call GET /solvers/:address/eligible-intents after auth.
   *
   * Capability updates (e.g. bond changes ingested via event-ingestion) call
   * `updateSolverPredicate()` directly — no reconnect required.
   */
  private async handleAuth(client: WebSocket, payload: Record<string, unknown>) {
    if (typeof payload.token === "string") {
      await this.authenticateJwt(client, payload.token);
      return;
    }
    const solver = typeof payload.solver === "string" ? payload.solver : "";
    const timestamp = payload.timestamp;
    const signature = typeof payload.signature === "string" ? payload.signature : "";

    if (!solver || !signature || typeof timestamp !== "number") {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "auth payload requires solver, timestamp, and signature" }));
      return;
    }

    const now = Math.floor(Date.now() / 1000);
    const skew = Math.abs(now - timestamp);
    if (skew > 300) {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "stale or future auth timestamp" }));
      return;
    }

    const solverRecord = await this.solversService.get(solver);
    if (!solverRecord || !solverRecord.isActive) {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "solver not registered or inactive" }));
      return;
    }

    try {
      verifyStellarSignature(solver, buildWsAuthMessage(solver, timestamp), signature);
    } catch {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "invalid solver signature" }));
      return;
    }

    await this.installSolver(client, solverRecord, "signature");
  }

  /**
   * Authenticates with a solver JWT from the SEP-10 flow (issue #455 / #442).
   * `sub` is the solver address; the solver must be registered and active.
   * An invalid token leaves the connection anonymous with an `auth_error`.
   */
  private async authenticateJwt(client: WebSocket, token: string): Promise<void> {
    const claims = verifyHs256Jwt(token, this.jwtSecret);
    if (!claims) {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "invalid or expired token" }));
      return;
    }
    const solverRecord = await this.solversService.get(claims.sub);
    if (!solverRecord || !solverRecord.isActive) {
      this.send(client, JSON.stringify({ type: "auth_error", reason: "solver not registered or inactive" }));
      return;
    }
    await this.installSolver(client, solverRecord, "jwt");
  }

  /** Binds a verified solver identity to the connection and sends its scoped snapshot. */
  private async installSolver(
    client: WebSocket,
    solverRecord: NonNullable<Awaited<ReturnType<SolversService["get"]>>>,
    method: "signature" | "jwt",
  ): Promise<void> {
    const solver = solverRecord.address;
    const predicate = buildMatchPredicate(solverRecord);
    this.authenticatedSolver.set(client, solver);
    const state = this.connections.get(client);
    if (state) state.identity = solver;
    const existing = this.subscribers.get(client);
    this.subscribers.set(client, { chains: null, solver: predicate, wantAll: false, subscriptionCount: existing?.subscriptionCount ?? 0 });

    this.send(client, JSON.stringify({ type: "auth_ok", method }));

    // Send scoped snapshot of currently-eligible intents (issue #436).
    try {
      const eligible = this.intentIndex.getEligibleFor(solverRecord);
      this.send(client, JSON.stringify({
        type: "eligible_snapshot",
        intents: eligible,
        count: eligible.length,
      }));
    } catch {
      // Non-fatal — solver can fall back to GET /solvers/:address/eligible-intents.
    }

    logger.info(`ws solver auth ok: address=${solver} chains=${solverRecord.supportedChains.join(",")} tokens=${solverRecord.supportedTokens.join(",")}`);
  }

  /**
   * Update the capability predicate for all live connections authenticated as
   * the given solver address.
   *
   * Called by EventIngestionService when a BondDeposited / BondWithdrawn /
   * SolverRegistered event updates a solver's capabilities — no reconnect needed.
   */
  async updateSolverPredicate(solverAddress: string): Promise<void> {
    const solverRecord = await this.solversService.get(solverAddress);
    if (!solverRecord) return;

    const predicate = buildMatchPredicate(solverRecord);
    for (const [client, filter] of this.subscribers) {
      if (this.authenticatedSolver.get(client) === solverAddress && filter.solver !== null) {
        this.subscribers.set(client, { ...filter, solver: predicate });
      }
    }

    logger.debug(`ws solver predicate updated for ${solverAddress}`);
  }

  /**
   * Resolve the source chain for an event payload.
   */
  private async getEventChain(
    event: { type: string; [key: string]: unknown },
  ): Promise<SupportedChain | null> {
    if (event.type === "intent_created") {
      const intent = event.intent as { srcChain?: string } | undefined;
      const chain = intent?.srcChain;
      if (chain && (SUPPORTED_CHAINS as readonly string[]).includes(chain)) {
        return chain as SupportedChain;
      }
      return null;
    }

    const lookupTypes = new Set([
      "intent_accepted",
      "intent_filled",
      "intent_cancelled",
      "intent_expired",
      "intent_slashed",
      "intent_slash_cancelled",
    ]);

    if (lookupTypes.has(event.type)) {
      const intentId = typeof event.intentId === "string" ? event.intentId : null;
      if (!intentId) return null;

      try {
        const intent = await this.intentsService.get(intentId);
        if (intent && (SUPPORTED_CHAINS as readonly string[]).includes(intent.srcChain)) {
          return intent.srcChain as SupportedChain;
        }
      } catch {
        // Lookup failure is non-fatal — deliver to all subscribers.
      }
      return null;
    }

    return null;
  }

  /**
   * Broadcast an event to every client on every replica (issue #454).
   *
   * The backplane assigns the global sequence number and hands the event
   * back to each replica's {@link deliver}. In memory mode this resolves after
   * local delivery (unchanged behaviour); in redis mode it resolves once the
   * event is queued, so request handlers never wait on Redis.
   */
  async broadcast(event: { type: string; [key: string]: unknown }): Promise<void> {
    await this.backplane.publish(event);
  }

  /** Chains deliveries so async chain lookups cannot reorder events. */
  private enqueueDelivery(event: SequencedEvent): Promise<void> {
    const run = this.deliveryChain.then(() => this.deliver(event));
    this.deliveryChain = run.catch((err: Error) => {
      logger.error(`ws delivery failed: ${err.message}`);
    });
    return this.deliveryChain;
  }

  /**
   * Push a sequenced event into the replay buffer, then deliver it to every
   * subscriber whose filter matches.
   *
   * For authenticated solvers without `all=true`, only intents matching their
   * capability predicate are delivered.  State-transition events (no inlined
   * intent) are always delivered to authenticated subscribers.
   *
   * Side-effects:
   * - Updates the intent index for `intent_created` (add) and terminal-state
   *   events (remove), keeping the capability index fresh without a rebuild.
   */
  private async deliver(sequencedEvent: SequencedEvent): Promise<void> {
    const enqueuedAt = Date.now();
    const { seq, ...event } = sequencedEvent;

    // Update the capability index before delivery so a racing replay or
    // eligible-intents call sees fresh state.
    this.updateIndexForEvent(sequencedEvent);

    // Push into replay buffer before sending.
    this.ringBuffer.push(sequencedEvent);

    logger.debug(`ws broadcast type=${event.type} seq=${seq} subscribers=${this.subscribers.size}`);

    // Resolve the chain once — shared across all subscriber checks.
    const eventChain = await this.getEventChain(sequencedEvent);

    const payload = JSON.stringify(sequencedEvent);
    this.deliverToMatchingSubscribers(payload, eventChain, sequencedEvent);

    try {
      this.metricsService?.observeWsDelivery((Date.now() - enqueuedAt) / 1000);
    } catch {
      // Metrics must never break broadcasts.
    }
  }

  /** Keep the IntentCapabilityIndex in sync with broadcast events. */
  private updateIndexForEvent(event: { type: string; [key: string]: unknown }): void {
    try {
      if (event.type === "intent_created") {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const intent = (event as any).intent;
        if (intent) this.intentIndex.addIntent(intent);
      } else if (
        event.type === "intent_accepted" ||
        event.type === "intent_filled" ||
        event.type === "intent_cancelled" ||
        event.type === "intent_expired" ||
        event.type === "intent_slashed"
      ) {
        const intentId = typeof event.intentId === "string" ? event.intentId : null;
        if (intentId) this.intentIndex.removeIntent(intentId);
      }
    } catch {
      // Index update is best-effort — never break broadcasts.
    }
  }

  getAliveCount(): number {
    let count = 0;
    for (const client of this.subscribers.keys()) {
      if (this.alive.get(client) === true) count++;
    }
    return count;
  }

  getSubscriberCount(): number {
    return this.subscribers.size;
  }

  /** Returns the current number of active WebSocket subscribers. */
  get subscriberCount(): number {
    return this.subscribers.size;
  }

  private heartbeat() {
    for (const [client] of this.subscribers) {
      if (this.alive.get(client) === false) {
        client.terminate();
        this.removeSubscriber(client);
        logger.debug(
          `ws heartbeat terminated dead client (subscribers=${this.subscribers.size})`,
        );
        continue;
      }

      this.alive.set(client, false);
      if (client.readyState === WebSocket.OPEN) {
        client.ping();
      }
    }
  }

  async onModuleDestroy() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.backplane.close();
    for (const [client] of this.subscribers) {
      client.close(1001, "Server shutting down");
      this.removeSubscriber(client);
    }
  }
}
