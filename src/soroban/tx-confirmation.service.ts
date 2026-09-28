/**
 * TxConfirmationService (issue #394)
 * ────────────────────────────────────
 * Polls the Soroban RPC until a submitted transaction reaches a terminal
 * status (SUCCESS or FAILED) or the configured timeout elapses.
 *
 * Used by StellarTxService after every live-path submitTransaction call so
 * callers receive a definitive result rather than an optimistic "sent".
 *
 * Metrics: records fill-to-confirmation latency via MetricsService
 * (vortex_tx_confirmation_duration_seconds, SLO SLI from issue #480).
 */

import { Injectable, Logger, Optional } from "@nestjs/common";
import { SorobanRpc } from "@stellar/stellar-sdk";
import { SorobanService } from "./soroban.service";
import { MetricsService } from "../metrics/metrics.service";

const DEFAULT_POLL_INTERVAL_MS = 3_000;
const DEFAULT_TIMEOUT_MS = 120_000; // 2 minutes

/**
 * Normalized outcome of a single, non-blocking lookup ({@link TxConfirmationService.check}).
 *
 *   success   — applied successfully in a closed ledger.
 *   failed    — included in a ledger but the invocation failed.
 *   not_found — unknown to the RPC node: still pending, dropped, or expired
 *               past its time bound. Callers decide which using their own
 *               notion of elapsed time.
 */
export type TxConfirmationStatus = "success" | "failed" | "not_found";

export interface TxConfirmation {
  status: TxConfirmationStatus;
  /** Ledger the transaction was included in (success/failed only). */
  ledger?: number;
  /** Ledger close time, unix seconds (success/failed only). */
  ledgerCloseTime?: number;
}

export interface ConfirmationResult {
  hash: string;
  status: "SUCCESS" | "FAILED" | "TIMEOUT";
  /** Full RPC response when status is SUCCESS or FAILED. */
  response?: SorobanRpc.Api.GetTransactionResponse;
  /** Error message when status is FAILED or TIMEOUT. */
  error?: string;
  /** Wall-clock milliseconds from first poll until terminal status. */
  durationMs: number;
}

@Injectable()
export class TxConfirmationService {
  private readonly logger = new Logger(TxConfirmationService.name);

  constructor(
    private readonly sorobanService: SorobanService,
    @Optional() private readonly metricsService?: MetricsService,
  ) {}

  /**
   * Single, non-blocking lookup of `hash` (issues #396 / #397).
   *
   * For durable callers — the outbox relay and the slashing saga — that
   * persist the hashes they wait on and re-check on their own schedule, so a
   * restart never loses an in-flight transaction and a worker tick never
   * blocks for the full {@link waitForConfirmation} timeout. RPC transport
   * errors propagate so callers can retry rather than mistake an outage for
   * NOT_FOUND.
   */
  async check(hash: string): Promise<TxConfirmation> {
    const response = await this.sorobanService.getTransaction(hash);
    switch (response.status) {
      case SorobanRpc.Api.GetTransactionStatus.SUCCESS:
        return { status: "success", ledger: response.ledger, ledgerCloseTime: response.createdAt };
      case SorobanRpc.Api.GetTransactionStatus.FAILED:
        this.logger.warn(`[tx-confirmation] tx ${hash} failed in ledger ${response.ledger}`);
        return { status: "failed", ledger: response.ledger, ledgerCloseTime: response.createdAt };
      default:
        return { status: "not_found" };
    }
  }

  /**
   * Poll until the transaction identified by `hash` reaches a terminal state.
   *
   * @param hash         Transaction hash returned by `sendTransaction`.
   * @param submittedAt  Unix-ms timestamp when the transaction was submitted
   *                     (used to compute confirmation latency for the SLO SLI).
   * @param pollIntervalMs  How often to poll (default 3 s).
   * @param timeoutMs       Give-up threshold (default 2 min).
   */
  async waitForConfirmation(
    hash: string,
    submittedAt: number,
    pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  ): Promise<ConfirmationResult> {
    const deadline = Date.now() + timeoutMs;

    while (Date.now() < deadline) {
      let response: SorobanRpc.Api.GetTransactionResponse;
      try {
        response = await this.sorobanService.getTransaction(hash);
      } catch (err) {
        this.logger.warn(
          `[tx-confirmation] getTransaction(${hash}) threw: ${(err as Error).message}; retrying`,
        );
        await sleep(pollIntervalMs);
        continue;
      }

      if (response.status === SorobanRpc.Api.GetTransactionStatus.NOT_FOUND) {
        await sleep(pollIntervalMs);
        continue;
      }

      const durationMs = Date.now() - submittedAt;

      if (response.status === SorobanRpc.Api.GetTransactionStatus.SUCCESS) {
        this.logger.log(
          `[tx-confirmation] SUCCESS hash=${hash} durationMs=${durationMs}`,
        );
        try {
          this.metricsService?.observeTxConfirmation(durationMs / 1000);
        } catch {
          /* metrics must never throw */
        }
        return { hash, status: "SUCCESS", response, durationMs };
      }

      // FAILED
      const errorDetail = (response as { resultXdr?: string }).resultXdr ?? "unknown";
      this.logger.warn(
        `[tx-confirmation] FAILED hash=${hash} durationMs=${durationMs} detail=${errorDetail}`,
      );
      return { hash, status: "FAILED", response, error: errorDetail, durationMs };
    }

    const durationMs = Date.now() - submittedAt;
    this.logger.warn(
      `[tx-confirmation] TIMEOUT hash=${hash} after ${durationMs}ms`,
    );
    return {
      hash,
      status: "TIMEOUT",
      error: `Transaction not confirmed within ${timeoutMs}ms`,
      durationMs,
    };
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
