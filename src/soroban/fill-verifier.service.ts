import { Injectable, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { SorobanRpc, StrKey, scValToNative, xdr } from "@stellar/stellar-sdk";
import { AppConfig } from "../config/configuration";
import { SorobanService } from "./soroban.service";

/** Approximate Stellar ledger close interval, used to size the event lookback. */
const LEDGER_SECONDS = 5;
/** Longest per-chain fill window (CHAIN_FILL_WINDOW_DEFAULTS.ethereum) — a fill can't predate acceptance by more. */
const MAX_FILL_WINDOW_SECONDS = 1800;
const EVENTS_PAGE_LIMIT = 200;
const MAX_EVENT_PAGES = 10;

export interface LandedFill {
  txHash: string;
  ledger: number;
  /** Ledger close time, unix seconds — chain time, not server time. */
  closedAt: number;
}

/**
 * Re-verification for the slashing saga (issue #397): answers "did a fill for
 * this intent land on-chain in time?" using chain data only.
 *
 * Timeliness is judged on the ledger close time (chain clock) against
 * `latestAcceptable = fillDeadline + SLASH_CLOCK_SKEW_TOLERANCE_SECONDS`, so
 * skew between the sweeper host and the network never slashes a solver whose
 * fill closed a few seconds "late" by server time.
 *
 * Methods throw on RPC failure: the caller must treat "couldn't check" as
 * "don't slash yet", never as "no fill".
 */
@Injectable()
export class FillVerifierService {
  private readonly logger = new Logger(FillVerifierService.name);
  private readonly settlementContractId: string;

  constructor(
    private readonly sorobanService: SorobanService,
    configService: ConfigService<AppConfig, true>,
  ) {
    this.settlementContractId = configService.get("stellar.settlementContractId", { infer: true });
  }

  /**
   * Scans the settlement contract's recent events for an `intent_filled`
   * event for `intentId` that closed by `latestAcceptable`.
   *
   * Returns null when none is found — including when SETTLEMENT_CONTRACT_ID is
   * unset, since there is then no on-chain fill path to verify against.
   */
  async findLandedFill(
    intentId: string,
    fillDeadline: number,
    latestAcceptable: number,
    now: number = Math.floor(Date.now() / 1000),
  ): Promise<LandedFill | null> {
    if (!this.settlementContractId) return null;

    const latest = await this.sorobanService.getLatestLedger();
    const lookbackSeconds = Math.max(0, now - fillDeadline) + MAX_FILL_WINDOW_SECONDS;
    const startLedger = Math.max(1, latest.sequence - Math.ceil(lookbackSeconds / LEDGER_SECONDS));

    let cursor: string | undefined;
    for (let page = 0; page < MAX_EVENT_PAGES; page++) {
      const response = await this.sorobanService.getEvents({
        ...(cursor ? { cursor } : { startLedger }),
        filters: [{ type: "contract", contractIds: [this.settlementContractId] }],
        limit: EVENTS_PAGE_LIMIT,
      });

      for (const event of response.events) {
        if (!isIntentFilled(event.topic, intentId)) continue;
        const closedAt = Math.floor(Date.parse(event.ledgerClosedAt) / 1000);
        if (closedAt <= latestAcceptable) {
          return { txHash: event.txHash, ledger: event.ledger, closedAt };
        }
        this.logger.log(
          `[fill-verifier] intent=${intentId} fill ${event.txHash} closed at ${closedAt}, ` +
            `after the acceptable bound ${latestAcceptable} — does not cancel the slash`,
        );
      }

      if (response.events.length < EVENTS_PAGE_LIMIT) break;
      cursor = response.events[response.events.length - 1].pagingToken;
    }
    return null;
  }

  /**
   * Verifies a solver-supplied fill proof: `txHash` must be a successful
   * transaction, closed by `latestAcceptable`, that emitted `intent_filled`
   * for `intentId` (from the settlement contract, when configured).
   */
  async verifyFillProof(
    txHash: string,
    intentId: string,
    latestAcceptable: number,
  ): Promise<{ valid: true; fill: LandedFill } | { valid: false; reason: string }> {
    const tx = await this.sorobanService.getTransaction(txHash);
    if (tx.status !== SorobanRpc.Api.GetTransactionStatus.SUCCESS) {
      return { valid: false, reason: `transaction status is ${tx.status}` };
    }
    if (tx.createdAt > latestAcceptable) {
      return {
        valid: false,
        reason: `fill closed at ${tx.createdAt}, after the acceptable bound ${latestAcceptable}`,
      };
    }
    if (!this.emitsIntentFilled(tx.resultMetaXdr, intentId)) {
      return { valid: false, reason: "transaction did not emit intent_filled for this intent" };
    }
    return { valid: true, fill: { txHash, ledger: tx.ledger, closedAt: tx.createdAt } };
  }

  private emitsIntentFilled(meta: xdr.TransactionMeta, intentId: string): boolean {
    let events: xdr.ContractEvent[] = [];
    try {
      events = meta.v3().sorobanMeta()?.events() ?? [];
    } catch {
      return false;
    }
    return events.some((event) => {
      if (this.settlementContractId) {
        const contractId = event.contractId();
        if (!contractId || !this.matchesSettlementContract(contractId)) return false;
      }
      try {
        return isIntentFilled(event.body().v0().topics(), intentId);
      } catch {
        return false;
      }
    });
  }

  private matchesSettlementContract(contractId: Buffer): boolean {
    return StrKey.encodeContract(contractId) === this.settlementContractId;
  }
}

/** Topic layout shared with EventIngestionService: [event name, intentId, ...]. */
function isIntentFilled(topic: xdr.ScVal[], intentId: string): boolean {
  const decoded = topic.slice(0, 2).map((scVal) => {
    try {
      return scValToNative(scVal);
    } catch {
      return undefined;
    }
  });
  return decoded[0] === "intent_filled" && decoded[1] === intentId;
}
