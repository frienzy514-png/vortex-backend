import { ConfigService } from "@nestjs/config";
import { SorobanRpc, StrKey, nativeToScVal, xdr } from "@stellar/stellar-sdk";
import { AppConfig } from "../config/configuration";
import { FillVerifierService } from "./fill-verifier.service";
import { SorobanService } from "./soroban.service";

const CONTRACT_ID = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA";
const OTHER_CONTRACT = StrKey.encodeContract(Buffer.alloc(32, 7));
const DEADLINE = 1_900_000_000;
const BOUND = DEADLINE + 30;

const topic = (name: string, intentId: string) => [
  nativeToScVal(name, { type: "symbol" }),
  nativeToScVal(intentId, { type: "string" }),
];
const event = (name: string, intentId: string, closedAt: number, txHash = "fill") => ({
  topic: topic(name, intentId),
  ledgerClosedAt: new Date(closedAt * 1000).toISOString(),
  txHash,
  ledger: 42,
  pagingToken: `pt-${closedAt}`,
});

function metaWith(events: Array<{ contract?: string; topics: xdr.ScVal[] }>) {
  const contractEvents = events.map((e) => ({
    contractId: () => (e.contract ? StrKey.decodeContract(e.contract) : null),
    body: () => ({ v0: () => ({ topics: () => e.topics }) }),
  }));
  return { v3: () => ({ sorobanMeta: () => ({ events: () => contractEvents }) }) } as unknown as xdr.TransactionMeta;
}

describe("FillVerifierService (#397)", () => {
  let soroban: { getLatestLedger: jest.Mock; getEvents: jest.Mock; getTransaction: jest.Mock };
  const build = (contractId = CONTRACT_ID) =>
    new FillVerifierService(
      soroban as unknown as SorobanService,
      { get: () => contractId } as unknown as ConfigService<AppConfig, true>,
    );

  beforeEach(() => {
    soroban = {
      getLatestLedger: jest.fn().mockResolvedValue({ sequence: 100_000 }),
      getEvents: jest.fn().mockResolvedValue({ events: [], latestLedger: 100_000 }),
      getTransaction: jest.fn(),
    };
  });

  describe("findLandedFill", () => {
    it("returns null without a settlement contract (nothing to verify against)", async () => {
      expect(await build("").findLandedFill("i1", DEADLINE, BOUND)).toBeNull();
      expect(soroban.getEvents).not.toHaveBeenCalled();
    });

    it("looks back far enough to cover the fill window and finds an in-time fill", async () => {
      soroban.getEvents.mockResolvedValueOnce({
        events: [event("intent_filled", "other", BOUND), event("intent_accepted", "i1", BOUND), event("intent_filled", "i1", BOUND)],
      });
      const now = DEADLINE + 600;
      expect(await build().findLandedFill("i1", DEADLINE, BOUND, now)).toEqual({ txHash: "fill", ledger: 42, closedAt: BOUND });
      expect(soroban.getEvents.mock.calls[0][0]).toMatchObject({
        startLedger: 100_000 - Math.ceil((600 + 1800) / 5),
        filters: [{ type: "contract", contractIds: [CONTRACT_ID] }],
      });
    });

    it("ignores fills that closed after deadline + tolerance (chain time)", async () => {
      soroban.getEvents.mockResolvedValueOnce({ events: [event("intent_filled", "i1", BOUND + 1)] });
      expect(await build().findLandedFill("i1", DEADLINE, BOUND)).toBeNull();
    });

    it("paginates with the last paging token", async () => {
      const page = Array.from({ length: 200 }, (_, i) => event("noise", "x", DEADLINE + i));
      soroban.getEvents
        .mockResolvedValueOnce({ events: page })
        .mockResolvedValueOnce({ events: [event("intent_filled", "i1", DEADLINE)] });
      expect(await build().findLandedFill("i1", DEADLINE, BOUND)).not.toBeNull();
      expect(soroban.getEvents.mock.calls[1][0]).toMatchObject({ cursor: `pt-${DEADLINE + 199}` });
      expect(soroban.getEvents.mock.calls[1][0].startLedger).toBeUndefined();
    });

    it("stops after the page cap and tolerates undecodable topics", async () => {
      const junk = { ...event("x", "y", DEADLINE), topic: [{} as xdr.ScVal] };
      soroban.getEvents.mockResolvedValue({ events: Array.from({ length: 200 }, () => junk) });
      expect(await build().findLandedFill("i1", DEADLINE, BOUND)).toBeNull();
      expect(soroban.getEvents).toHaveBeenCalledTimes(10);
    });

    it("propagates RPC errors so the saga retries instead of slashing", async () => {
      soroban.getLatestLedger.mockRejectedValueOnce(new Error("RPC down"));
      await expect(build().findLandedFill("i1", DEADLINE, BOUND)).rejects.toThrow("RPC down");
    });
  });

  describe("verifyFillProof", () => {
    const success = (meta: xdr.TransactionMeta, createdAt = BOUND) => ({
      status: SorobanRpc.Api.GetTransactionStatus.SUCCESS, ledger: 5, createdAt, resultMetaXdr: meta,
    });

    it("accepts a successful, timely tx that emitted intent_filled from the settlement contract", async () => {
      soroban.getTransaction.mockResolvedValueOnce(success(metaWith([{ contract: CONTRACT_ID, topics: topic("intent_filled", "i1") }])));
      expect(await build().verifyFillProof("h", "i1", BOUND)).toEqual({
        valid: true, fill: { txHash: "h", ledger: 5, closedAt: BOUND },
      });
    });

    it.each([
      ["not successful", { status: SorobanRpc.Api.GetTransactionStatus.NOT_FOUND }, /status is NOT_FOUND/],
      ["too late", success(metaWith([{ contract: CONTRACT_ID, topics: topic("intent_filled", "i1") }]), BOUND + 1), /after the acceptable bound/],
      ["wrong contract", success(metaWith([{ contract: OTHER_CONTRACT, topics: topic("intent_filled", "i1") }])), /did not emit/],
      ["no contract id", success(metaWith([{ topics: topic("intent_filled", "i1") }])), /did not emit/],
      ["other intent", success(metaWith([{ contract: CONTRACT_ID, topics: topic("intent_filled", "i2") }])), /did not emit/],
      ["unreadable meta", success({ v3: () => { throw new Error("v2 meta"); } } as unknown as xdr.TransactionMeta), /did not emit/],
    ])("rejects a proof that is %s", async (_label, tx, reason) => {
      soroban.getTransaction.mockResolvedValueOnce(tx);
      const result = await build().verifyFillProof("h", "i1", BOUND);
      expect(result.valid).toBe(false);
      expect((result as { reason: string }).reason).toMatch(reason);
    });

    it("skips the contract check when no settlement contract is configured, and survives malformed bodies", async () => {
      const meta = metaWith([{ topics: topic("intent_filled", "i1") }]);
      soroban.getTransaction.mockResolvedValueOnce(success(meta));
      expect((await build("").verifyFillProof("h", "i1", BOUND)).valid).toBe(true);

      const broken = {
        v3: () => ({ sorobanMeta: () => ({ events: () => [{ contractId: () => null, body: () => { throw new Error("x"); } }] }) }),
      } as unknown as xdr.TransactionMeta;
      soroban.getTransaction.mockResolvedValueOnce(success(broken));
      expect((await build("").verifyFillProof("h", "i1", BOUND)).valid).toBe(false);

      const noSorobanMeta = { v3: () => ({ sorobanMeta: () => null }) } as unknown as xdr.TransactionMeta;
      soroban.getTransaction.mockResolvedValueOnce(success(noSorobanMeta));
      expect((await build("").verifyFillProof("h", "i1", BOUND)).valid).toBe(false);
    });
  });
});
