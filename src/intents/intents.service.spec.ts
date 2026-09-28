import { Test, TestingModule } from "@nestjs/testing";
import { ConfigService } from "@nestjs/config";
import { Keypair } from "@stellar/stellar-sdk";
import { AppConfig, CHAIN_FILL_WINDOW_DEFAULTS, DEFAULT_FILL_WINDOW_SECONDS } from "../config/configuration";
import { StellarTxService } from "../soroban/stellar-tx.service";
import { IntentsService } from "./intents.service";
import { INTENTS_REPOSITORY, InMemoryIntentsRepository } from "./intents.repository";
import { PrismaService } from "../prisma/prisma.service";
import { ProtocolParamsService } from "../governance/params.service";
import { InMemoryOutboxRepository } from "../soroban/outbox.repository";
import { InMemoryIntentsUnitOfWork } from "./intents.unit-of-work";

const VALID_CONTRACT_ID = "CBIELTK6YBZJU5UP2WWQEUCYKLPU6AUNZ2BQ4WWFEIE3USCIHMXQDAMA";

function fakeConfig(overrides: { onchainIntentsEnabled?: boolean; settlementContractId?: string } = {}) {
  const values: Record<string, unknown> = {
    onchainIntentsEnabled: overrides.onchainIntentsEnabled ?? false,
    "stellar.settlementContractId": overrides.settlementContractId ?? "",
  };
  return { get: (path: string) => values[path] } as ConfigService<AppConfig, true>;
}

function fakeStellarTxService() {
  return { invokeContract: jest.fn() } as unknown as jest.Mocked<StellarTxService>;
}

function fakePrismaService(): PrismaService {
  return {
    intentAuditLog: {
      create: jest.fn().mockResolvedValue({}),
      findMany: jest.fn().mockResolvedValue([]),
    },
  } as unknown as PrismaService;
}

function fakeProtocolParamsService(): ProtocolParamsService {
  return {
    snapshotForChain: jest.fn().mockReturnValue({
      version: 0,
      feeBps: 30,
      deadlineSeconds: 1800,
      fillWindowSeconds: 600,
      capturedAt: new Date().toISOString(),
    }),
    getCurrent: jest.fn().mockReturnValue({ version: 0, feeBps: 30, chains: {}, maxExposureRatio: 0.05, slashAmount: "100000000", activeSinceLedger: 0, adoptedAt: new Date().toISOString() }),
    getPending: jest.fn().mockReturnValue(null),
    getHistory: jest.fn().mockReturnValue([]),
  } as unknown as ProtocolParamsService;
}

function makeService(
  configOverrides: { onchainIntentsEnabled?: boolean; settlementContractId?: string } = {},
  stellarTx?: jest.Mocked<StellarTxService>,
) {
  return new IntentsService(
    new InMemoryIntentsRepository(),
    fakeConfig(configOverrides),
    stellarTx ?? fakeStellarTxService(),
    fakePrismaService(),
    fakeProtocolParamsService(),
  );
}

function validCreateData() {
  return {
    user: Keypair.random().publicKey(),
    srcChain: "ethereum" as const,
    srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6, chain: "ethereum" as const },
    srcAmount: "1000000",
    dstToken: { contract: VALID_CONTRACT_ID, symbol: "USDC", decimals: 7 },
    minDstAmount: "990000",
    deadline: Math.floor(Date.now() / 1000) + 1800,
  };
}

async function buildService(
  configOverrides: { onchainIntentsEnabled?: boolean; settlementContractId?: string } = {},
  stellarTxService?: jest.Mocked<StellarTxService>,
): Promise<IntentsService> {
  const module: TestingModule = await Test.createTestingModule({
    providers: [
      {
        provide: INTENTS_REPOSITORY,
        useClass: InMemoryIntentsRepository,
      },
      {
        provide: ConfigService,
        useValue: fakeConfig(configOverrides),
      },
      {
        provide: StellarTxService,
        useValue: stellarTxService ?? fakeStellarTxService(),
      },
      {
        provide: PrismaService,
        useValue: fakePrismaService(),
      },
      {
        provide: ProtocolParamsService,
        useValue: fakeProtocolParamsService(),
      },
      IntentsService,
    ],
  }).compile();

  return module.get<IntentsService>(IntentsService);
}

describe("IntentsService", () => {
  let service: IntentsService;

  beforeEach(() => {
    service = makeService();
  });

  it("seeds 5 intents on construction", async () => {
    expect(await service.getAll()).toHaveLength(5);
  });

  it("getAll returns intents sorted by createdAt descending", async () => {
    const all = await service.getAll();
    for (let i = 1; i < all.length; i++) {
      expect(all[i - 1].createdAt).toBeGreaterThanOrEqual(all[i].createdAt);
    }
  });

  it("create adds an open intent with a generated id", async () => {
    const before = (await service.getAll()).length;
    const deadline = Math.floor(Date.now() / 1000) + 1800;
    const intent = await service.create({
      user: "GTEST...0000",
      srcChain: "ethereum",
      srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6, chain: "ethereum" },
      srcAmount: "1000000",
      dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
      minDstAmount: "990000",
      deadline,
    });

    expect(intent.state).toBe("open");
    expect(intent.intentId).toBeTruthy();
    expect(intent.deadline).toBe(deadline);
    expect(await service.getAll()).toHaveLength(before + 1);
  });

  it("create defaults deadline to now + 1800 when omitted", async () => {
    const before = Math.floor(Date.now() / 1000);
    const intent = await service.create({
      user: "GTEST...0000",
      srcChain: "ethereum",
      srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6, chain: "ethereum" },
      srcAmount: "1000000",
      dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
      minDstAmount: "990000",
      deadline: undefined as unknown as number,
    });

    expect(intent.deadline).toBeGreaterThanOrEqual(before + 1800);
  });

  it("get returns undefined for an unknown id", async () => {
    expect(await service.get("does-not-exist")).toBeUndefined();
  });

  it("update mutates and returns the patched intent", async () => {
    const [existing] = await service.getByState("open");
    const updated = await service.update(existing.intentId, { state: "accepted", solver: "SOLVER_X" });

    expect(updated?.state).toBe("accepted");
    expect(updated?.solver).toBe("SOLVER_X");
    expect((await service.get(existing.intentId))?.state).toBe("accepted");
  });

  it("update returns null for an unknown id", async () => {
    expect(await service.update("does-not-exist", { state: "cancelled" })).toBeNull();
  });

  it("getByUser is case-insensitive", async () => {
    const [existing] = await service.getAll();
    const found = await service.getByUser(existing.user.toLowerCase());
    expect(found.some((i) => i.intentId === existing.intentId)).toBe(true);
  });

  it("getByState only returns intents in that state", async () => {
    for (const intent of await service.getByState("filled")) {
      expect(intent.state).toBe("filled");
    }
  });

  describe("acceptIfOpen", () => {
    it("transitions an open intent to accepted and returns it", async () => {
      const [open] = await service.getByState("open");
      const result = await service.acceptIfOpen(open.intentId, "SOLVER_X");

      expect(result).not.toBeNull();
      expect(result!.state).toBe("accepted");
      expect(result!.solver).toBe("SOLVER_X");
      expect((await service.get(open.intentId))!.state).toBe("accepted");
    });

    it("returns null for a non-existent intent", async () => {
      expect(await service.acceptIfOpen("does-not-exist", "SOLVER_X")).toBeNull();
    });

    it("returns null when the intent is already accepted", async () => {
      const [accepted] = await service.getByState("accepted");
      expect(await service.acceptIfOpen(accepted.intentId, "SOLVER_X")).toBeNull();
    });

    it("only the first caller wins under simulated concurrency", async () => {
      const [open] = await service.getByState("open");
      const results = await Promise.all(
        Array.from({ length: 10 }, (_, i) =>
          service.acceptIfOpen(open.intentId, `SOLVER_${i}`),
        ),
      );

      const successes = results.filter((r) => r !== null);
      expect(successes).toHaveLength(1);
      expect(successes[0]!.state).toBe("accepted");
    });

    // -----------------------------------------------------------------------
    // Per-chain fill-window tests (issue: chain-aware fill window)
    // -----------------------------------------------------------------------

    it("sets deadline to now + stellar fill window (120 s) for a stellar intent", async () => {
      const now = Math.floor(Date.now() / 1000);
      const intent = await service.create({
        user: "GTEST_STELLAR_CHAIN1",
        srcChain: "stellar",
        srcToken: { address: "native", symbol: "XLM", name: "Stellar Lumens", decimals: 7, chain: "stellar" },
        srcAmount: "1000000",
        dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
        minDstAmount: "990000",
        deadline: now + 900,
      });

      const result = await service.acceptIfOpen(intent.intentId, "SOLVER_X");

      expect(result).not.toBeNull();
      const expectedWindow = CHAIN_FILL_WINDOW_DEFAULTS["stellar"] ?? DEFAULT_FILL_WINDOW_SECONDS;
      // Allow a 2-second tolerance for test execution time
      expect(result!.deadline).toBeGreaterThanOrEqual(now + expectedWindow - 2);
      expect(result!.deadline).toBeLessThanOrEqual(now + expectedWindow + 2);
    });

    it("sets deadline to now + ethereum fill window (1800 s) for an ethereum intent", async () => {
      const now = Math.floor(Date.now() / 1000);
      const intent = await service.create({
        user: "GTEST_ETHEREUM_CHAIN1",
        srcChain: "ethereum",
        srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6, chain: "ethereum" },
        srcAmount: "1000000",
        dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
        minDstAmount: "990000",
        deadline: now + 3600,
      });

      const result = await service.acceptIfOpen(intent.intentId, "SOLVER_X");

      expect(result).not.toBeNull();
      const expectedWindow = CHAIN_FILL_WINDOW_DEFAULTS["ethereum"] ?? DEFAULT_FILL_WINDOW_SECONDS;
      // Allow a 2-second tolerance for test execution time
      expect(result!.deadline).toBeGreaterThanOrEqual(now + expectedWindow - 2);
      expect(result!.deadline).toBeLessThanOrEqual(now + expectedWindow + 2);
    });

    it("stellar and ethereum accepted intents get distinct (non-equal) fill deadlines", async () => {
      const now = Math.floor(Date.now() / 1000);

      const stellarIntent = await service.create({
        user: "GTEST_STELLAR_DIFF1",
        srcChain: "stellar",
        srcToken: { address: "native", symbol: "XLM", name: "Stellar Lumens", decimals: 7, chain: "stellar" },
        srcAmount: "1000000",
        dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
        minDstAmount: "990000",
        deadline: now + 900,
      });
      const ethIntent = await service.create({
        user: "GTEST_ETHEREUM_DIFF1",
        srcChain: "ethereum",
        srcToken: { address: "0xabc", symbol: "USDC", name: "USD Coin", decimals: 6, chain: "ethereum" },
        srcAmount: "1000000",
        dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
        minDstAmount: "990000",
        deadline: now + 3600,
      });

      const stellarResult = await service.acceptIfOpen(stellarIntent.intentId, "SOLVER_STELLAR");
      const ethResult = await service.acceptIfOpen(ethIntent.intentId, "SOLVER_ETH");

      expect(stellarResult).not.toBeNull();
      expect(ethResult).not.toBeNull();

      // Ethereum solver gets a materially larger fill window than Stellar
      expect(ethResult!.deadline).toBeGreaterThan(stellarResult!.deadline);

      // Confirm the windows match the config constants exactly (allowing 2 s clock drift)
      const stellarWindow = CHAIN_FILL_WINDOW_DEFAULTS["stellar"] ?? DEFAULT_FILL_WINDOW_SECONDS;
      const ethWindow = CHAIN_FILL_WINDOW_DEFAULTS["ethereum"] ?? DEFAULT_FILL_WINDOW_SECONDS;
      expect(ethWindow).toBeGreaterThan(stellarWindow); // sanity-check on config
    });

    it("falls back to DEFAULT_FILL_WINDOW_SECONDS for an unknown chain", async () => {
      const now = Math.floor(Date.now() / 1000);
      const intent = await service.create({
        user: "GTEST_UNKNOWN_CHAIN01",
        srcChain: "stellar", // create as valid chain, then patch for test
        srcToken: { address: "native", symbol: "XLM", name: "Stellar Lumens", decimals: 7, chain: "stellar" },
        srcAmount: "1000000",
        dstToken: { contract: "CTEST", symbol: "USDC", decimals: 7 },
        minDstAmount: "990000",
        deadline: now + 3600,
      });
      // Manually patch to an unknown chain to exercise the fallback
      await service.update(intent.intentId, { srcChain: "unknown_chain" as never });

      const result = await service.acceptIfOpen(intent.intentId, "SOLVER_X");

      expect(result).not.toBeNull();
      expect(result!.deadline).toBeGreaterThanOrEqual(now + DEFAULT_FILL_WINDOW_SECONDS - 2);
      expect(result!.deadline).toBeLessThanOrEqual(now + DEFAULT_FILL_WINDOW_SECONDS + 2);
    });
  }); // end describe("acceptIfOpen")

  describe("fillIfAccepted", () => {
    it("transitions an accepted intent to filled when solver matches", async () => {
      const [accepted] = await service.getByState("accepted");
      const result = await service.fillIfAccepted(accepted.intentId, accepted.solver!, {
        fillAmount: "100",
        txHash: "test-hash",
        filledAt: Math.floor(Date.now() / 1000),
      });

      expect(result).not.toBeNull();
      expect(result!.state).toBe("filled");
      expect(result!.fillAmount).toBe("100");
    });

    it("returns null when solver does not match", async () => {
      const [accepted] = await service.getByState("accepted");
      const result = await service.fillIfAccepted(accepted.intentId, "WRONG_SOLVER", {
        fillAmount: "100",
      });
      expect(result).toBeNull();
    });

    it("returns null for a non-existent intent", async () => {
      expect(await service.fillIfAccepted("nope", "SOLVER_X", {})).toBeNull();
    });

    it("only the first caller wins under simulated concurrency", async () => {
      const [accepted] = await service.getByState("accepted");
      const results = await Promise.all(
        Array.from({ length: 10 }, () =>
          service.fillIfAccepted(accepted.intentId, accepted.solver!, {
            fillAmount: "100",
            txHash: "race-hash",
            filledAt: Math.floor(Date.now() / 1000),
          }),
        ),
      );

      const successes = results.filter((r) => r !== null);
      expect(successes).toHaveLength(1);
      expect(successes[0]!.state).toBe("filled");
    });
  });

  describe("on-chain writes via the transactional outbox (ONCHAIN_INTENTS_ENABLED, #396)", () => {
    function makeOutboxService(onchain: boolean, settlementContractId = VALID_CONTRACT_ID) {
      const stellarTxService = fakeStellarTxService();
      const repo = new InMemoryIntentsRepository();
      const outbox = new InMemoryOutboxRepository();
      const service = new IntentsService(
        repo,
        fakeConfig({ onchainIntentsEnabled: onchain, settlementContractId }),
        stellarTxService,
        fakePrismaService(),
        undefined, // shadowService
        undefined, // metricsService
        fakeProtocolParamsService(),
        undefined, // flags
        new InMemoryIntentsUnitOfWork(repo, outbox),
      );
      return { service, outbox, stellarTxService, repo };
    }

    it("stays fully off-chain when the flag is off: no outbox rows, no StellarTxService", async () => {
      const { service, outbox, stellarTxService } = makeOutboxService(false);

      const intent = await service.create(validCreateData());

      expect(stellarTxService.invokeContract).not.toHaveBeenCalled();
      expect(await outbox.findByIntent(intent.intentId)).toEqual([]);
      expect(await service.get(intent.intentId)).toEqual(intent);
    });

    it("commits the intent and a create_intent outbox row together, without submitting inline", async () => {
      const { service, outbox, stellarTxService } = makeOutboxService(true);

      const intent = await service.create(validCreateData());

      expect(stellarTxService.invokeContract).not.toHaveBeenCalled();
      const rows = await outbox.findByIntent(intent.intentId);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({
        operation: "create_intent",
        status: "pending",
        payload: expect.objectContaining({ intentId: intent.intentId, srcAmount: "1000000" }),
      });
      expect(await service.get(intent.intentId)).toEqual(intent);
    });

    it("rejects with a clear error and writes nothing when SETTLEMENT_CONTRACT_ID is unset", async () => {
      const { service, outbox, repo } = makeOutboxService(true, "");
      const before = repo.findAll().length;

      await expect(service.create(validCreateData())).rejects.toMatchObject({
        message: expect.stringContaining("SETTLEMENT_CONTRACT_ID"),
      });
      expect(repo.findAll()).toHaveLength(before);
      expect((await outbox.countByStatus()).pending).toBe(0);
    });

    it("rejects a payload that cannot be encoded instead of creating a poison row", async () => {
      const { service, outbox } = makeOutboxService(true);

      await expect(service.create({ ...validCreateData(), user: "not-a-stellar-address" })).rejects.toThrow();
      expect((await outbox.countByStatus()).pending).toBe(0);
    });

    it("drops the outbox row when the intent write fails (atomic unit of work)", async () => {
      const { service, outbox, repo } = makeOutboxService(true);
      jest.spyOn(repo, "save").mockImplementationOnce(() => {
        throw new Error("db down");
      });

      await expect(service.create(validCreateData())).rejects.toThrow("db down");
      expect((await outbox.countByStatus()).pending).toBe(0);
    });

    it("enqueues accept, fill and cancel transitions in order, and nothing for a lost race", async () => {
      const { service, outbox } = makeOutboxService(true);
      const solver = Keypair.random().publicKey();

      const a = await service.create(validCreateData());
      expect(await service.acceptIfOpen(a.intentId, solver)).not.toBeNull();
      expect(await service.acceptIfOpen(a.intentId, solver)).toBeNull(); // lost race → no row
      expect(
        await service.fillIfAccepted(a.intentId, solver, { fillAmount: "995000", txHash: "ab".repeat(32) }),
      ).not.toBeNull();

      const b = await service.create(validCreateData());
      expect(await service.cancelIfOpen(b.intentId)).not.toBeNull();

      expect((await outbox.findByIntent(a.intentId)).map((r) => r.operation)).toEqual([
        "create_intent",
        "accept_intent",
        "fill_intent",
      ]);
      expect((await outbox.findByIntent(b.intentId)).map((r) => r.operation)).toEqual([
        "create_intent",
        "cancel_intent",
      ]);
    });

    it("follows the onchain-intents-enabled runtime flag per intent when flags are wired", async () => {
      const repo = new InMemoryIntentsRepository();
      const outbox = new InMemoryOutboxRepository();
      const flags = { getBooleanValue: jest.fn().mockResolvedValue(true) };
      const service = new IntentsService(
        repo,
        fakeConfig({ onchainIntentsEnabled: false, settlementContractId: VALID_CONTRACT_ID }),
        fakeStellarTxService(),
        fakePrismaService(),
        undefined,
        undefined,
        fakeProtocolParamsService(),
        flags as never,
        new InMemoryIntentsUnitOfWork(repo, outbox),
      );

      const intent = await service.create(validCreateData());
      expect(flags.getBooleanValue).toHaveBeenCalledWith("onchain-intents-enabled", {
        targetingKey: intent.intentId,
        chain: "ethereum",
      });
      expect(await outbox.findByIntent(intent.intentId)).toHaveLength(1);
    });
  });

  // ---------------------------------------------------------------------------
  // Audit trail (issue #217 / #62)
  // ---------------------------------------------------------------------------

  describe("appendAuditEntry / getAuditLog", () => {
    it("returns an empty array for an intent with no audit entries", () => {
      expect(service.getAuditLog("no-such-intent")).toEqual([]);
    });

    it("appends a single entry and getAuditLog returns it", () => {
      service.appendAuditEntry("intent-1", "cancelled", "USER_ADDR", "user cancelled");
      const log = service.getAuditLog("intent-1");
      expect(log).toHaveLength(1);
      expect(log[0]).toMatchObject({
        toState: "cancelled",
        actor: "USER_ADDR",
        reason: "user cancelled",
      });
      expect(log[0].timestamp).toBeTruthy(); // ISO timestamp
    });

    it("appends multiple entries in order and getAuditLog returns oldest-first", async () => {
      service.appendAuditEntry("intent-2", "accepted", "SOLVER_A", "solver accepted");
      await new Promise((r) => setTimeout(r, 5)); // small gap so timestamps differ
      service.appendAuditEntry("intent-2", "filled", "SOLVER_A", "solver filled");

      const log = service.getAuditLog("intent-2");
      expect(log).toHaveLength(2);
      expect(log[0].toState).toBe("accepted");
      expect(log[1].toState).toBe("filled");
    });

    it("stores optional metadata in the entry", () => {
      service.appendAuditEntry("intent-3", "expired", "system", "deadline passed", {
        deadline: 1234567890,
        sweepedAt: 1234567900,
      });
      const log = service.getAuditLog("intent-3");
      expect(log[0].metadata).toEqual({ deadline: 1234567890, sweepedAt: 1234567900 });
    });

    it("does not mix entries across different intentIds", () => {
      service.appendAuditEntry("intent-A", "cancelled", "USER_A", "cancel A");
      service.appendAuditEntry("intent-B", "expired", "system", "expire B");

      expect(service.getAuditLog("intent-A")).toHaveLength(1);
      expect(service.getAuditLog("intent-B")).toHaveLength(1);
      expect(service.getAuditLog("intent-A")[0].toState).toBe("cancelled");
      expect(service.getAuditLog("intent-B")[0].toState).toBe("expired");
    });

    it("fires a DB write via PrismaService on each append (non-blocking)", async () => {
      const prismaService = {
        intentAuditLog: {
          create: jest.fn().mockResolvedValue({}),
          findMany: jest.fn().mockResolvedValue([]),
        },
      } as unknown as PrismaService;
      const svc = new IntentsService(new InMemoryIntentsRepository(), fakeConfig(), fakeStellarTxService(), prismaService, fakeProtocolParamsService());

      svc.appendAuditEntry("intent-db", "slashed", "system", "missed fill", { foo: "bar" });

      // The DB write is fire-and-forget — wait one tick for the promise chain
      await new Promise((r) => setImmediate(r));

      const mockPrisma = prismaService as unknown as {
        intentAuditLog: { create: jest.Mock };
      };
      expect(mockPrisma.intentAuditLog.create).toHaveBeenCalledWith(
        expect.objectContaining({
          data: expect.objectContaining({
            intentId: "intent-db",
            toState: "slashed",
            actor: "system",
            reason: "missed fill",
          }),
        }),
      );
    });

    it("does NOT throw when the DB write fails — logs an error but returns normally", async () => {
      const prismaService = {
        intentAuditLog: {
          create: jest.fn().mockRejectedValue(new Error("DB is down")),
          findMany: jest.fn().mockResolvedValue([]),
        },
      } as unknown as PrismaService;
      const svc = new IntentsService(new InMemoryIntentsRepository(), fakeConfig(), fakeStellarTxService(), prismaService, fakeProtocolParamsService());

      // Should not throw synchronously
      expect(() =>
        svc.appendAuditEntry("intent-fail", "expired", "system", "deadline"),
      ).not.toThrow();

      // In-memory log still has the entry
      expect(svc.getAuditLog("intent-fail")).toHaveLength(1);

      // Wait for the rejected promise — should not propagate
      await new Promise((r) => setImmediate(r));
      // No unhandled rejection here (jest would fail the test if one occurred)
    });
  });
});
