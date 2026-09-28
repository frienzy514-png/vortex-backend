import { SorobanRpc } from "@stellar/stellar-sdk";
import { SorobanService } from "./soroban.service";
import { TxConfirmationService } from "./tx-confirmation.service";

describe("TxConfirmationService", () => {
  const getTransaction = jest.fn();
  const service = new TxConfirmationService({ getTransaction } as unknown as SorobanService);

  it.each([
    [SorobanRpc.Api.GetTransactionStatus.SUCCESS, { status: "success", ledger: 5, ledgerCloseTime: 100 }],
    [SorobanRpc.Api.GetTransactionStatus.FAILED, { status: "failed", ledger: 5, ledgerCloseTime: 100 }],
    [SorobanRpc.Api.GetTransactionStatus.NOT_FOUND, { status: "not_found" }],
  ])("maps %s", async (status, expected) => {
    getTransaction.mockResolvedValueOnce({ status, ledger: 5, createdAt: 100 });
    await expect(service.check("h")).resolves.toEqual(expected);
    expect(getTransaction).toHaveBeenLastCalledWith("h");
  });

  it("propagates transport errors instead of reporting not_found", async () => {
    getTransaction.mockRejectedValueOnce(new Error("ECONNRESET"));
    await expect(service.check("h")).rejects.toThrow("ECONNRESET");
  });
});
