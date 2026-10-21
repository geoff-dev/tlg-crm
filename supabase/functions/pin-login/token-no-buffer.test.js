import assert from "node:assert/strict";
import { describe, it } from "node:test";

describe("token signing without a global Buffer", () => {
  it("signs and verifies after the global Buffer is removed", async () => {
    const previous = globalThis.Buffer;
    delete globalThis.Buffer;
    try {
      assert.equal(globalThis.Buffer, undefined);
      const { signStaffToken, verifyStaffToken } = await import("./token.js");
      const issuedAt = 1_700_000_000;
      const signed = signStaffToken(
        { crm_user_id: 4, name: "Geoff Horen", role: "Owner", tv: 1, app: "lms" },
        "test-staff-secret",
        issuedAt,
      );
      const verified = verifyStaffToken(signed.token, "test-staff-secret", issuedAt);
      assert.equal(verified.ok, true);
      assert.equal(verified.claims.crm_user_id, 4);
      assert.equal(verified.claims.role, "Owner");
      assert.equal(verified.claims.exp - verified.claims.iat, 8 * 60 * 60);
    } finally {
      globalThis.Buffer = previous;
    }
  });
});
