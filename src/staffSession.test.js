import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { lockSeconds, readStoredSession, sessionFromLogin, sessionFromWhoami } from "./staffSession.js";

describe("LMS session helpers", () => {
  const now = Date.parse("2026-10-21T16:00:00Z");

  it("stores the server expiry and the token", () => {
    const session = sessionFromLogin({
      token: "signed",
      expires_at: "2026-10-22T00:00:00.000Z",
      user: { id: 4, name: "Geoff Horen", role: "Owner" },
    }, now);
    assert.equal(session.expires, Date.parse("2026-10-22T00:00:00.000Z"));
    assert.equal(session.token, "signed");
    assert.equal(session.role, "Owner");
  });

  it("drops a stored session with no token or a past expiry", () => {
    assert.equal(readStoredSession({ id: 4, name: "Geoff Horen", role: "Owner", expires: now + 1000 }, now), null);
    assert.equal(readStoredSession({ token: "signed", expires: now }, now), null);
    assert.equal(readStoredSession(null, now), null);
  });

  it("replaces the cached role with the whoami re-read", () => {
    const stored = { id: 4, name: "Geoff Horen", role: "Owner", expires: now + 1000, token: "signed" };
    const next = sessionFromWhoami(stored, { id: 4, name: "Geoff Horen", role: "Sales", active: true }, now);
    assert.equal(next.role, "Sales");
    assert.equal(next.token, "signed");
    assert.equal(sessionFromWhoami(stored, { id: 4, name: "Geoff Horen", role: "Owner", active: false }, now), null);
  });

  it("uses the server retry_after for the lock countdown", () => {
    assert.equal(lockSeconds({ retry_after: 900 }), 900);
    assert.equal(lockSeconds({}), 900);
  });
});

describe("LMS client no longer reads crm_users", () => {
  it("removed the anon PIN query from both App files", () => {
    for (const path of ["App.jsx", "../App.jsx"]) {
      const source = readFileSync(new URL(path, import.meta.url), "utf8");
      assert.equal(source.includes("crm_users"), false);
      assert.equal(source.includes('action: "login"') || source.includes("action: \"login\""), true);
      assert.equal(source.includes('action: "whoami"') || source.includes("action: \"whoami\""), true);
      assert.equal(source.includes('action: "names"') || source.includes("action: \"names\""), true);
    }
  });
});
