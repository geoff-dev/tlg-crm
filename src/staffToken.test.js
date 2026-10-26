import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { signStaffToken } from "../supabase/functions/pin-login/token.js";
import {
  EMPTY_WRITE_MESSAGE,
  SESSION_EXPIRED_MESSAGE,
  checkStaffWrite,
  interpretWriteResult,
  reportStaffWriteFailure,
  staffRequestHeaders,
  subscribeStaffWriteFailure,
} from "./staffToken.js";

const SB_URL = "https://dwkvdelvnmvniewakzoq.supabase.co";
const SECRET = "test-secret-not-real";
const NOW = Date.parse("2026-10-26T12:00:00Z");

function memoryStorage(value) {
  return {
    getItem(key) {
      if (key !== "tlg_session") return null;
      return value == null ? null : JSON.stringify(value);
    },
  };
}

function session(token, expires) {
  return { id: 1, name: "n", role: "Owner", token: token, expires: expires };
}

describe("staff token header", () => {
  it("adds x-staff-token when a non-expired session is stored", () => {
    const headers = staffRequestHeaders(
      { apikey: "anon", Authorization: "Bearer anon" },
      SB_URL + "/rest/v1/projects?select=id",
      SB_URL,
      { storage: memoryStorage(session("signed-token", NOW + 60_000)), now: NOW },
    );
    assert.equal(headers["x-staff-token"], "signed-token");
    assert.equal(headers.apikey, "anon");
    assert.equal(headers.Authorization, "Bearer anon");
  });

  it("omits the header when nobody is logged in", () => {
    const headers = staffRequestHeaders(
      { apikey: "anon" },
      SB_URL + "/rest/v1/projects",
      SB_URL,
      { storage: memoryStorage(null), now: NOW },
    );
    assert.equal(Object.prototype.hasOwnProperty.call(headers, "x-staff-token"), false);
    assert.equal(headers.apikey, "anon");
  });

  it("omits the header when the stored session has no token", () => {
    const headers = staffRequestHeaders(
      { apikey: "anon" },
      SB_URL,
      SB_URL,
      { storage: memoryStorage({ expires: NOW + 60_000, role: "Owner" }), now: NOW },
    );
    assert.equal(Object.prototype.hasOwnProperty.call(headers, "x-staff-token"), false);
  });

  it("never adds the header to a non-Supabase URL", () => {
    const active = { storage: memoryStorage(session("signed-token", NOW + 60_000)), now: NOW };
    const base = { apikey: "anon", "x-staff-token": "should-not-leak", "X-Staff-Token": "should-not-leak" };
    const targets = [
      "https://example.com/hook",
      "https://dwkvdelvnmvniewakzoq.supabase.co.evil.example/rest/v1/projects",
      "https://evil.example/rest/v1/projects?next=" + SB_URL,
      "http://dwkvdelvnmvniewakzoq.supabase.co/rest/v1/projects",
      "/rest/v1/projects",
    ];
    for (const target of targets) {
      const headers = staffRequestHeaders(base, target, SB_URL, active);
      assert.equal(Object.prototype.hasOwnProperty.call(headers, "x-staff-token"), false, target);
      assert.equal(Object.prototype.hasOwnProperty.call(headers, "X-Staff-Token"), false, target);
      assert.equal(headers.apikey, "anon");
    }
  });
});

describe("staff write guard on the client", () => {
  it("refuses an expired token before the write and names the message", () => {
    const issuedAt = Math.floor(NOW / 1000) - 9 * 60 * 60;
    const signed = signStaffToken(
      { crm_user_id: 1, name: "n", role: "Owner", tv: 1, app: "lms" },
      SECRET,
      issuedAt,
    );
    const stored = session(signed.token, signed.claims.exp * 1000);
    const check = checkStaffWrite({ session: stored, now: NOW });
    assert.equal(check.ok, false);
    assert.equal(check.message, "Session expired, please sign in again");
    assert.equal(check.message, SESSION_EXPIRED_MESSAGE);

    const headers = staffRequestHeaders({ apikey: "anon" }, SB_URL + "/rest/v1/projects", SB_URL, {
      session: stored,
      now: NOW,
    });
    assert.equal(Object.prototype.hasOwnProperty.call(headers, "x-staff-token"), false);

    const messages = [];
    const unsubscribe = subscribeStaffWriteFailure((message) => messages.push(message));
    if (!check.ok) reportStaffWriteFailure(check.message);
    unsubscribe();
    reportStaffWriteFailure("ignored");
    assert.deepEqual(messages, ["Session expired, please sign in again"]);
  });

  it("refuses a write when the token exp has passed even if the stored expires is still ahead", () => {
    const issuedAt = Math.floor(NOW / 1000) - 9 * 60 * 60;
    const signed = signStaffToken(
      { crm_user_id: 1, name: "n", role: "Owner", tv: 1 },
      SECRET,
      issuedAt,
    );
    const check = checkStaffWrite({
      session: session(signed.token, NOW + 60_000),
      now: NOW,
    });
    assert.equal(check.ok, false);
    assert.equal(check.message, SESSION_EXPIRED_MESSAGE);
  });

  it("lets a live session write and still sends the anon key", () => {
    const issuedAt = Math.floor(NOW / 1000);
    const signed = signStaffToken(
      { crm_user_id: 1, name: "n", role: "Sales", tv: 1 },
      SECRET,
      issuedAt,
    );
    const stored = session(signed.token, signed.claims.exp * 1000);
    assert.equal(checkStaffWrite({ session: stored, now: NOW }).ok, true);
    const readHeaders = staffRequestHeaders(
      { apikey: "anon", Authorization: "Bearer anon", Prefer: "return=representation" },
      SB_URL + "/rest/v1/contacts?select=id",
      SB_URL,
      { session: stored, now: NOW },
    );
    assert.equal(readHeaders["x-staff-token"], signed.token);
    assert.equal(readHeaders.apikey, "anon");
  });

  it("treats an empty PATCH result as a failed write", () => {
    const empty = interpretWriteResult([]);
    assert.equal(empty.ok, false);
    assert.equal(empty.message, EMPTY_WRITE_MESSAGE);
    assert.equal(interpretWriteResult(null).ok, false);

    const saved = interpretWriteResult([{ id: 9, job_name: "Kitchen" }]);
    assert.equal(saved.ok, true);
    assert.deepEqual(saved.row, { id: 9, job_name: "Kitchen" });
    assert.equal(interpretWriteResult([]).message.includes("Not saved"), true);
  });

  it("treats an empty DELETE result as a failed write and keeps a deleted row", () => {
    assert.equal(interpretWriteResult([]).ok, false);
    const deleted = interpretWriteResult([{ id: 4 }]);
    assert.equal(deleted.ok, true);
    assert.equal(deleted.row.id, 4);
  });
});

describe("App.jsx wires the badge without blocking reads", () => {
  const source = readFileSync(new URL("./App.jsx", import.meta.url), "utf8");

  function sliceBetween(startMarker, endMarker) {
    const start = source.indexOf(startMarker);
    const end = source.indexOf(endMarker, start + startMarker.length);
    assert.equal(start >= 0 && end > start, true, startMarker);
    return source.slice(start, end);
  }

  it("sends the token from sbH and sbCount only to the Supabase URL", () => {
    const sbH = sliceBetween("function sbH(", "function blockIfStaffWriteExpired");
    const sbCount = sliceBetween("async function sbCount(", "async function sbInsert");
    assert.match(sbH, /staffRequestHeaders\(/);
    assert.match(sbH, /targetUrl \|\| SB_URL, SB_URL/);
    assert.match(sbCount, /staffRequestHeaders\(/);
    assert.match(sbCount, /, url, SB_URL\)/);
    assert.match(source, /changeOrderUrl,\{method:"DELETE",headers:sbH\(changeOrderUrl\)\}/);
    assert.match(source, /activityUrl,\{method:"DELETE",headers:sbH\(activityUrl\)\}/);
  });

  it("checks expiry before writes and shows the failure on the login screen", () => {
    for (const name of ["async function sbInsert", "async function sbUpdate", "async function sbUpsert", "async function sbDelete"]) {
      const start = source.indexOf(name);
      assert.equal(source.indexOf("blockIfStaffWriteExpired()", start) > start, true, name);
    }
    assert.match(source, /if \(blockIfStaffWriteExpired\(\)\) return;/);
    assert.match(source, /interpretWriteResult\(result\)/);
    assert.match(source, /subscribeStaffWriteFailure\(function\(message\)/);
    assert.match(source, /setSessionNotice\(message\)/);
    assert.match(source, /setAuthUser\(null\)/);
    assert.match(source, /function LoginScreen\(\{ onLogin, notice \}\)/);
    assert.match(source, /\{notice \?/);
  });

  it("does not expiry-check reads", () => {
    const sbGet = sliceBetween("async function sbGet(", "async function sbCount");
    assert.equal(sbGet.includes("blockIfStaffWriteExpired"), false);
    assert.equal(sbGet.includes("checkStaffWrite"), false);
    assert.equal(sbGet.includes("interpretWriteResult"), false);
    assert.match(sbGet, /headers: sbH\(url\)/);
    assert.match(sbGet, /return r\.json\(\)/);
  });
});
