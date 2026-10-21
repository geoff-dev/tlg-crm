import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { isAllowedOrigin } from "./cors.js";
import { handlePinLogin } from "./handler.js";
import { hashIp } from "./ip.js";
import {
  GLOBAL_PAUSE_MS,
  IP_BASE_LOCK_MS,
  IP_MAX_LOCK_MS,
  USER_LOCK_MS,
  lockState,
} from "./lock.js";
import { canApproveHistory, canEditScheduler } from "./policy.js";
import { authorizeSession } from "./session.js";
import { TOKEN_TTL_SEC, signStaffToken, verifyStaffToken } from "./token.js";

const SECRET = "test-staff-secret";
const SALT = "test-ip-salt";
const MIN = 60 * 1000;

function fail(at, ip, userId) {
  return { at, ip_hash: ip, ok: false, crm_user_id: userId };
}

describe("lockout", () => {
  it("locks a person on the 5th failure inside 15 minutes and not before", () => {
    const start = Date.parse("2026-10-21T12:00:00Z");
    const attempts = [];
    for (let i = 0; i < 4; i++) {
      attempts.push(fail(start + i * MIN, "ip-" + i, 7));
      const early = lockState({ attempts, now: start + i * MIN, ipHash: "ip-" + i, userId: 7 });
      assert.equal(early.locked, false);
    }
    attempts.push(fail(start + 4 * MIN, "ip-4", 7));
    const locked = lockState({ attempts, now: start + 4 * MIN, ipHash: "ip-4", userId: 7 });
    assert.equal(locked.locked, true);
    assert.equal(locked.scope, "user");
    assert.equal(locked.retry_after, USER_LOCK_MS / 1000);
    assert.equal(lockState({ attempts, now: start + 4 * MIN, ipHash: "ip-4", userId: 7 }).locked, true);
  });

  it("does not lock five failures spaced more than 15 minutes apart", () => {
    const start = Date.parse("2026-10-21T12:00:00Z");
    const attempts = [];
    for (let i = 0; i < 5; i++) attempts.push(fail(start + i * 16 * MIN, "ip-" + i, 7));
    const state = lockState({ attempts, now: start + 4 * 16 * MIN, ipHash: "ip-4", userId: 7 });
    assert.equal(state.locked, false);
  });

  it("locks that person from another IP and leaves a different person unlocked", () => {
    const start = Date.parse("2026-10-21T12:00:00Z");
    const attempts = [0, 1, 2, 3, 4].map((i) => fail(start + i, "ip-" + i, 7));
    const samePerson = lockState({ attempts, now: start + 4, ipHash: "other-office", userId: 7 });
    const otherPerson = lockState({ attempts, now: start + 4, ipHash: "other-office", userId: 8 });
    assert.equal(samePerson.scope, "user");
    assert.equal(otherPerson.locked, false);
  });

  it("locks an IP for everyone, then doubles the next burst, and caps at 24 hours", () => {
    const attempts = [];
    let now = 0;
    let until = 0;
    let userId = 1;
    for (let level = 1; level <= 8; level++) {
      if (level > 1) now = until + 1;
      for (let i = 0; i < 5; i++) {
        const at = now;
        attempts.push(fail(at, "office", userId++));
        const state = lockState({ attempts, now, ipHash: "office", userId: 99999 });
        if (i < 4) {
          assert.equal(state.locked, false, `level ${level} failure ${i + 1}`);
        } else {
          const expected = Math.min(IP_BASE_LOCK_MS * 2 ** (level - 1), IP_MAX_LOCK_MS);
          assert.equal(state.locked, true);
          assert.equal(state.scope, "ip");
          assert.equal(state.until - at, expected);
          until = state.until;
        }
      }
    }
    assert.equal(until - now, IP_MAX_LOCK_MS);

    const during = until - 1000;
    attempts.push(fail(during, "office", userId));
    const held = lockState({ attempts, now: during, ipHash: "office", userId: 99999 });
    assert.equal(held.until, until);

    const elsewhere = lockState({ attempts, now, ipHash: "somewhere-else", userId: 99999 });
    assert.equal(elsewhere.locked, false);
  });

  it("does not double a per-person lock", () => {
    const attempts = [];
    let now = Date.parse("2026-10-21T12:00:00Z");
    for (let burst = 0; burst < 2; burst++) {
      for (let i = 0; i < 5; i++) {
        attempts.push(fail(now, "ip-" + burst + "-" + i, 7));
      }
      const state = lockState({ attempts, now, ipHash: "fresh", userId: 7 });
      assert.equal(state.scope, "user");
      assert.equal(state.retry_after, USER_LOCK_MS / 1000);
      now = state.until + 1;
    }
  });

  it("pauses everyone on the 31st failure in an hour and releases after 15 minutes", () => {
    const start = Date.parse("2026-10-21T12:00:00Z");
    const attempts = [];
    for (let i = 0; i < 30; i++) attempts.push(fail(start + i * 1000, "ip-" + i, i + 1));
    assert.equal(lockState({ attempts, now: start + 29000, ipHash: "new", userId: 100 }).locked, false);

    attempts.push(fail(start + 30000, "ip-30", 31));
    const paused = lockState({ attempts, now: start + 30000, ipHash: "ip-30", userId: 31 });
    assert.equal(paused.locked, true);
    assert.equal(paused.scope, "global");
    assert.equal(paused.retry_after, GLOBAL_PAUSE_MS / 1000);

    const still = lockState({ attempts, now: paused.until - 1, ipHash: "brand-new", userId: 100 });
    assert.equal(still.scope, "global");
    const released = lockState({ attempts, now: paused.until, ipHash: "brand-new", userId: 100 });
    assert.equal(released.locked, false);
  });

  it("ignores successful attempts", () => {
    const now = Date.parse("2026-10-21T12:00:00Z");
    const attempts = [0, 1, 2, 3].map((i) => fail(now + i, "ip", 7));
    attempts.push({ at: now + 4, ip_hash: "ip", ok: true, crm_user_id: 7 });
    assert.equal(lockState({ attempts, now: now + 4, ipHash: "ip", userId: 7 }).locked, false);
  });
});

describe("token expiry and signature", () => {
  const issuedAt = 1_700_000_000;

  it("signs an 8 hour token that round-trips user id and role", () => {
    const signed = signStaffToken({
      crm_user_id: 4,
      name: "Geoff Horen",
      role: "Owner",
      tv: 2,
      app: "scheduler",
    }, SECRET, issuedAt);
    assert.equal(signed.claims.exp - signed.claims.iat, TOKEN_TTL_SEC);
    assert.equal(TOKEN_TTL_SEC, 8 * 60 * 60);
    const verified = verifyStaffToken(signed.token, SECRET, issuedAt + TOKEN_TTL_SEC - 1);
    assert.equal(verified.ok, true);
    assert.equal(verified.claims.crm_user_id, 4);
    assert.equal(verified.claims.role, "Owner");
    assert.equal(verified.claims.sub, "crm_user:4");
    assert.equal(verified.claims.app, "scheduler");
    assert.equal(verified.claims.iss, "tlg-staff-auth");
    assert.equal(verified.claims.aud, "tlg-staff");
  });

  it("rejects an expired token, a tampered token, and the wrong secret", () => {
    const { token } = signStaffToken({ crm_user_id: 4, name: "Geoff Horen", role: "Owner", tv: 1 }, SECRET, issuedAt);
    assert.equal(verifyStaffToken(token, SECRET, issuedAt + TOKEN_TTL_SEC).error, "session expired");
    const [h, p, s] = token.split(".");
    const flipped = s.slice(0, -2) + (s.endsWith("a") ? "b" : "a");
    assert.equal(verifyStaffToken(`${h}.${p}.${flipped}`, SECRET, issuedAt).ok, false);
    const payload = JSON.parse(Buffer.from(p, "base64url").toString("utf8"));
    payload.role = "Owner";
    payload.crm_user_id = 1;
    const forged = Buffer.from(JSON.stringify(payload)).toString("base64url");
    assert.equal(verifyStaffToken(`${h}.${forged}.${s}`, SECRET, issuedAt).ok, false);
    assert.equal(verifyStaffToken(token, "other-secret", issuedAt).ok, false);
  });
});

describe("role re-read", () => {
  const claims = { crm_user_id: 4, tv: 1, role: "Owner", exp: 999, name: "Geoff Horen" };

  it("uses the database role, not the token role", () => {
    const decision = authorizeSession(
      { ok: true, claims },
      { id: 4, name: "Geoff Horen", role: "Sales", active: true, token_version: 1 },
    );
    assert.equal(decision.ok, true);
    assert.equal(decision.user.role, "Sales");
  });

  it("rejects a deactivated user and a bumped token version", () => {
    const inactive = authorizeSession(
      { ok: true, claims },
      { id: 4, name: "Geoff Horen", role: "Owner", active: false, token_version: 1 },
    );
    assert.equal(inactive.ok, false);
    const bumped = authorizeSession(
      { ok: true, claims },
      { id: 4, name: "Geoff Horen", role: "Owner", active: true, token_version: 2 },
    );
    assert.equal(bumped.ok, false);
  });

  it("rejects an expired verify result before trusting the row", () => {
    const decision = authorizeSession(
      { ok: false, error: "session expired" },
      { id: 4, name: "Geoff Horen", role: "Owner", active: true, token_version: 1 },
    );
    assert.equal(decision.status, 401);
    assert.equal(decision.error, "session expired");
  });
});

describe("scheduler policy", () => {
  it("lets Owner and Admin edit, and only Geoff's id approve", () => {
    assert.equal(canEditScheduler("Owner"), true);
    assert.equal(canEditScheduler("Admin"), true);
    assert.equal(canEditScheduler("Sales"), false);
    assert.equal(canEditScheduler("Production"), false);
    assert.equal(canApproveHistory("Owner", 4, "4"), true);
    assert.equal(canApproveHistory("Owner", 4, [4]), true);
    assert.equal(canApproveHistory("Admin", 4, "4"), false);
    assert.equal(canApproveHistory("Owner", 9, "4"), false);
    assert.equal(canApproveHistory("Sales", 4, "4"), false);
  });
});

describe("ip hash", () => {
  it("is stable, salted, and not the raw address", () => {
    const hashed = hashIp("203.0.113.10", SALT);
    assert.equal(hashed, hashIp("203.0.113.10", SALT));
    assert.notEqual(hashed, hashIp("203.0.113.11", SALT));
    assert.notEqual(hashed, hashIp("203.0.113.10", "other-salt"));
    assert.equal(hashed.includes("203.0.113.10"), false);
  });
});

describe("cors", () => {
  it("allows the two production apps, localhost, and their Vercel previews", () => {
    assert.equal(isAllowedOrigin("https://tlg-crm-nine.vercel.app"), true);
    assert.equal(isAllowedOrigin("https://tlg-scheduler.vercel.app"), true);
    assert.equal(isAllowedOrigin("https://tlg-crm-nine-git-secure-pin-login-team.vercel.app"), true);
    assert.equal(isAllowedOrigin("https://tlg-50axbweq0-geoff-devs-projects.vercel.app"), true);
    assert.equal(isAllowedOrigin("https://tlg-50axbweq0-other-team.vercel.app"), false);
    assert.equal(isAllowedOrigin("http://localhost:5173"), true);
    assert.equal(isAllowedOrigin("https://evil.example"), false);
    assert.equal(isAllowedOrigin("https://tlg-crm-nine.vercel.app.evil.com"), false);
    assert.equal(isAllowedOrigin("https://evil-tlg-crm-nine.vercel.app"), false);
  });
});

function harness() {
  const attempts = [];
  const alerts = [];
  const checkCalls = [];
  const loadCalls = [];
  const users = {
    1: { id: 1, name: "Geoff Horen", role: "Owner", tv: 1, active: true, pin: "135790" },
    2: { id: 2, name: "Rebecca Rhea", role: "Admin", tv: 1, active: true, pin: "246801" },
  };
  let now = Date.parse("2026-10-21T15:00:00.000Z");
  const deps = {
    now: () => now,
    secret: SECRET,
    ipSalt: SALT,
    clientIp(headers) {
      return headers.get("x-test-ip") || "203.0.113.10";
    },
    async recentAttempts() {
      return attempts.map((row) => ({ ...row }));
    },
    async recordAttempt(row) {
      assert.equal("pin" in row, false);
      attempts.push({ ...row });
    },
    async recordAlert(alert) {
      alerts.push(alert);
    },
    async checkPin(id, pin) {
      checkCalls.push({ id, pin });
      const user = users[id];
      if (!user || user.active !== true || user.pin !== pin) return { ok: false };
      return { ok: true, id: user.id, name: user.name, role: user.role, tv: user.tv, active: true };
    },
    async loadUser(id) {
      loadCalls.push(id);
      const user = users[id];
      if (!user) return null;
      return { id: user.id, name: user.name, role: user.role, active: user.active, token_version: user.tv };
    },
    async listNames() {
      return Object.values(users)
        .filter((user) => user.active)
        .map((user) => ({ id: user.id, name: user.name, pin: user.pin, role: user.role }));
    },
  };
  return {
    deps,
    attempts,
    alerts,
    checkCalls,
    loadCalls,
    users,
    setNow(value) { now = value; },
    get now() { return now; },
  };
}

async function post(deps, body, headers = {}) {
  const req = new Request("https://dwkvdelvnmvniewakzoq.supabase.co/functions/v1/pin-login", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      origin: "https://tlg-crm-nine.vercel.app",
      ...headers,
    },
    body: JSON.stringify(body),
  });
  const res = await handlePinLogin(req, deps);
  return { status: res.status, data: await res.json(), headers: res.headers };
}

describe("pin-login handler", () => {
  it("lists ids and names only", async () => {
    const { deps } = harness();
    const res = await post(deps, { action: "names" });
    assert.equal(res.status, 200);
    assert.deepEqual(res.data.users.map((user) => user.name).sort(), ["Geoff Horen", "Rebecca Rhea"]);
    assert.equal(JSON.stringify(res.data).includes("135790"), false);
    assert.equal(JSON.stringify(res.data).includes("pin"), false);
    assert.equal(JSON.stringify(res.data).includes("Owner"), false);
  });

  it("returns an 8 hour token for the right person and refuses a bad PIN", async () => {
    const box = harness();
    const bad = await post(box.deps, { action: "login", user_id: 1, pin: "000000", app: "lms" });
    assert.equal(bad.status, 401);
    assert.equal(bad.data.error, "Invalid PIN");
    assert.equal(box.attempts.length, 1);
    assert.equal(box.attempts[0].ok, false);

    const good = await post(box.deps, { action: "login", user_id: 1, pin: "135790", app: "lms" }, { "x-test-ip": "203.0.113.20" });
    assert.equal(good.status, 200);
    assert.equal(good.data.user.id, 1);
    assert.equal(good.data.user.role, "Owner");
    const verified = verifyStaffToken(good.data.token, SECRET, Math.floor(box.now / 1000));
    assert.equal(verified.ok, true);
    assert.equal(verified.claims.exp - verified.claims.iat, TOKEN_TTL_SEC);
    assert.equal(good.data.expires_at, new Date(verified.claims.exp * 1000).toISOString());
  });

  it("locks the person on the 5th wrong PIN and refuses the correct PIN until it expires", async () => {
    const box = harness();
    for (let i = 0; i < 4; i++) {
      const res = await post(box.deps, { action: "login", user_id: 1, pin: "000000" }, { "x-test-ip": "198.51.100." + i });
      assert.equal(res.status, 401);
    }
    const fifth = await post(box.deps, { action: "login", user_id: 1, pin: "000000" }, { "x-test-ip": "198.51.100.4" });
    assert.equal(fifth.status, 429);
    assert.equal(fifth.data.scope, "user");
    assert.equal(fifth.data.retry_after, 900);

    const callsBefore = box.checkCalls.length;
    const correct = await post(box.deps, { action: "login", user_id: 1, pin: "135790" }, { "x-test-ip": "198.51.100.9" });
    assert.equal(correct.status, 429);
    assert.equal(correct.data.scope, "user");
    assert.equal(box.checkCalls.length, callsBefore);

    const other = await post(box.deps, { action: "login", user_id: 2, pin: "246801" }, { "x-test-ip": "198.51.100.50" });
    assert.equal(other.status, 200);
    assert.equal(other.data.user.role, "Admin");

    box.setNow(box.now + 15 * MIN + 1);
    const after = await post(box.deps, { action: "login", user_id: 1, pin: "135790" }, { "x-test-ip": "198.51.100.9" });
    assert.equal(after.status, 200);
  });

  it("locks one IP for a second person, and a different IP still works", async () => {
    const box = harness();
    for (let i = 0; i < 5; i++) {
      const res = await post(box.deps, { action: "login", user_id: 1, pin: "000000" }, { "x-test-ip": "203.0.113.10" });
      assert.equal(res.status, i === 4 ? 429 : 401);
    }
    const sameIp = await post(box.deps, { action: "login", user_id: 2, pin: "246801" }, { "x-test-ip": "203.0.113.10" });
    assert.equal(sameIp.status, 429);
    assert.equal(sameIp.data.scope, "ip");
    const otherIp = await post(box.deps, { action: "login", user_id: 2, pin: "246801" }, { "x-test-ip": "203.0.113.99" });
    assert.equal(otherIp.status, 200);
  });

  it("records a global pause on the 31st distributed failure", async () => {
    const box = harness();
    for (let i = 0; i < 31; i++) {
      box.users[1000 + i] = { id: 1000 + i, name: "Person " + i, role: "Sales", tv: 1, active: true, pin: "135790" };
      const res = await post(
        box.deps,
        { action: "login", user_id: 1000 + i, pin: "000000" },
        { "x-test-ip": "203.0.113." + i },
      );
      if (i < 30) assert.equal(res.status, 401);
      else {
        assert.equal(res.status, 429);
        assert.equal(res.data.scope, "global");
        assert.equal(res.data.retry_after, 900);
      }
    }
    assert.equal(box.alerts.length, 1);
    assert.equal(box.alerts[0].kind, "global_pause");
    const blocked = await post(box.deps, { action: "login", user_id: 1, pin: "135790" }, { "x-test-ip": "198.51.100.20" });
    assert.equal(blocked.status, 429);
    assert.equal(blocked.data.scope, "global");
    assert.equal(box.alerts.length, 1);
  });

  it("re-reads role on whoami and drops expired, inactive, and revoked tokens", async () => {
    const box = harness();
    const login = await post(box.deps, { action: "login", user_id: 1, pin: "135790" });
    const token = login.data.token;
    box.users[1].role = "Sales";
    const who = await post(box.deps, { action: "whoami" }, { "x-staff-token": token });
    assert.equal(who.status, 200);
    assert.equal(who.data.role, "Sales");
    assert.equal(who.data.name, "Geoff Horen");
    assert.equal(box.loadCalls.length, 1);

    box.users[1].active = false;
    const inactive = await post(box.deps, { action: "session" }, { authorization: "Bearer " + token });
    assert.equal(inactive.status, 401);

    box.users[1].active = true;
    box.users[1].tv = 2;
    const revoked = await post(box.deps, { action: "whoami", token });
    assert.equal(revoked.status, 401);

    const loadsBeforeExpiry = box.loadCalls.length;
    box.setNow(box.now + 8 * 60 * MIN + 1000);
    const expired = await post(box.deps, { action: "whoami" }, { "x-staff-token": token });
    assert.equal(expired.status, 401);
    assert.equal(expired.data.error, "session expired");
    assert.equal(box.loadCalls.length, loadsBeforeExpiry);
  });

  it("rejects a disallowed browser origin", async () => {
    const { deps } = harness();
    const res = await post(deps, { action: "names" }, { origin: "https://evil.example" });
    assert.equal(res.status, 403);
  });
});

describe("migration files", () => {
  const additive = readFileSync(new URL("../../migrations/20261021120000_staff_pin_auth.sql", import.meta.url), "utf8");
  const revoke = readFileSync(new URL("../../migrations/20261021120100_crm_users_revoke_anon.sql", import.meta.url), "utf8");

  it("hashes existing PINs with pgcrypto bcrypt and does not drop the plaintext column", () => {
    assert.match(additive, /extensions\.crypt\(pin, extensions\.gen_salt\('bf', 10\)\)/);
    assert.match(additive, /pin_hash text/);
    assert.match(additive, /token_version/);
    assert.equal(additive.includes("drop column"), false);
    assert.match(additive, /staff_login_attempts/);
    assert.match(additive, /staff_login_alerts/);
    assert.match(additive, /grant select, insert on table public\.staff_login_attempts to service_role/);
    assert.match(additive, /grant select, insert on table public\.staff_login_alerts to service_role/);
    assert.match(additive, /revoke all privileges on table public\.staff_login_attempts from public, anon, authenticated/);
    assert.match(additive, /revoke all on function public\.staff_check_pin\(integer, text\) from public, anon, authenticated/);
    assert.match(additive, /grant execute on function public\.staff_check_pin\(integer, text\) to service_role/);
    assert.match(additive, /where active is true/);
  });

  it("revokes anon and authenticated on crm_users and turns RLS on with no policy", () => {
    assert.match(revoke, /revoke all privileges on table public\.crm_users from anon, authenticated/);
    assert.match(revoke, /revoke all privileges on table public\.crm_users from public/);
    assert.match(revoke, /alter table public\.crm_users enable row level security/);
    const executable = revoke.replace(/--.*$/gm, "");
    assert.equal(executable.includes("create policy"), false);
    assert.equal(executable.includes("grant "), false);
  });
});
