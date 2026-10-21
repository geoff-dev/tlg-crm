import { isAllowedOrigin, corsHeaders } from "./cors.js";
import { hashIp, clientIpFromHeaders } from "./ip.js";
import { lockState } from "./lock.js";
import { authorizeSession } from "./session.js";
import { signStaffToken, verifyStaffToken } from "./token.js";

const LOOKBACK_MS = 7 * 24 * 60 * 60 * 1000;

function json(body, status, cors) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...cors },
  });
}

function normalizeAttempt(row) {
  const at = typeof row.at === "number" ? row.at : Date.parse(row.at);
  return {
    at,
    ip_hash: row.ip_hash,
    ok: row.ok === true,
    crm_user_id: row.crm_user_id,
    app: row.app,
  };
}

function readToken(req, body) {
  const dedicated = req.headers.get("x-staff-token");
  if (dedicated) return dedicated.trim();
  const auth = req.headers.get("authorization") || "";
  const match = auth.match(/^Bearer\s+(.+)$/i);
  if (match) return match[1].trim();
  if (body && typeof body.token === "string") return body.token.trim();
  return "";
}

async function loadAttempts(deps, now) {
  const since = new Date(now - LOOKBACK_MS).toISOString();
  const rows = await deps.recentAttempts(since);
  return (rows || []).map(normalizeAttempt).filter((row) => Number.isFinite(row.at));
}

function publicNames(rows) {
  return (rows || [])
    .filter((row) => row && row.id != null && row.name)
    .map((row) => ({ id: row.id, name: row.name }));
}

export async function handlePinLogin(req, deps) {
  const origin = req.headers.get("origin");
  const cors = corsHeaders(origin);
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });
  if (origin && !isAllowedOrigin(origin)) return json({ error: "origin not allowed" }, 403, cors);
  if (req.method !== "POST") return json({ error: "POST only" }, 405, cors);

  let body = {};
  try {
    body = await req.json();
  } catch {
    return json({ error: "invalid JSON" }, 400, cors);
  }
  if (!body || typeof body !== "object") return json({ error: "invalid JSON" }, 400, cors);

  const action = String(body.action || "");
  try {
    if (action === "names") return await handleNames(deps, cors);
    if (action === "login") return await handleLogin(req, body, deps, cors);
    if (action === "session" || action === "whoami") return await handleSession(req, body, deps, cors);
    return json({ error: "unknown action" }, 400, cors);
  } catch {
    return json({ error: "staff auth failed" }, 500, cors);
  }
}

async function handleNames(deps, cors) {
  const users = publicNames(await deps.listNames());
  return json({ users }, 200, cors);
}

async function handleLogin(req, body, deps, cors) {
  if (!deps.secret || !deps.ipSalt) return json({ error: "staff auth is not configured" }, 500, cors);

  const now = deps.now();
  const rawId = Number(body.user_id ?? body.userId);
  const userOk = Number.isInteger(rawId) && rawId > 0;
  const pin = String(body.pin ?? "");
  const pinOk = /^[0-9]{6}$/.test(pin);
  const app = body.app === "scheduler" ? "scheduler" : "lms";
  const ip = (deps.clientIp || clientIpFromHeaders)(req.headers);
  const ipHash = hashIp(ip, deps.ipSalt);
  const userId = userOk ? rawId : null;

  const attempts = await loadAttempts(deps, now);
  const before = lockState({ attempts, now, ipHash, userId });
  if (before.locked) {
    return json(
      { error: "Too many attempts", retry_after: before.retry_after, scope: before.scope },
      429,
      cors,
    );
  }

  let checked = null;
  if (userOk && pinOk) checked = await deps.checkPin(userId, pin);
  const ok = !!(checked && checked.ok === true && checked.active !== false);

  await deps.recordAttempt({
    at: new Date(now).toISOString(),
    ip_hash: ipHash,
    ok,
    crm_user_id: userId,
    app,
  });

  const afterAttempts = attempts.concat([{
    at: now,
    ip_hash: ipHash,
    ok,
    crm_user_id: userId,
    app,
  }]);
  const after = lockState({ attempts: afterAttempts, now, ipHash, userId });

  if (!ok) {
    if (after.locked && after.scope === "global" && deps.recordAlert) {
      try {
        await deps.recordAlert({ kind: "global_pause", detail: { retry_after: after.retry_after } });
      } catch {
        // The pause still applies. A failed alert insert must not hide the 429.
      }
    }
    if (after.locked) {
      return json(
        { error: "Too many attempts", retry_after: after.retry_after, scope: after.scope },
        429,
        cors,
      );
    }
    return json({ error: "Invalid PIN" }, 401, cors);
  }

  const signed = signStaffToken({
    crm_user_id: checked.id,
    name: checked.name,
    role: checked.role,
    tv: checked.tv,
    app,
  }, deps.secret, Math.floor(now / 1000));

  return json({
    token: signed.token,
    user: { id: Number(checked.id), name: checked.name, role: checked.role },
    expires_at: new Date(signed.claims.exp * 1000).toISOString(),
  }, 200, cors);
}

async function handleSession(req, body, deps, cors) {
  if (!deps.secret) return json({ error: "staff auth is not configured" }, 500, cors);
  const token = readToken(req, body);
  const nowSec = Math.floor(deps.now() / 1000);
  const verified = verifyStaffToken(token, deps.secret, nowSec);
  if (!verified.ok) return json({ error: verified.error }, 401, cors);
  const row = await deps.loadUser(Number(verified.claims.crm_user_id));
  const decision = authorizeSession(verified, row);
  if (!decision.ok) return json({ error: decision.error }, decision.status, cors);
  return json(decision.user, 200, cors);
}
