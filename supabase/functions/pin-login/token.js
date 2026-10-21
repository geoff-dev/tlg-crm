// Staff session token. HS256 JWT signed with STAFF_SESSION_SECRET.
// This is not a Supabase/PostgREST token. Verifiers check the signature,
// iss, aud, and exp, then re-read crm_users (active, role, token_version).
// The role claim is a snapshot. Authorization uses the database row.
//
// Header: { "alg": "HS256", "typ": "JWT" }
// Payload:
//   iss: "tlg-staff-auth"
//   aud: "tlg-staff"
//   sub: "crm_user:<id>"
//   crm_user_id, name, role, tv, iat, exp (iat + 8h), app ("lms" | "scheduler")
//
// tlg-scheduler verifies with the same secret (Vercel env STAFF_SESSION_SECRET)
// or calls pin-login action "whoami" / "session", which re-reads the row.

import { createHmac, timingSafeEqual } from "node:crypto";

export const TOKEN_ISS = "tlg-staff-auth";
export const TOKEN_AUD = "tlg-staff";
export const TOKEN_TTL_SEC = 8 * 60 * 60;

function b64url(value) {
  const buf = Buffer.isBuffer(value) ? value : Buffer.from(value);
  return buf.toString("base64url");
}

function encodeJson(value) {
  return b64url(JSON.stringify(value));
}

function decodeJson(segment) {
  return JSON.parse(Buffer.from(segment, "base64url").toString("utf8"));
}

export function signStaffToken(fields, secret, nowSec = Math.floor(Date.now() / 1000)) {
  if (!secret) throw new Error("STAFF_SESSION_SECRET is not set");
  const claims = {
    iss: TOKEN_ISS,
    aud: TOKEN_AUD,
    sub: `crm_user:${Number(fields.crm_user_id)}`,
    crm_user_id: Number(fields.crm_user_id),
    name: fields.name,
    role: fields.role,
    tv: Number(fields.tv ?? 1),
    iat: nowSec,
    exp: nowSec + TOKEN_TTL_SEC,
    app: fields.app === "scheduler" ? "scheduler" : "lms",
  };
  const header = encodeJson({ alg: "HS256", typ: "JWT" });
  const payload = encodeJson(claims);
  const data = `${header}.${payload}`;
  const sig = createHmac("sha256", secret).update(data).digest();
  return { token: `${data}.${b64url(sig)}`, claims };
}

export function verifyStaffToken(token, secret, nowSec = Math.floor(Date.now() / 1000)) {
  if (!secret || typeof token !== "string") return { ok: false, error: "invalid session" };
  const parts = token.split(".");
  if (parts.length !== 3 || parts.some((part) => part.length === 0)) {
    return { ok: false, error: "invalid session" };
  }
  const [headerSeg, payloadSeg, sigSeg] = parts;
  let header;
  let claims;
  try {
    header = decodeJson(headerSeg);
    claims = decodeJson(payloadSeg);
  } catch {
    return { ok: false, error: "invalid session" };
  }
  if (!header || header.alg !== "HS256") return { ok: false, error: "invalid session" };

  const expected = createHmac("sha256", secret).update(`${headerSeg}.${payloadSeg}`).digest();
  let actual;
  try {
    actual = Buffer.from(sigSeg, "base64url");
  } catch {
    return { ok: false, error: "invalid session" };
  }
  if (actual.length !== expected.length || !timingSafeEqual(actual, expected)) {
    return { ok: false, error: "invalid session" };
  }
  if (claims.iss !== TOKEN_ISS || claims.aud !== TOKEN_AUD) {
    return { ok: false, error: "invalid session" };
  }
  if (typeof claims.exp !== "number" || claims.exp <= nowSec) {
    return { ok: false, error: "session expired" };
  }
  if (claims.crm_user_id == null || claims.tv == null) {
    return { ok: false, error: "invalid session" };
  }
  return { ok: true, claims };
}
