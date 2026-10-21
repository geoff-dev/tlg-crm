import { createHmac } from "node:crypto";

export function hashIp(ip, salt) {
  if (!salt) throw new Error("STAFF_IP_SALT is not set");
  return createHmac("sha256", salt).update(String(ip || "unknown")).digest("hex");
}

export function clientIpFromHeaders(headers) {
  const cf = headers.get("cf-connecting-ip");
  if (cf) return cf.trim();
  const real = headers.get("x-real-ip");
  if (real) return real.trim();
  const forwarded = headers.get("x-forwarded-for");
  if (forwarded) return forwarded.split(",")[0].trim();
  return "unknown";
}
